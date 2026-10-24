using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Xunit;

namespace Whitewater.Tests;

/// <summary>
/// Executes the shared fixture corpus under sdk/spec/conformance/ in
/// whitewater against a live Riverbed, per the fixture vocabulary in
/// conformance/README.md. Enabled when the endpoint set and the corpus
/// directory are configured:
///
///   WHITEWATER_ENDPOINTS=http://localhost:7071,...   (or the legacy
///     WHITEWATER_CONFORMANCE_URLS)
///   WHITEWATER_SPEC_DIR=.../whitewater/sdk/spec/conformance
///     (or the legacy WHITEWATER_FIXTURE_DIR)
///
/// WHITEWATER_API_KEY (legacy FINNSTREAM_ADMIN_API_KEY) defaults to the
/// local development key. WHITEWATER_CHAOS_CMD names the environment's
/// fault driver; without it chaos.* steps report unsupported rather
/// than skipping. A scenario whose op this SDK cannot express fails
/// explicitly rather than silently skipping — a spec-required signal.
///
/// Steps dispatch at the wire surface so expect asserts the same
/// (status, body) contract every language runner implements: poll
/// windows re-dispatch asynchronous steps until expectations settle,
/// while a separate transient budget rides out transport errors and
/// 5xx unavailability without spending poll attempts.
/// </summary>
public class ConformanceRunner
{
    private static readonly string? BaseUrls =
        Environment.GetEnvironmentVariable("WHITEWATER_ENDPOINTS")
        ?? Environment.GetEnvironmentVariable("WHITEWATER_CONFORMANCE_URLS");
    private static readonly string? FixtureDir =
        Environment.GetEnvironmentVariable("WHITEWATER_SPEC_DIR")
        ?? Environment.GetEnvironmentVariable("WHITEWATER_FIXTURE_DIR");
    private static readonly string ApiKey =
        Environment.GetEnvironmentVariable("WHITEWATER_API_KEY")
        ?? Environment.GetEnvironmentVariable("FINNSTREAM_ADMIN_API_KEY")
        ?? "whitewater-local-development-admin-key";
    private static readonly string ChaosCmd =
        Environment.GetEnvironmentVariable("WHITEWATER_CHAOS_CMD") ?? "";

    private const int TransientAttempts = 120;
    private static readonly TimeSpan TransientInterval = TimeSpan.FromMilliseconds(500);

    /// <summary>The shared op vocabulary onto the wire surface.</summary>
    private static readonly Dictionary<string, (string Method, string Path)> WireEndpoints = new()
    {
        ["wcl"] = ("POST", "/v1/admin/wcl"),
        ["admin.wcl"] = ("POST", "/v1/admin/wcl"),
        ["admin.commands"] = ("POST", "/v1/admin/commands"),
        ["writer.append"] = ("POST", "/v1/writers/append"),
        ["writer.append_batch"] = ("POST", "/v1/writers/append-batch"),
        ["reader.open"] = ("POST", "/v1/readers/open"),
        ["reader.fetch"] = ("POST", "/v1/readers/fetch"),
        ["reader.ack"] = ("POST", "/v1/readers/ack"),
        ["reader.close"] = ("POST", "/v1/readers/close"),
        ["reader.temporary_fetch"] = ("POST", "/v1/readers/temporary/fetch"),
        ["feed.append"] = ("POST", "/v1/feeds/append"),
        ["member.join"] = ("POST", "/v1/subscriptions/members/join"),
        ["member.claim"] = ("POST", "/v1/subscriptions/members/claim"),
        ["member.renew"] = ("POST", "/v1/subscriptions/members/renew"),
        ["member.release"] = ("POST", "/v1/subscriptions/members/release"),
        ["member.transfer"] = ("POST", "/v1/subscriptions/members/transfer"),
        ["member.fetch"] = ("POST", "/v1/subscriptions/members/fetch"),
        ["member.ack"] = ("POST", "/v1/subscriptions/members/ack"),
        ["subscription.seek"] = ("POST", "/v1/subscriptions/seek"),
        ["statestore.lookup"] = ("POST", "/v1/state-stores/lookup"),
        ["statestore.get_by"] = ("POST", "/v1/state-stores/get-by"),
        ["statestore.iterate"] = ("POST", "/v1/state-stores/iterate"),
        ["statestore.clear"] = ("POST", "/v1/state-stores/clear"),
    };

    private sealed class TransportException : Exception
    {
        public TransportException(string message, Exception inner)
            : base(message, inner) { }
    }

    /// <summary>Status-preserving JSON-over-HTTP calls; non-2xx replies
    /// return the parsed body so expect asserts error_status/error_code
    /// against it.</summary>
    private sealed class Wire : IDisposable
    {
        private readonly Uri[] _endpoints;
        private readonly string _apiKey;
        private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(30) };
        private int _next;

        public Wire(Uri[] endpoints, string apiKey)
        {
            _endpoints = endpoints;
            _apiKey = apiKey;
        }

        public async Task<(int Status, JsonNode? Body)> RequestAsync(
            string method, string path, JsonNode? body)
        {
            var baseUri = _endpoints[_next];
            _next = (_next + 1) % _endpoints.Length;
            using var request = new HttpRequestMessage(
                new HttpMethod(method), new Uri(baseUri, path));
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _apiKey);
            request.Headers.Accept.ParseAdd("application/json");
            if (body != null)
            {
                request.Content = new StringContent(
                    body.ToJsonString(), Encoding.UTF8, "application/json");
            }
            HttpResponseMessage response;
            try
            {
                response = await _http.SendAsync(request);
            }
            catch (Exception error) when (error is HttpRequestException or TaskCanceledException)
            {
                throw new TransportException($"Node unreachable: {error.Message}", error);
            }
            var text = await response.Content.ReadAsStringAsync();
            JsonNode? parsed = null;
            if (text.Length > 0)
            {
                try
                {
                    parsed = JsonNode.Parse(text);
                }
                catch (JsonException)
                {
                    parsed = null;
                }
            }
            return ((int)response.StatusCode, parsed);
        }

        public void Dispose() => _http.Dispose();
    }

    [Fact]
    public async Task SharedScenariosPassAgainstTheRiverbed()
    {
        if (string.IsNullOrWhiteSpace(BaseUrls)
            || string.IsNullOrWhiteSpace(FixtureDir)
            || !Directory.Exists(FixtureDir))
        {
            return;
        }
        var endpoints = BaseUrls.Split(',')
            .Select(raw => new Uri(raw.Trim().TrimEnd('/') + "/")).ToArray();
        var files = Directory.GetFiles(FixtureDir, "*.json").OrderBy(f => f).ToArray();
        Assert.NotEmpty(files);
        foreach (var file in files)
        {
            var scenario = JsonNode.Parse(await File.ReadAllTextAsync(file))!;
            var apiKey = scenario["api_key"]?.GetValue<string>() ?? ApiKey;
            using var wire = new Wire(endpoints, apiKey);
            await new Runner(wire, ChaosCmd).RunAsync(
                Path.GetFileName(file), scenario);
        }
    }

    private sealed class Runner
    {
        private static readonly Regex TokenPattern = new(@"\{\{(.*?)\}\}",
            RegexOptions.Compiled);
        private static readonly Regex WholeTokenPattern = new(@"^\{\{(.*?)\}\}$",
            RegexOptions.Compiled);

        private readonly Wire _wire;
        private readonly string _chaosCmd;
        private readonly Dictionary<string, JsonNode?> _bound = new();
        private readonly Dictionary<string, string> _namedUuids = new();
        private readonly string _uniq =
            $"u{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds():x}{Random.Shared.Next(0, 0xFFFFFF):x6}";

        public Runner(Wire wire, string chaosCmd)
        {
            _wire = wire;
            _chaosCmd = chaosCmd;
        }

        // ---- substitution ----

        private JsonNode? ResolveNode(string token)
        {
            token = token.Trim();
            if (token == "uniq")
            {
                return JsonValue.Create(_uniq);
            }
            if (token == "uuid")
            {
                return JsonValue.Create(Guid.NewGuid().ToString());
            }
            if (token.StartsWith("uuid:"))
            {
                var name = token[5..];
                if (!_namedUuids.TryGetValue(name, out var id))
                {
                    id = Guid.NewGuid().ToString();
                    _namedUuids[name] = id;
                }
                return JsonValue.Create(id);
            }
            if (token == "now_ns" || token.StartsWith("now_ns:"))
            {
                // Wall-clock epoch nanoseconds as a decimal string; an
                // optional signed offset shifts the instant.
                long shift = token.StartsWith("now_ns:")
                    ? long.Parse(token[7..])
                    : 0;
                var nanos = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L + shift;
                return JsonValue.Create(nanos.ToString());
            }
            var dot = token.IndexOf('.');
            if (dot > 0
                && _bound.TryGetValue(token[..dot], out var bound)
                && bound != null)
            {
                var value = Dig(bound, token[(dot + 1)..]);
                if (value != null)
                {
                    return value.DeepClone();
                }
            }
            throw new InvalidOperationException(
                $"unresolved fixture token {{{{{token}}}}}");
        }

        private JsonNode? Substitute(JsonNode? node)
        {
            switch (node)
            {
                case JsonObject obj:
                {
                    var copy = new JsonObject();
                    foreach (var entry in obj)
                    {
                        copy[entry.Key] = entry.Value == null
                            ? null
                            : Substitute(entry.Value);
                    }
                    return copy;
                }
                case JsonArray array:
                {
                    var copy = new JsonArray();
                    foreach (var item in array)
                    {
                        copy.Add(item == null ? null : Substitute(item));
                    }
                    return copy;
                }
                case JsonValue value when value.TryGetValue<string>(out var text):
                {
                    var whole = WholeTokenPattern.Match(text);
                    if (whole.Success)
                    {
                        try
                        {
                            return ResolveNode(whole.Groups[1].Value);
                        }
                        catch (InvalidOperationException)
                        {
                            return JsonValue.Create("null");
                        }
                    }
                    return JsonValue.Create(TokenPattern.Replace(
                        text, match =>
                        {
                            try
                            {
                                var resolved = ResolveNode(match.Groups[1].Value);
                                return resolved is JsonValue scalar
                                    && scalar.TryGetValue<string>(out var inner)
                                        ? inner
                                        : resolved?.ToJsonString() ?? "null";
                            }
                            catch (InvalidOperationException)
                            {
                                return "null";
                            }
                        }));
                }
                default:
                    return node?.DeepClone();
            }
        }

        private static IEnumerable<string> SplitPath(string path)
        {
            var segments = new List<string>();
            var depth = 0;
            var start = 0;
            for (var i = 0; i < path.Length; i++)
            {
                var c = path[i];
                if (c == '[')
                {
                    depth += 1;
                }
                else if (c == ']')
                {
                    depth = Math.Max(0, depth - 1);
                }
                else if (c == '.' && depth == 0)
                {
                    segments.Add(path[start..i]);
                    start = i + 1;
                }
            }
            segments.Add(path[start..]);
            return segments;
        }

        private static JsonNode? Dig(JsonNode? node, string path)
        {
            var current = node;
            foreach (var segment in SplitPath(path))
            {
                if (current == null)
                {
                    return null;
                }
                var name = segment;
                var indices = new List<string>();
                var bracket = segment.IndexOf('[');
                if (bracket >= 0)
                {
                    name = segment[..bracket];
                    var rest = segment[bracket..];
                    while (rest.StartsWith('['))
                    {
                        var close = rest.IndexOf(']');
                        if (close < 0)
                        {
                            return null;
                        }
                        indices.Add(rest[1..close]);
                        rest = rest[(close + 1)..];
                    }
                }
                if (name.Length > 0)
                {
                    if (current is not JsonObject obj || !obj.TryGetPropertyValue(name, out var next))
                    {
                        return null;
                    }
                    current = next;
                }
                foreach (var index in indices)
                {
                    if (index.Length >= 2 && index.StartsWith('"') && index.EndsWith('"'))
                    {
                        var key = index[1..^1];
                        if (current is not JsonObject quoted
                            || !quoted.TryGetPropertyValue(key, out var next))
                        {
                            return null;
                        }
                        current = next;
                        continue;
                    }
                    if (current is not JsonArray array
                        || !int.TryParse(index, out var position)
                        || position >= array.Count)
                    {
                        return null;
                    }
                    current = array[position];
                }
            }
            return current;
        }

        // ---- wire lowering ----

        private static byte[] RawBytes(JsonNode? node)
        {
            switch (node)
            {
                case null:
                    return Array.Empty<byte>();
                case JsonObject wrapper:
                    if (wrapper["utf8"] is { } utf8)
                    {
                        return Encoding.UTF8.GetBytes(utf8.GetValue<string>());
                    }
                    if (wrapper["hex"] is { } hex)
                    {
                        return Convert.FromHexString(hex.GetValue<string>());
                    }
                    if (wrapper["base64"] is { } b64)
                    {
                        return Convert.FromBase64String(b64.GetValue<string>());
                    }
                    return Array.Empty<byte>();
                case JsonValue value when value.TryGetValue<string>(out var text):
                    return Encoding.UTF8.GetBytes(text);
                default:
                    return Array.Empty<byte>();
            }
        }

        private static void WireFields(JsonObject row)
        {
            foreach (var (raw, encoded) in new[]
            {
                ("key", "key_base64"),
                ("payload", "payload_base64"),
                ("after_key", "after_key_base64"),
            })
            {
                if (row[raw] is { } value)
                {
                    row.Remove(raw);
                    row[encoded] = Convert.ToBase64String(RawBytes(value));
                }
            }
            if (row["metadata"] is { } metadata)
            {
                row.Remove("metadata");
                var encoded = new JsonObject();
                if (metadata is JsonObject entries)
                {
                    foreach (var entry in entries)
                    {
                        encoded[entry.Key] = Convert.ToBase64String(RawBytes(entry.Value));
                    }
                }
                row["metadata_base64"] = encoded;
            }
            foreach (var field in new[] { "event_time_ns", "after_event_time_ns", "timestamp_ns" })
            {
                if (row[field] is JsonValue number
                    && number.TryGetValue<long>(out var integer))
                {
                    row[field] = integer.ToString();
                }
            }
        }

        private static JsonNode? WireArgs(JsonNode? args)
        {
            if (args is not JsonObject obj)
            {
                return args?.DeepClone();
            }
            var wired = (JsonObject)obj.DeepClone();
            if (wired["records"] is JsonArray records)
            {
                foreach (var record in records)
                {
                    if (record is JsonObject row)
                    {
                        WireFields(row);
                    }
                }
            }
            WireFields(wired);
            return wired;
        }

        // ---- dispatch ----

        private async Task<(int Status, JsonNode? Body)> DispatchAsync(
            string op, JsonNode? args)
        {
            if (op == "health")
            {
                return await _wire.RequestAsync("GET", "/health", null);
            }
            if (op == "feed.records")
            {
                var a = args as JsonObject ?? new JsonObject();
                var path = "/v1/feeds/records?feed="
                    + Uri.EscapeDataString(a["feed"]?.GetValue<string>() ?? "");
                if (a["limit"] is { } limit)
                {
                    path += "&limit=" + limit.ToJsonString();
                }
                if (a["after"] is { } after)
                {
                    path += "&after=" + Uri.EscapeDataString(after.GetValue<string>());
                }
                var (status, body) = await _wire.RequestAsync("GET", path, null);
                if (body is JsonArray bare)
                {
                    var wrapped = new JsonObject();
                    wrapped["records"] = bare.DeepClone();
                    body = wrapped;
                }
                return (status, body);
            }
            if (op == "statestore.mutate")
            {
                // Flat {store, mutation_id, kind, key, payload, named}
                // lowers to {store, mutation_id, op: {kind, key_base64?, ...}}.
                var body = WireArgs(args) as JsonObject
                    ?? throw new InvalidOperationException(
                        "statestore.mutate args must be an object");
                var kind = body["kind"]?.GetValue<string>()
                    ?? throw new InvalidOperationException("statestore.mutate needs a `kind`");
                body.Remove("kind");
                var operation = new JsonObject { ["kind"] = kind };
                foreach (var name in new[] { "key_base64", "payload_base64", "named" })
                {
                    if (body[name] is { } value)
                    {
                        body.Remove(name);
                        operation[name] = value;
                    }
                }
                body["op"] = operation;
                return await _wire.RequestAsync("POST", "/v1/state-stores/mutate", body);
            }
            if (!WireEndpoints.TryGetValue(op, out var target))
            {
                throw new InvalidOperationException($"unsupported op {op}");
            }
            return await _wire.RequestAsync(target.Method, target.Path, WireArgs(args));
        }

        private void RunChaos(string op, JsonNode? args)
        {
            if (_chaosCmd.Length == 0)
            {
                throw new InvalidOperationException(
                    $"unsupported: {op} needs a WHITEWATER_CHAOS_CMD hook");
            }
            var rendered = Substitute(args ?? new JsonObject()) as JsonObject ?? new JsonObject();
            var action = new StringBuilder(op);
            foreach (var key in rendered.Select(e => e.Key).OrderBy(k => k, StringComparer.Ordinal))
            {
                action.Append(' ').Append(rendered[key]?.ToJsonString().Trim('"'));
            }
            var shell = OperatingSystem.IsWindows()
                ? ("/c", "cmd.exe")
                : ("-c", "/bin/sh");
            var start = new ProcessStartInfo
            {
                FileName = shell.Item2,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            start.ArgumentList.Add(shell.Item1);
            start.ArgumentList.Add($"{_chaosCmd} {action}");
            using var process = Process.Start(start)!;
            var stdout = process.StandardOutput.ReadToEnd();
            var stderr = process.StandardError.ReadToEnd();
            process.WaitForExit();
            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"chaos hook failed: {process.ExitCode}: {stdout}{stderr}");
            }
        }

        // ---- expectations ----

        private void CheckExpect(int status, JsonNode? body, JsonObject expect)
        {
            foreach (var entry in expect)
            {
                var path = entry.Key;
                var expected = entry.Value;
                if (path == "error_status")
                {
                    if (status != expected!.GetValue<int>())
                    {
                        throw new InvalidOperationException(
                            $"expected status {expected.ToJsonString()}, got {status}: "
                                + (body?.ToJsonString() ?? "null"));
                    }
                    continue;
                }
                if (path == "error_code")
                {
                    var code = body is JsonObject obj && obj.TryGetPropertyValue("code", out var c)
                        ? c
                        : null;
                    if (!JsonNode.DeepEquals(code, expected))
                    {
                        throw new InvalidOperationException(
                            $"expected code {expected?.ToJsonString()}, got body "
                                + (body?.ToJsonString() ?? "null"));
                    }
                    continue;
                }
                Check(path, Dig(body, path), expected, body);
            }
        }

        private void Check(string path, JsonNode? actual, JsonNode? expected, JsonNode? body)
        {
            if (expected is JsonValue expectedValue
                && expectedValue.TryGetValue<string>(out var text))
            {
                switch (text)
                {
                    case "present":
                    case "!=null":
                        if (actual == null)
                        {
                            throw new InvalidOperationException(
                                $"{path} absent in {body?.ToJsonString()}");
                        }
                        return;
                    case "absent":
                        if (actual != null)
                        {
                            throw new InvalidOperationException(
                                $"{path} present: {actual.ToJsonString()}");
                        }
                        return;
                }
                if (text.StartsWith("len>="))
                {
                    var minimum = int.Parse(text[5..]);
                    var length = actual switch
                    {
                        JsonArray array => array.Count,
                        JsonObject obj => obj.Count,
                        JsonValue v when v.TryGetValue<string>(out var s) => s.Length,
                        _ => -1,
                    };
                    if (length < minimum)
                    {
                        throw new InvalidOperationException(
                            $"{path} len {length} < {minimum} in {body?.ToJsonString()}");
                    }
                    return;
                }
                if (text.StartsWith("=="))
                {
                    var reference = text[2..];
                    var dot = reference.IndexOf('.');
                    var head = dot < 0 ? reference : reference[..dot];
                    JsonNode? expectedBound = null;
                    if (_bound.TryGetValue(head, out var bound))
                    {
                        expectedBound = dot < 0 ? bound : Dig(bound, reference[(dot + 1)..]);
                    }
                    if (!JsonNode.DeepEquals(actual, expectedBound))
                    {
                        throw new InvalidOperationException(
                            $"{path} = {actual?.ToJsonString() ?? "null"}, expected "
                                + (expectedBound?.ToJsonString() ?? "null"));
                    }
                    return;
                }
            }
            if (expected is JsonObject wrapper && wrapper["base64_json"] is JsonObject nested)
            {
                // Decode the actual base64 string as JSON, then apply each
                // named expectation to the decoded document.
                JsonNode? decoded = null;
                if (actual is JsonValue av && av.TryGetValue<string>(out var encoded))
                {
                    try
                    {
                        decoded = JsonNode.Parse(
                            Encoding.UTF8.GetString(Convert.FromBase64String(encoded)));
                    }
                    catch (Exception)
                    {
                        decoded = null;
                    }
                }
                if (decoded == null)
                {
                    throw new InvalidOperationException(
                        $"{path} is not base64 JSON: {actual?.ToJsonString() ?? "null"}");
                }
                try
                {
                    CheckExpect(0, decoded, (JsonObject)nested.DeepClone());
                }
                catch (InvalidOperationException inner)
                {
                    throw new InvalidOperationException($"{path} decoded: {inner.Message}");
                }
                return;
            }
            // i64 nanoseconds may arrive as decimal strings or JSON numbers.
            if (!LooseEquals(expected, actual))
            {
                throw new InvalidOperationException(
                    $"{path} = {actual?.ToJsonString() ?? "null"}, expected "
                        + (expected?.ToJsonString() ?? "null")
                        + $" in {body?.ToJsonString() ?? "null"}");
            }
        }

        private static bool LooseEquals(JsonNode? expected, JsonNode? actual)
        {
            if (expected == null || actual == null)
            {
                return expected == null && actual == null;
            }
            if (expected is JsonValue expectedValue && actual is JsonValue actualValue)
            {
                if (expectedValue.TryGetValue<string>(out var expectedText)
                    && actualValue.TryGetValue<string>(out var actualText))
                {
                    return expectedText == actualText;
                }
                if (TryNumber(expectedValue, out var expectedNumber)
                    && TryNumber(actualValue, out var actualNumber))
                {
                    return expectedNumber == actualNumber;
                }
            }
            return JsonNode.DeepEquals(expected, actual);
        }

        private static bool TryNumber(JsonValue value, out long number)
        {
            if (value.TryGetValue<long>(out number))
            {
                return true;
            }
            if (value.TryGetValue<string>(out var text) && long.TryParse(text, out number))
            {
                return true;
            }
            return false;
        }

        // ---- anonymous-uuid rebinding ----

        private static JsonNode? BindAnonymousUuids(JsonNode? node, int step, int[] ordinal)
        {
            switch (node)
            {
                case JsonObject obj:
                {
                    var copy = new JsonObject();
                    foreach (var entry in obj)
                    {
                        copy[entry.Key] = entry.Value == null
                            ? null
                            : BindAnonymousUuids(entry.Value, step, ordinal);
                    }
                    return copy;
                }
                case JsonArray array:
                {
                    var copy = new JsonArray();
                    foreach (var item in array)
                    {
                        copy.Add(item == null ? null : BindAnonymousUuids(item, step, ordinal));
                    }
                    return copy;
                }
                case JsonValue value when value.TryGetValue<string>(out var text):
                    while (text.Contains("{{uuid}}"))
                    {
                        text = text.Replace("{{uuid}}", $"{{{{uuid:anon-{step}-{ordinal[0]}}}}}");
                        ordinal[0] += 1;
                    }
                    return JsonValue.Create(text);
                default:
                    return node?.DeepClone();
            }
        }

        // ---- driver ----

        public async Task RunAsync(string name, JsonNode scenario)
        {
            var steps = scenario["steps"] as JsonArray
                ?? throw new InvalidOperationException($"{name}: no steps");
            var index = 0;
            foreach (var stepNode in steps)
            {
                var step = stepNode!.AsObject();
                var op = step["op"]!.GetValue<string>();

                if (op.StartsWith("chaos."))
                {
                    try
                    {
                        RunChaos(op, step["args"]);
                    }
                    catch (Exception error)
                    {
                        throw new InvalidOperationException(
                            $"{name}: step {index} ({op}): {error.Message}", error);
                    }
                    index += 1;
                    continue;
                }

                var isSpecial = op is "feed.records" or "health" or "statestore.mutate";
                if (!WireEndpoints.ContainsKey(op) && !isSpecial)
                {
                    throw new InvalidOperationException(
                        $"{name}: step {index}: unsupported op {op}");
                }

                var argsTemplate = BindAnonymousUuids(step["args"], index, new[] { 0 });
                var poll = step["poll"] as JsonObject;
                var pollTimes = Math.Clamp(poll?["times"]?.GetValue<int>() ?? 1, 1, 30);
                var pollIntervalMs = Math.Min(
                    poll?["interval_ms"]?.GetValue<int>() is { } interval && interval > 0
                        ? interval
                        : 500,
                    10_000);

                var attempt = 0;
                var transient = 0;
                int status;
                JsonNode? body;
                while (true)
                {
                    var args = Substitute(argsTemplate);
                    try
                    {
                        (status, body) = await DispatchAsync(op, args);
                    }
                    catch (TransportException)
                    {
                        if (transient < TransientAttempts)
                        {
                            transient += 1;
                            await Task.Delay(TransientInterval);
                            continue;
                        }
                        attempt += 1;
                        if (attempt < pollTimes)
                        {
                            await Task.Delay(pollIntervalMs);
                            continue;
                        }
                        throw new InvalidOperationException(
                            $"{name}: step {index} ({op}): transport exhausted");
                    }
                    catch (Exception error)
                    {
                        throw new InvalidOperationException(
                            $"{name}: step {index} ({op}): {error.Message}", error);
                    }
                    if (status >= 500 && transient < TransientAttempts)
                    {
                        transient += 1;
                        await Task.Delay(TransientInterval);
                        continue;
                    }
                    attempt += 1;
                    if (step["expect"] is JsonObject rawExpect)
                    {
                        var substituted = Substitute(rawExpect) as JsonObject ?? rawExpect;
                        try
                        {
                            CheckExpect(status, body, substituted);
                        }
                        catch (InvalidOperationException error)
                        {
                            if (attempt < pollTimes)
                            {
                                await Task.Delay(pollIntervalMs);
                                continue;
                            }
                            throw new InvalidOperationException(
                                $"{name}: step {index} ({op}) expectation failed "
                                    + $"(status {status}): {error.Message}");
                        }
                    }
                    break;
                }
                if (step["bind"] is { } bind)
                {
                    _bound[bind.GetValue<string>()] = body;
                }
                index += 1;
            }
        }
    }
}
