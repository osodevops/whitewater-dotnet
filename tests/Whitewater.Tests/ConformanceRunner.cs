using System.Text.Json;
using System.Text.Json.Nodes;
using Whitewater;
using Whitewater.Internal;
using Xunit;

namespace Whitewater.Tests;

/// <summary>
/// Executes the shared fixture corpus under sdk/spec/conformance/ in
/// whitewater against a live Riverbed, per the fixture vocabulary in
/// conformance/README.md. Set WHITEWATER_CONFORMANCE_URLS
/// (comma-separated Node base URLs) and WHITEWATER_FIXTURE_DIR; skipped
/// otherwise. The admin key comes from FINNSTREAM_ADMIN_API_KEY with the
/// documented development default.
/// </summary>
public class ConformanceRunner
{
    private static readonly string? BaseUrls = Environment.GetEnvironmentVariable(
        "WHITEWATER_CONFORMANCE_URLS");
    private static readonly string? FixtureDir = Environment.GetEnvironmentVariable(
        "WHITEWATER_FIXTURE_DIR");
    private static readonly string ApiKey = Environment.GetEnvironmentVariable(
        "FINNSTREAM_ADMIN_API_KEY") ?? "whitewater-local-development-admin-key";

    [Fact]
    public async Task SharedScenariosPassAgainstTheRiverbed()
    {
        if (BaseUrls == null || FixtureDir == null)
        {
            return;
        }
        var endpoints = BaseUrls.Split(',').Select(raw => new Uri(raw.Trim())).ToArray();
        var files = Directory.GetFiles(FixtureDir, "*.json").OrderBy(f => f).ToArray();
        Assert.NotEmpty(files);
        foreach (var file in files)
        {
            var scenario = JsonNode.Parse(await File.ReadAllTextAsync(file))!;
            var apiKey = scenario["api_key"]?.GetValue<string>() ?? ApiKey;
            var client = new WhitewaterClient(apiKey, endpoints, TimeSpan.FromSeconds(30));
            await RunScenario(client, scenario);
        }
    }

    private static async Task RunScenario(WhitewaterClient client, JsonNode scenario)
    {
        var name = scenario["scenario"]!.GetValue<string>();
        var context = new ScenarioContext();
        var steps = scenario["steps"] as JsonArray
            ?? throw new InvalidOperationException($"{name}: no steps");
        var index = 0;
        foreach (var stepNode in steps)
        {
            var step = stepNode!.AsObject();
            var op = step["op"]!.GetValue<string>();
            index += 1;
            var expect = step["expect"] as JsonObject;
            var errorCode = expect?["error_code"]?.GetValue<string>();
            var errorStatus = expect?["error_status"]?.GetValue<int>();
            var expectsError = errorCode != null || errorStatus != null;
            JsonNode? result;
            try
            {
                result = await Call(client, Substitute(step, context));
                if (expectsError)
                {
                    throw new InvalidOperationException(
                        $"{name}: step {index} ({op}) succeeded but expected " +
                        $"error {expect?.ToJsonString()}");
                }
            }
            catch (WhitewaterException error) when (expectsError)
            {
                if (errorCode != null && error.Code != errorCode)
                {
                    throw new InvalidOperationException(
                        $"{name}: step {index} ({op}) failed with " +
                        $"{error.Code ?? error.GetType().Name}, expected error_code {errorCode}");
                }
                if (errorStatus != null && error.Status != errorStatus.Value)
                {
                    throw new InvalidOperationException(
                        $"{name}: step {index} ({op}) failed with HTTP " +
                        $"{error.Status}, expected error_status {errorStatus}");
                }
                result = new JsonObject { ["error_code"] = error.Code };
            }
            catch (Exception error)
            {
                throw new InvalidOperationException(
                    $"{name}: step {index} ({op}) failed: {error.Message}", error);
            }
            if (step["bind"] is { } bind)
            {
                context.Bindings[bind.GetValue<string>()] = result;
            }
            if (expect == null)
            {
                continue;
            }
            foreach (var entry in expect)
            {
                if (entry.Key == "error_code" || entry.Key == "error_status")
                {
                    continue;
                }
                var expected = entry.Value == null
                    ? null
                    : Substitute(entry.Value.DeepClone(), context);
                CheckExpectation(name, index, op, entry.Key, expected, result, context);
            }
        }
    }

    private static void CheckExpectation(
        string name, int index, string op, string path, JsonNode? expected,
        JsonNode? result, ScenarioContext context)
    {
        var actual = Dig(result, path);
        var fail = (string detail) => new InvalidOperationException(
            $"{name}: step {index} ({op}) expectation '{path}' failed: {detail}");
        var expectedText = expected is JsonValue expectedValue
            && expectedValue.TryGetValue<string>(out var expectedString)
                ? expectedString
                : null;
        switch (expectedText)
        {
            case "!=null":
                if (actual == null)
                {
                    throw fail("expected non-null, got null");
                }
                return;
            case "present":
                if (actual == null)
                {
                    throw fail("expected present, got absent");
                }
                return;
            case "absent":
                if (actual != null)
                {
                    throw fail($"expected absent, got {actual.ToJsonString()}");
                }
                return;
            case { } text when text.StartsWith("len>="):
            {
                var bound = int.Parse(text[5..]);
                var length = actual switch
                {
                    JsonArray array => array.Count,
                    JsonObject obj => obj.Count,
                    JsonValue textValue when textValue.TryGetValue<string>(out var innerText)
                        => innerText.Length,
                    _ => -1,
                };
                if (length < bound)
                {
                    throw fail($"expected len>={bound}, got {length}");
                }
                return;
            }
            case { } text when text.StartsWith("=="):
            {
                var bound = Dig(context.Bindings, text[2..]);
                if (!LooseEquals(bound, actual))
                {
                    throw fail($"expected {text}=" +
                        $"{bound?.ToJsonString() ?? "null"}, got " +
                        $"{actual?.ToJsonString() ?? "null"}");
                }
                return;
            }
            default:
                if (!LooseEquals(expected, actual))
                {
                    throw fail($"expected {expected?.ToJsonString() ?? "null"}, got " +
                        $"{actual?.ToJsonString() ?? "null"}");
                }
                return;
        }
    }

    private static async Task<JsonNode?> Call(WhitewaterClient client, JsonNode step)
    {
        var transport = client.Transport;
        var args = step["args"] as JsonObject ?? new JsonObject();
        switch (step["op"]!.GetValue<string>())
        {
            case "wcl":
                return ResultNode(await client.Admin.WclAsync(
                    args["script"]!.GetValue<string>()));
            case "admin.commands":
            {
                var commands = new List<JsonObject>();
                foreach (var command in args["commands"]!.AsArray())
                {
                    commands.Add(command!.AsObject());
                }
                return ResultNode(await client.Admin.CommandsAsync(commands));
            }
            case "feed.append":
            {
                var record = new FeedAppend(
                    Guid.Parse(args["request_id"]!.GetValue<string>()),
                    Guid.Parse(args["writer_session_id"]!.GetValue<string>()),
                    args["writer_epoch"]!.GetValue<long>(),
                    args["sequence"]!.GetValue<long>(),
                    DecodeBytes(args["key"]),
                    DecodeBytes(args["payload"])!,
                    args["event_time_ns"] is { } ns ? long.Parse(ns.GetValue<string>()) : null,
                    DecodeMetadata(args["metadata"] as JsonObject));
                return ResultNode(await client.Feeds.AppendAsync(
                    args["feed"]!.GetValue<string>(), record));
            }
            case "reader.temporary_fetch":
            {
                var options = new TemporaryFetchOptions(
                    After: args["after"] is { } a ? new Cursor(a.GetValue<string>()) : null,
                    Limit: LimitArg(args),
                    NewOnly: args["new_only"]?.GetValue<bool>() ?? false,
                    Tail: args["tail"]?.GetValue<bool>() ?? false,
                    WaitMs: args["wait_ms"]?.GetValue<int>(),
                    AfterEventTimeNs: args["after_event_time_ns"] is { } t
                        ? long.Parse(t.GetValue<string>()) : null);
                return ResultNode(await client.Feeds.TemporaryFetchAsync(
                    args["feed"]!.GetValue<string>(), options));
            }
            case "writer.append":
            {
                var writer = SessionWriter(client, args);
                return ResultNode(await writer.AppendAsync(
                    DecodeBytes(args["key"]),
                    DecodeBytes(args["payload"])!,
                    Guid.Parse(args["request_id"]!.GetValue<string>()),
                    args["event_time_ns"] is { } t ? long.Parse(t.GetValue<string>()) : null,
                    DecodeMetadata(args["metadata"] as JsonObject)));
            }
            case "writer.append_batch":
            {
                var records = new List<WriterSessionClient.AppendInput>();
                foreach (var recordNode in args["records"]!.AsArray())
                {
                    var record = recordNode!.AsObject();
                    var writer = SessionWriter(client, record);
                    records.Add(new WriterSessionClient.AppendInput(
                        DecodeBytes(record["key"]),
                        DecodeBytes(record["payload"])!,
                        Guid.Parse(record["request_id"]!.GetValue<string>()),
                        record["event_time_ns"] is { } t
                            ? long.Parse(t.GetValue<string>()) : null,
                        DecodeMetadata(record["metadata"] as JsonObject)));
                }
                // Batch writer/session come from the first record; every
                // record in a fixture batch shares them.
                var batchWriter = SessionWriter(client, args["records"]![0]!.AsObject());
                return ResultNode(await batchWriter.AppendBatchAsync(records));
            }
            case "reader.open":
                return ResultNode(await client.Reader(
                        args["reader"]!.GetValue<string>())
                    .OpenAsync(args["capacity"]?.GetValue<int>() ?? 64));
            case "reader.fetch":
                return ResultNode(await client.Reader(args["reader"]!.GetValue<string>())
                    .FetchAsync(SessionFromArgs(args), LimitArg(args)));
            case "reader.ack":
                return ResultNode(await client.Reader(args["reader"]!.GetValue<string>())
                    .AckAsync(SessionFromArgs(args), new Cursor(args["cursor"]!.GetValue<string>())));
            case "reader.close":
                return ResultNode(await client.Reader(args["reader"]!.GetValue<string>())
                    .CloseAsync(SessionFromArgs(args)));
            case "member.join":
                return ResultNode(await Member(client, args).JoinAsync(
                    Guid.Parse(args["member_id"]!.GetValue<string>())));
            case "member.claim":
            {
                var lease = await Member(client, args).ClaimAsync(JoinedFromArgs(args),
                    Guid.Parse(args["work_id"]!.GetValue<string>()),
                    args["lease_ticks"] is { } ticks ? ticks.GetValue<int>() : null);
                return new JsonObject { ["lease"] = ResultNode(lease) };
            }
            case "member.renew":
            {
                var lease = await Member(client, args)
                    .RenewAsync(JoinedFromArgs(args), LeaseFromArgs(args));
                return new JsonObject { ["lease"] = ResultNode(lease) };
            }
            case "member.fetch":
                return ResultNode(await Member(client, args).FetchAsync(
                    JoinedFromArgs(args), LeaseFromArgs(args), LimitArg(args)));
            case "member.ack":
            {
                var page = new MemberPage(
                    new List<FeedRecord>(),
                    Cursor.Of(args["cursor"]?.GetValue<string>()),
                    DecodePositions(args["positions"] as JsonObject),
                    args["ownership_epoch"]?.GetValue<long>() ?? 0);
                await Member(client, args)
                    .AckAsync(JoinedFromArgs(args), LeaseFromArgs(args), page);
                return new JsonObject();
            }
            case "member.release":
                await Member(client, args)
                    .ReleaseAsync(JoinedFromArgs(args), LeaseFromArgs(args));
                return new JsonObject();
            case "feed.records":
            {
                var path = "/v1/feeds/records?feed="
                    + Uri.EscapeDataString(args["feed"]!.GetValue<string>());
                if (args["limit"] is { } limit)
                {
                    path += "&limit=" + limit.GetValue<int>();
                }
                // Bound raw: fixture paths reference wire field names.
                var raw = await transport.GetAsync(path);
                return raw is JsonObject ? raw : new JsonObject { ["records"] = raw?.DeepClone() };
            }
            default:
                throw new InvalidOperationException(
                    $"unsupported op {step["op"]!.GetValue<string>()}");
        }
    }

    private static int? LimitArg(JsonNode args)
        => args["limit"] is { } limit ? limit.GetValue<int>() : null;

    private static WriterSessionClient SessionWriter(WhitewaterClient client, JsonNode args)
    {
        return client.Writer(
            args["writer"]!.GetValue<string>(),
            args["session_epoch"]!.GetValue<long>());
    }

    private static SubscriptionMemberClient Member(WhitewaterClient client, JsonNode args)
    {
        return client.SubscriptionMember(args["subscription"]!.GetValue<string>());
    }

    private static ReaderSession SessionFromArgs(JsonNode args)
    {
        return new ReaderSession(
            Guid.NewGuid(), Guid.NewGuid(), args["session_epoch"]!.GetValue<long>(), 0,
            null, null);
    }

    private static Joined JoinedFromArgs(JsonNode args)
    {
        return new Joined(
            Guid.Parse(args["member_id"]!.GetValue<string>()),
            args["member_epoch"]!.GetValue<long>(), 0, 0);
    }

    private static WorkLease LeaseFromArgs(JsonNode args)
    {
        return new WorkLease(
            Guid.Parse(args["work_id"]!.GetValue<string>()),
            Guid.Parse(args["member_id"]!.GetValue<string>()),
            args["member_epoch"]!.GetValue<long>(),
            args["lease_epoch"]!.GetValue<long>(), 0);
    }

    // -- fixture vocabulary ---------------------------------------------

    /// <summary>Decodes a fixture byte wrapper: utf8, hex, or base64.</summary>
    private static byte[]? DecodeBytes(JsonNode? node)
    {
        switch (node)
        {
            case null:
                return null;
            case JsonValue value when value.TryGetValue<string>(out var base64):
                return Convert.FromBase64String(base64);
            case JsonObject wrapper:
                if (wrapper["utf8"] is { } utf8)
                {
                    return System.Text.Encoding.UTF8.GetBytes(utf8.GetValue<string>());
                }
                if (wrapper["hex"] is { } hex)
                {
                    return Convert.FromHexString(hex.GetValue<string>());
                }
                if (wrapper["base64"] is { } b64)
                {
                    return Convert.FromBase64String(b64.GetValue<string>());
                }
                throw new InvalidOperationException(
                    $"unknown byte wrapper {wrapper.ToJsonString()}");
            default:
                throw new InvalidOperationException(
                    $"cannot decode bytes from {node.ToJsonString()}");
        }
    }

    private static Dictionary<string, byte[]>? DecodeMetadata(JsonObject? metadata)
    {
        if (metadata == null)
        {
            return null;
        }
        var decoded = new Dictionary<string, byte[]>();
        foreach (var entry in metadata)
        {
            decoded[entry.Key] = DecodeBytes(entry.Value)!;
        }
        return decoded;
    }

    private static Dictionary<string, string> DecodePositions(JsonObject? positions)
    {
        var decoded = new Dictionary<string, string>();
        if (positions == null)
        {
            return decoded;
        }
        foreach (var entry in positions)
        {
            decoded[entry.Key] = entry.Value!.GetValue<string>();
        }
        return decoded;
    }

    // -- result -> wire-name binding ------------------------------------

    private static readonly JsonSerializerOptions ResultOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DictionaryKeyPolicy = JsonNamingPolicy.SnakeCaseLower,
        Converters = { new CursorJsonConverter(), new JsonNodeConverter() },
    };

    private static JsonNode ResultNode(object value)
        => JsonNode.Parse(JsonSerializer.Serialize(value, ResultOptions))!;

    private sealed class CursorJsonConverter
        : System.Text.Json.Serialization.JsonConverter<Cursor>
    {
        public override Cursor Read(ref Utf8JsonReader reader, Type typeToConvert,
            JsonSerializerOptions options)
            => new(reader.GetString()!);
        public override void Write(Utf8JsonWriter writer, Cursor value,
            JsonSerializerOptions options)
            => writer.WriteStringValue(value.Value);
    }

    private sealed class JsonNodeConverter
        : System.Text.Json.Serialization.JsonConverter<JsonNode>
    {
        public override JsonNode Read(ref Utf8JsonReader reader, Type typeToConvert,
            JsonSerializerOptions options)
            => JsonNode.Parse(ref reader)!;
        public override void Write(Utf8JsonWriter writer, JsonNode value,
            JsonSerializerOptions options)
            => value.WriteTo(writer);
    }

    // -- substitution + deep comparison ---------------------------------

    private sealed class ScenarioContext
    {
        public string Token { get; } = $"t{Guid.NewGuid():N}"[..12];
        public Dictionary<string, Guid> NamedIds { get; } = new();
        public Dictionary<string, JsonNode?> Bindings { get; } = new();
    }

    private static JsonNode Substitute(JsonNode node, ScenarioContext context)
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
                        : Substitute(entry.Value, context);
                }
                return copy;
            }
            case JsonArray array:
            {
                var copy = new JsonArray();
                foreach (var item in array)
                {
                    copy.Add(item == null ? null : Substitute(item, context));
                }
                return copy;
            }
            case JsonValue value:
            {
                if (value.TryGetValue<string>(out var text))
                {
                    if (text.StartsWith("{{") && text.EndsWith("}}"))
                    {
                        return Resolve(text, context);
                    }
                    return JsonValue.Create(text.Replace("{{uniq}}", context.Token))!;
                }
                return value.DeepClone();
            }
            default:
                return node.DeepClone();
        }
    }

    private static JsonNode Resolve(string template, ScenarioContext context)
    {
        var inner = template[2..^2];
        if (inner == "uniq")
        {
            return JsonValue.Create(context.Token)!;
        }
        if (inner == "uuid")
        {
            return JsonValue.Create(Guid.NewGuid().ToString())!;
        }
        if (inner.StartsWith("uuid:"))
        {
            var name = inner[5..];
            if (!context.NamedIds.TryGetValue(name, out var id))
            {
                id = Guid.NewGuid();
                context.NamedIds[name] = id;
            }
            return JsonValue.Create(id.ToString())!;
        }
        var bound = Dig(context.Bindings, inner)
            ?? throw new InvalidOperationException($"unresolved token {template}");
        // Whole-node splice: objects/arrays substitute verbatim; scalars
        // come back as their JSON value.
        return bound.DeepClone();
    }

    private static JsonNode? Dig(JsonNode? node, string path)
    {
        var current = node;
        foreach (var segment in path.Split('.'))
        {
            if (current == null)
            {
                return null;
            }
            if (segment.EndsWith(']') && segment.Contains('['))
            {
                var bracket = segment.IndexOf('[');
                var key = segment[..bracket];
                var index = int.Parse(segment[(bracket + 1)..^1]);
                current = current[key];
                current = current is JsonArray array && index < array.Count
                    ? array[index]
                    : null;
            }
            else
            {
                current = current[segment];
            }
        }
        return current;
    }

    private static JsonNode? Dig(Dictionary<string, JsonNode?> bindings, string path)
    {
        var dot = path.IndexOf('.');
        var head = dot < 0 ? path : path[..dot];
        if (!bindings.TryGetValue(head, out var node) || node == null)
        {
            return null;
        }
        return dot < 0 ? node : Dig(node, path[(dot + 1)..]);
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
            // Loose numeric compare: i64 values may arrive as decimal
            // strings or JSON numbers.
            if (TryNumber(expectedValue, out var expectedNumber)
                && TryNumber(actualValue, out var actualNumber))
            {
                return expectedNumber == actualNumber;
            }
        }
        return expected.ToJsonString() == actual.ToJsonString();
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
}
