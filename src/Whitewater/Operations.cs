using System.Text.Json.Nodes;
using Whitewater.Internal;

namespace Whitewater;

/// <summary>
/// The catalog row <c>DESCRIBE PIPE</c>/<c>SHOW PIPES</c> returns for a
/// declared Pipe. <see cref="Operation"/> stays an opaque
/// <see cref="JsonNode"/> — Forward, Window, and Enrich variants evolve
/// server-side without an SDK bump.
/// </summary>
public sealed record PipeInfo(
    string Name,
    /// <summary><c>declared</c> while driven, <c>paused</c> under
    /// <c>PAUSE PIPE</c>.</summary>
    string Stage,
    Guid SubscriptionId,
    JsonNode? Operation,
    Guid? OutputFeedId,
    Guid? OutputStoreId,
    string Status,
    long CreatedAtNs);

/// <summary>
/// A declared Pipe's lifecycle surface. Pausing is cooperative — an
/// in-flight effect finishes and the frontier stays put so redelivery
/// stays replay-safe — and resume restarts driving from the same
/// durable frontier.
/// </summary>
public sealed class Pipe
{
    private readonly WhitewaterClient _client;

    internal Pipe(WhitewaterClient client, string name)
    {
        _client = client;
        Name = name;
    }

    /// <summary>The declared Pipe name.</summary>
    public string Name { get; }

    /// <summary><c>DESCRIBE PIPE</c>: the live catalog row including
    /// stage and the opaque operation definition.</summary>
    public async Task<PipeInfo> DescribeAsync(CancellationToken ct = default)
    {
        var execution = await _client.CommandsAsync(
            new JsonObject[]
            {
                new() { ["command"] = "describe", ["kind"] = "pipe", ["name"] = Name },
            }, ct: ct).ConfigureAwait(false);
        var data = execution.Results.Count > 0 ? execution.Results[0].Data : null;
        var infos = Operations.DecodePipes(data);
        if (infos.Count == 0)
        {
            throw new WhitewaterException(0, null, "describe pipe returned no definition");
        }
        return infos[0];
    }

    /// <summary><c>PAUSE PIPE</c>: halts every drive cooperatively;
    /// idempotent.</summary>
    public Task PauseAsync(CancellationToken ct = default)
        => LifecycleAsync("pause_pipe", ct);

    /// <summary><c>RESUME PIPE</c>: restarts driving from the durable
    /// frontier.</summary>
    public Task ResumeAsync(CancellationToken ct = default)
        => LifecycleAsync("resume_pipe", ct);

    /// <summary><c>DROP PIPE</c>: retires the declaration and its
    /// compute lease.</summary>
    public async Task DropAsync(CancellationToken ct = default)
    {
        await _client.CommandsAsync(
            new JsonObject[]
            {
                new() { ["command"] = "drop", ["kind"] = "pipe", ["name"] = Name },
            }, ct: ct).ConfigureAwait(false);
    }

    private async Task LifecycleAsync(string command, CancellationToken ct)
    {
        await _client.CommandsAsync(
            new JsonObject[] { new() { ["command"] = command, ["name"] = Name } },
            ct: ct).ConfigureAwait(false);
    }
}

/// <summary>Inclusive/exclusive bound for an Index range query.</summary>
public sealed record IndexBound(object? Value, bool Inclusive)
{
    /// <summary>Marks the bound as inclusive.</summary>
    public static IndexBound InclusiveBound(object? value) => new(value, true);

    /// <summary>Marks the bound as exclusive.</summary>
    public static IndexBound ExclusiveBound(object? value) => new(value, false);
}

/// <summary>
/// The query shape <see cref="WhitewaterClient.QueryIndexAsync"/> lowers
/// onto the wire: every declared field exact, a prefix over leading
/// fields, or a range over the next field after exact leaders. Values
/// accept the same typed scalars as StateStore named values
/// (null/bool/integer/string/byte[]); floats are refused — the wire
/// carries exact i64/text/bytes.
/// </summary>
public sealed record IndexPredicate
{
    internal string Mode { get; init; } = "";
    internal IReadOnlyList<object?> Leading { get; init; } = Array.Empty<object?>();
    internal string? PartialText { get; init; }
    internal IndexBound? Lower { get; init; }
    internal IndexBound? Upper { get; init; }

    /// <summary>Every declared field, exact match.</summary>
    public static IndexPredicate Exact(params object?[] values)
        => new() { Mode = "exact", Leading = values };

    /// <summary>Leading fields exact, plus an optional text prefix on
    /// the next.</summary>
    public static IndexPredicate Prefix(IReadOnlyList<object?> leading, string? partialText = null)
        => new() { Mode = "prefix", Leading = leading, PartialText = partialText };

    /// <summary>Leading fields exact, the next field between
    /// <paramref name="lower"/>/<paramref name="upper"/>.</summary>
    public static IndexPredicate Range(
        IReadOnlyList<object?> leading, IndexBound lower, IndexBound upper)
        => new() { Mode = "range", Leading = leading, Lower = lower, Upper = upper };

    internal JsonObject WireFields()
    {
        var body = new JsonObject { ["mode"] = Mode };
        var leading = new JsonArray();
        foreach (var value in Leading)
        {
            leading.Add(Wire.IndexValue(value));
        }
        switch (Mode)
        {
            case "exact":
                body["values"] = leading;
                break;
            case "prefix":
                body["leading"] = leading;
                if (!string.IsNullOrEmpty(PartialText))
                {
                    body["partial_text"] = PartialText;
                }
                break;
            case "range":
                body["leading"] = leading;
                body["lower"] = new JsonObject
                {
                    ["value"] = Wire.IndexValue(Lower?.Value),
                    ["inclusive"] = Lower?.Inclusive ?? true,
                };
                body["upper"] = new JsonObject
                {
                    ["value"] = Wire.IndexValue(Upper?.Value),
                    ["inclusive"] = Upper?.Inclusive ?? true,
                };
                break;
            default:
                throw new ArgumentException($"unknown index predicate mode: {Mode}");
        }
        return body;
    }
}

/// <summary>One decoded index posting row.</summary>
public sealed record IndexRow(byte[] Key, byte[] Payload);

/// <summary>A page returned by <see cref="WhitewaterClient.QueryIndexAsync"/>.</summary>
public sealed record IndexPage(
    IReadOnlyList<IndexRow> Rows,
    /// <summary>Active generation that served — changes on rebuild.</summary>
    long Generation,
    string? AppliedCursor,
    string? TargetCursor,
    /// <summary>Serving Node still materializing the activation frontier.</summary>
    bool Behind,
    /// <summary>The page hit a server bound; narrow the query to continue.</summary>
    bool Truncated,
    /// <summary>Node identity that served — diagnostics, never routing input.</summary>
    string ServedBy);

/// <summary>
/// One demand row for a declared resource. <see cref="Fields"/> carries
/// the server's counters verbatim — new counters arrive without an SDK
/// update, the same contract as <see cref="PipeInfo.Operation"/>.
/// </summary>
public sealed record DemandRow(
    string Name,
    /// <summary>Owning Domain when the server attributes one.</summary>
    string? Domain,
    JsonObject Fields)
{
    /// <summary>One counter as <see cref="long"/> when present.</summary>
    public long? Counter(string name)
    {
        var value = Fields[name];
        return value switch
        {
            null => null,
            JsonValue v when v.TryGetValue<long>(out var l) => l,
            JsonValue v when v.TryGetValue<int>(out var i) => i,
            _ => null,
        };
    }
}

/// <summary>A demand snapshot for one resource kind: per-resource rows
/// plus the per-domain rollup map.</summary>
public sealed record DemandReport(
    IReadOnlyList<DemandRow> Rows,
    /// <summary><c>{"&lt;domain&gt;": {&lt;counters&gt;}}</c> — opaque
    /// rollup rows keyed by Domain.</summary>
    JsonObject Domains)
{
    /// <summary>One domain's aggregated counters.</summary>
    public JsonObject? Domain(string name) => Domains[name] as JsonObject;
}

/// <summary>
/// Evidence that one Feed Key dominates its traffic window. A strictly
/// ordered hot Key is never parallelized — the honest operator signal
/// is to spread or shed load for the Key, not add capacity.
/// </summary>
public sealed record HotKey(
    /// <summary>The evidence object verbatim — includes any
    /// range-placement fields the server reports; those are
    /// diagnostics, not a public placement contract.</summary>
    JsonObject Evidence)
{
    /// <summary>The Feed carrying the dominant Key, when reported.</summary>
    public string? Feed => Evidence["feed"]?.GetValue<string>();

    /// <summary>Dominant-Key share of the window in basis points.</summary>
    public long? ShareBps => Evidence["share_bps"]?.GetValue<long>();

    /// <summary>Records attributed to the Key inside the sampled window.</summary>
    public long? WindowRecords => Evidence["window_records"]?.GetValue<long>();

    /// <summary>Bytes attributed to the Key inside the sampled window.</summary>
    public long? WindowBytes => Evidence["window_bytes"]?.GetValue<long>();
}

internal static class Operations
{
    /// <summary>Accepts every shape the catalog uses: a bare list, a
    /// <c>{"rows": []}</c> envelope, or a single row object.</summary>
    public static List<PipeInfo> DecodePipes(JsonNode? data)
    {
        var rows = new List<JsonNode?>();
        if (data is JsonArray array)
        {
            rows.AddRange(array);
        }
        else if (data is JsonObject obj && obj["rows"] is JsonArray wrapped)
        {
            rows.AddRange(wrapped);
        }
        else if (data != null)
        {
            rows.Add(data);
        }
        var infos = new List<PipeInfo>();
        foreach (var row in rows)
        {
            if (row is not JsonObject o || o["name"]?.GetValue<string>() is not { } name)
            {
                continue;
            }
            if (!Guid.TryParse(o["subscription_id"]?.GetValue<string>(), out var subscription))
            {
                continue;
            }
            infos.Add(new PipeInfo(
                name,
                o["stage"]?.GetValue<string>() ?? "",
                subscription,
                o["operation"]?.DeepClone(),
                Guid.TryParse(o["output_feed_id"]?.GetValue<string>(), out var feed)
                    ? feed : null,
                Guid.TryParse(o["output_store_id"]?.GetValue<string>(), out var store)
                    ? store : null,
                o["status"]?.GetValue<string>() ?? "",
                o["created_at_ns"]?.GetValue<long>() ?? 0));
        }
        return infos;
    }

    public static IndexPage DecodeIndexPage(JsonNode? body)
    {
        var rows = new List<IndexRow>();
        if (body?["rows"] is JsonArray array)
        {
            foreach (var row in array)
            {
                if (row is not JsonObject o
                    || o["key_base64"]?.GetValue<string>() is not { } key
                    || o["payload_base64"]?.GetValue<string>() is not { } payload)
                {
                    throw new WhitewaterException(0, null, "index row missing key_base64/payload_base64");
                }
                rows.Add(new IndexRow(
                    Convert.FromBase64String(key), Convert.FromBase64String(payload)));
            }
        }
        return new IndexPage(
            rows,
            body?["generation"]?.GetValue<long>() ?? 0,
            body?["applied_cursor"]?.GetValue<string>(),
            body?["target_cursor"]?.GetValue<string>(),
            body?["behind"]?.GetValue<bool>() ?? false,
            body?["truncated"]?.GetValue<bool>() ?? false,
            body?["served_by"]?.GetValue<string>() ?? "");
    }

    public static DemandReport DecodeDemandReport(
        JsonNode? body, string resourceKey, string nameField)
    {
        var rows = new List<DemandRow>();
        if (body?[resourceKey] is JsonArray array)
        {
            foreach (var row in array)
            {
                if (row is not JsonObject o)
                {
                    continue;
                }
                rows.Add(new DemandRow(
                    o[nameField]?.GetValue<string>() ?? "",
                    o["domain"]?.GetValue<string>(),
                    (JsonObject)o.DeepClone()));
            }
        }
        var domains = body?["domains"] as JsonObject ?? new JsonObject();
        return new DemandReport(rows, (JsonObject)domains.DeepClone());
    }

    public static List<HotKey> DecodeHotKeys(JsonNode? body)
    {
        var keys = new List<HotKey>();
        if (body?["hot_keys"] is JsonArray array)
        {
            foreach (var row in array)
            {
                if (row is JsonObject o)
                {
                    keys.Add(new HotKey((JsonObject)o.DeepClone()));
                }
            }
        }
        return keys;
    }
}
