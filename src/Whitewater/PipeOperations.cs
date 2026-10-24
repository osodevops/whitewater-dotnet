using System.Text.Json.Nodes;

namespace Whitewater;

/// <summary>Which record clock a <c>window</c> Pipe buckets into panes.</summary>
public enum WindowTime
{
    /// <summary><c>event_time_ns</c> — the application-stamped occurrence
    /// clock (default).</summary>
    Event,

    /// <summary><c>ingest_time_ns</c> — the durable-arrival clock.</summary>
    Ingest,
}

/// <summary>How fresh an <c>enrich</c> Pipe lookup row must be before it
/// is journaled.</summary>
public enum EnrichFreshness
{
    /// <summary>Serve the replica's applied state (default; resolution is
    /// journaled either way).</summary>
    Latest,

    /// <summary>Require applied to cover committed at read entry; lag
    /// yields a retryable drive failure.</summary>
    Strict,
}

/// <summary>What an <c>enrich</c> Pipe lookup miss does to the output
/// record.</summary>
public enum EnrichMiss
{
    /// <summary>Forward the record with <c>enrich.matched = false</c>
    /// (default).</summary>
    Forward,

    /// <summary>Consume without emitting; the input frontier still
    /// commits.</summary>
    Drop,

    /// <summary>Route the miss to the declared quarantine Feed.</summary>
    Quarantine,
}

/// <summary>Wire names for the Pipe operation enums.</summary>
internal static class PipeWireNames
{
    public static string Wire(this WindowTime time) => time switch
    {
        WindowTime.Ingest => "ingest",
        _ => "event",
    };

    public static string Wire(this EnrichFreshness freshness) => freshness switch
    {
        EnrichFreshness.Strict => "strict",
        _ => "latest",
    };

    public static string Wire(this EnrichMiss miss) => miss switch
    {
        EnrichMiss.Drop => "drop",
        EnrichMiss.Quarantine => "quarantine",
        _ => "forward",
    };
}

/// <summary>
/// What a <c>window</c> Pipe adds to each pane's accumulator:
/// <c>count</c> records, <c>sum</c> an extracted integer, or
/// <c>average</c> tracking count+sum (readers divide).
/// </summary>
public sealed record WindowAggregation
{
    private WindowAggregation(string aggregation, Extractor? value)
    {
        Aggregation = aggregation;
        Value = value;
    }

    internal string Aggregation { get; }
    internal Extractor? Value { get; }

    /// <summary>Count records per (group Key, pane).</summary>
    public static WindowAggregation Count() => new("count", null);

    /// <summary>Sum one extracted integer value per pane.</summary>
    public static WindowAggregation Sum(Extractor value)
        => new("sum", value ?? throw new ArgumentNullException(nameof(value)));

    /// <summary>Track count and integer sum per pane; readers divide.</summary>
    public static WindowAggregation Average(Extractor value)
        => new("average", value ?? throw new ArgumentNullException(nameof(value)));

    /// <summary>The wire form:
    /// <c>{"aggregation":"sum","value":{…}}</c>.</summary>
    public JsonObject ToWire()
    {
        var node = new JsonObject { ["aggregation"] = Aggregation };
        if (Value != null)
        {
            node["value"] = Value.ToWire();
        }
        return node;
    }
}

/// <summary>
/// The processing step a Pipe applies to each delivered input record.
/// Build with <see cref="Forward"/>, <see cref="Enrich"/>, or
/// <see cref="Window"/>; the wire form is internally tagged on
/// <c>operation</c>.
/// </summary>
public sealed record PipeOperation
{
    private PipeOperation(string operation) => Operation = operation;

    /// <summary>The wire operation kind: <c>forward</c>, <c>enrich</c>,
    /// or <c>window</c>.</summary>
    internal string Operation { get; }

    internal Extractor? Key { get; init; }
    internal long? WindowNs { get; init; }
    internal WindowAggregation? Aggregation { get; init; }
    internal WindowTime? Time { get; init; }
    internal long? MaxLateNs { get; init; }
    internal long? WatermarkNs { get; init; }
    internal long? GraceNs { get; init; }
    internal long? ExpireNs { get; init; }
    internal long? KeyLimit { get; init; }
    internal long? ByteLimit { get; init; }
    internal EnrichFreshness? Freshness { get; init; }
    internal string? Named { get; init; }
    internal EnrichMiss? OnMiss { get; init; }

    /// <summary>Copy each delivered record to the output Feed
    /// unchanged.</summary>
    public static PipeOperation Forward() => new("forward");

    /// <summary>
    /// Point-lookup enrichment against a StateStore: resolve one row by
    /// the projected <paramref name="key"/> and copy its payload into
    /// output Metadata (<c>enrich.payload</c>, <c>enrich.matched</c>).
    /// The lookup resolution rides inside the effect journal, so
    /// replayed outputs are byte-identical. <paramref name="named"/>
    /// resolves through a declared secondary key instead of the primary;
    /// <paramref name="onMiss"/> picks the miss disposition
    /// (<see cref="EnrichMiss.Quarantine"/> needs the define's
    /// <c>quarantine_feed</c>).
    /// </summary>
    public static PipeOperation Enrich(
        Extractor key,
        EnrichFreshness freshness = EnrichFreshness.Latest,
        string? named = null,
        EnrichMiss onMiss = EnrichMiss.Forward)
        => new("enrich")
        {
            Key = key ?? throw new ArgumentNullException(
                nameof(key), "enrich key extractor is required"),
            Freshness = freshness,
            Named = named,
            OnMiss = onMiss,
        };

    /// <summary>
    /// Accumulate each delivered record into fixed
    /// <paramref name="windowNs"/>-wide panes of a managed StateStore
    /// view. Panes are immutable once written; <paramref name="maxLateNs"/>
    /// bounds late-event policy (later arrivals quarantine into a
    /// countable <c>late</c> row), <paramref name="watermarkNs"/> drives a
    /// durable close horizon with <paramref name="graceNs"/> bounding
    /// emitted-pane revision, and <paramref name="expireNs"/> bounds pane
    /// retention once the pane provably closes.
    /// </summary>
    public static PipeOperation Window(
        long windowNs,
        Extractor key,
        WindowAggregation aggregation,
        WindowTime time = WindowTime.Event,
        long? maxLateNs = null,
        long? watermarkNs = null,
        long? graceNs = null,
        long? expireNs = null,
        long? keyLimit = null,
        long? byteLimit = null)
    {
        if (windowNs <= 0)
        {
            throw new ArgumentException("windowNs must be positive", nameof(windowNs));
        }
        return new PipeOperation("window")
        {
            Key = key ?? throw new ArgumentNullException(
                nameof(key), "window key extractor is required"),
            Aggregation = aggregation ?? throw new ArgumentNullException(
                nameof(aggregation), "aggregation is required"),
            WindowNs = windowNs,
            Time = time,
            MaxLateNs = maxLateNs,
            WatermarkNs = watermarkNs,
            GraceNs = graceNs,
            ExpireNs = expireNs,
            KeyLimit = keyLimit,
            ByteLimit = byteLimit,
        };
    }

    /// <summary>The wire form:
    /// <c>{"operation":"window","window_ns":…}</c>.</summary>
    public JsonObject ToWire()
    {
        var node = new JsonObject { ["operation"] = Operation };
        if (WindowNs != null)
        {
            node["window_ns"] = WindowNs.Value;
        }
        if (Key != null)
        {
            node["key"] = Key.ToWire();
        }
        if (Aggregation != null)
        {
            node["aggregation"] = Aggregation.ToWire();
        }
        if (Time != null)
        {
            node["time"] = Time.Value.Wire();
        }
        if (MaxLateNs != null)
        {
            node["max_late_ns"] = MaxLateNs.Value;
        }
        if (WatermarkNs != null)
        {
            node["watermark_ns"] = WatermarkNs.Value;
        }
        if (GraceNs != null)
        {
            node["grace_ns"] = GraceNs.Value;
        }
        if (ExpireNs != null)
        {
            node["expire_ns"] = ExpireNs.Value;
        }
        if (KeyLimit != null)
        {
            node["key_limit"] = KeyLimit.Value;
        }
        if (ByteLimit != null)
        {
            node["byte_limit"] = ByteLimit.Value;
        }
        if (Freshness != null)
        {
            node["freshness"] = Freshness.Value.Wire();
        }
        if (Named != null)
        {
            node["named"] = Named;
        }
        if (OnMiss != null)
        {
            node["on_miss"] = OnMiss.Value.Wire();
        }
        return node;
    }
}
