using System.Text.Json.Nodes;

namespace Whitewater;

/// <summary>
/// An opaque stream position. Cursors are never parsed, ordered, or
/// fabricated — they pass back to the Riverbed verbatim.
/// </summary>
public readonly record struct Cursor(string Value)
{
    public static Cursor? Of(string? value) => value is null ? null : new Cursor(value);
    public override string ToString() => Value;
}

/// <summary>
/// One immutable Feed record. <see cref="EventTimeNs"/>/
/// <see cref="IngestTimeNs"/> are signed 64-bit nanoseconds; on the wire
/// they travel as decimal strings (JSON numbers lose i64 range).
/// </summary>
public sealed record FeedRecord(
    Cursor Cursor,
    Guid MessageId,
    Guid ProducerId,
    long Sequence,
    long EventTimeNs,
    long IngestTimeNs,
    byte[]? Key,
    byte[] Payload,
    IReadOnlyDictionary<string, byte[]> Metadata);

public sealed record ServerFeedback(
    int RecommendedBatchCount,
    int RecommendedBatchBytes,
    double Pressure,
    int RetryAfterMs,
    long MaxFrameBytes,
    bool HotKey = false);

public sealed record AppendResult(
    Guid MessageId,
    Cursor Cursor,
    bool Deduplicated,
    ServerFeedback Feedback);

public sealed record AppendBatchResult(
    IReadOnlyList<AppendResult> Results,
    ServerFeedback Feedback);

public sealed record ReaderSession(
    Guid ReaderId,
    Guid FeedId,
    long SessionEpoch,
    int Capacity,
    Cursor? DeliveredCursor,
    Cursor? AcknowledgedCursor);

public sealed record ReaderPage(
    IReadOnlyList<FeedRecord> Records,
    Cursor? DeliveredCursor,
    Cursor? AcknowledgedCursor,
    int RecommendedCapacity,
    int RetryAfterMs);

/// <summary>One executed WCL statement; <paramref name="Data"/> is the
/// raw per-statement payload, interpreted by callers per statement.</summary>
public sealed record StatementResult(string Statement, string Message, JsonNode Data);

public sealed record ControlExecution(
    Guid RequestId,
    long Revision,
    string Authority,
    string Warning,
    IReadOnlyList<StatementResult> Results);

public sealed record Joined(
    Guid MemberId,
    long MemberEpoch,
    long OwnershipEpoch,
    long Tick);

public sealed record WorkLease(
    Guid WorkId,
    Guid MemberId,
    long MemberEpoch,
    long LeaseEpoch,
    long ExpiresAtTick);

public sealed record MemberPage(
    IReadOnlyList<FeedRecord> Records,
    Cursor? Cursor,
    IReadOnlyDictionary<string, string> Positions,
    long OwnershipEpoch);

/// <summary>
/// One ad-hoc append for <c>POST /v1/feeds/append</c>: the caller
/// supplies the writer identity directly instead of holding a named
/// Writer session. Identical <paramref name="RequestId"/> retries dedupe
/// server-side.
/// </summary>
public sealed record FeedAppend(
    Guid RequestId,
    Guid WriterSessionId,
    long WriterEpoch,
    long Sequence,
    byte[]? Key,
    byte[] Payload,
    long? EventTimeNs = null,
    IReadOnlyDictionary<string, byte[]>? Metadata = null);

/// <summary>
/// Options for an anonymous temporary Reader fetch. <paramref name="After"/>
/// continues after an opaque Cursor; <paramref name="AfterEventTimeNs"/>
/// seeks by record event time; <paramref name="NewOnly"/> positions at the
/// current end without returning history; <paramref name="Tail"/> plus
/// <paramref name="WaitMs"/> long-polls for newly committed records.
/// </summary>
public sealed record TemporaryFetchOptions(
    Cursor? After = null,
    int? Limit = null,
    bool NewOnly = false,
    bool Tail = false,
    int? WaitMs = null,
    long? AfterEventTimeNs = null)
{
    /// <summary>Read a bounded page after <paramref name="after"/>.</summary>
    public static TemporaryFetchOptions Page(Cursor? after, int limit)
        => new(After: after, Limit: limit);

    /// <summary>Long-poll for newly committed records after <paramref name="after"/>.</summary>
    public static TemporaryFetchOptions Tailed(Cursor? after, int limit, int waitMs)
        => new(After: after, Limit: limit, Tail: true, WaitMs: waitMs);

    /// <summary>Position at the current end Cursor without returning records.</summary>
    public static TemporaryFetchOptions AtEnd() => new(NewOnly: true);
}

public sealed record TemporaryFetchPage(
    IReadOnlyList<FeedRecord> Records,
    Cursor? NextCursor);

/// <summary>Bounds the named secondary keys one StateStore declares.</summary>
public static class StateStoreLimits
{
    public const int MaxNamedKeys = 10;
}

/// <summary>
/// One declared secondary key on a StateStore. Non-unique keys map a
/// value to every primary row carrying it (the group/list lookup);
/// <paramref name="Unique"/> enforces one row per value, arbitrated
/// first-committed-wins by the store's journal order.
///
/// <paramref name="Extractor"/> is feed-sourced stores only: the
/// projection server-side replay applies to each committed source
/// record. Manual stores take values on each Put instead.
/// </summary>
public sealed record NamedKey(string Name, bool Unique, Extractor? Extractor = null);

/// <summary>
/// How a feed-sourced StateStore projects one committed source record
/// to a named key's typed lookup values — declared on
/// <c>AddKey(..., extractor)</c> and applied by server-side replay.
/// Build with <see cref="RecordKey"/>, <see cref="Metadata(string)"/>,
/// or <see cref="Json(string)"/>.
/// </summary>
public sealed record Extractor
{
    private Extractor(string kind, string? name, string? extractor, int? version, string? field)
    {
        Kind = kind;
        Name = name;
        ExtractorName = extractor;
        Version = version;
        Field = field;
    }

    internal string Kind { get; }
    internal string? Name { get; }
    internal string? ExtractorName { get; }
    internal int? Version { get; }
    internal string? Field { get; }

    /// <summary>The source record's application Key.</summary>
    public static Extractor RecordKey() => new("key", null, null, null, null);

    /// <summary>A named record Metadata entry.</summary>
    public static Extractor Metadata(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return new("metadata", name, null, null, null);
    }

    /// <summary>A dotted JSON-path projection over the record payload.</summary>
    public static Extractor Json(string field)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(field);
        return new("projection", null, "json", 1, field);
    }

    /// <summary>Serialize to the wire IndexFieldSpec object.</summary>
    internal JsonObject ToWire()
    {
        var node = new JsonObject { ["kind"] = Kind };
        if (Name != null) node["name"] = Name;
        if (ExtractorName != null) node["extractor"] = ExtractorName;
        if (Version != null) node["version"] = Version;
        if (Field != null) node["field"] = Field;
        return node;
    }
}

/// <summary>
/// Freshness evidence every StateStore read reports: the serving
/// replica's applied and committed journal positions, whether it
/// answers behind its frontier, and which Node served.
/// </summary>
public sealed record StoreFreshness(
    long AppliedSeq,
    long CommittedSeq,
    bool Behind,
    string ServedBy);

/// <summary>One StateStore row: its primary key and payload bytes plus
/// the serving replica's freshness evidence when the read carried it.</summary>
public sealed record StoreRow(
    byte[] Key,
    byte[] Payload,
    string? AppliedCursor = null,
    StoreFreshness? Freshness = null);

/// <summary>A named-key lookup result: every primary row the value
/// points at plus the serving replica's freshness evidence.</summary>
public sealed record StoreRows(
    IReadOnlyList<StoreRow> Rows,
    StoreFreshness Freshness);

/// <summary>One bounded iteration page in primary-key order.
/// <paramref name="PinnedSeq"/> is the serving replica's applied journal
/// position the page was pinned to — the iteration's freshness evidence.
/// <paramref name="NextKey"/> continues the scan.</summary>
public sealed record StorePage(
    IReadOnlyList<StoreRow> Rows,
    byte[]? NextKey,
    long PinnedSeq,
    string ServedBy);

/// <summary>A committed StateStore mutation: its journal sequence and
/// whether the <paramref name="MutationId"/> idempotency identity
/// deduplicated a retry.</summary>
public sealed record MutationResult(
    Guid StoreId,
    Guid MutationId,
    long Sequence,
    bool Deduplicated);
