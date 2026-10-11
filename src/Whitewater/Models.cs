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
