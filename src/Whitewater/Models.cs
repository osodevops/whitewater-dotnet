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
