namespace Whitewater;

/// <summary>
/// Where a Reader or Subscription starts consuming a Feed — maps to the
/// WCL <c>START AT</c> clause. Build with the static factories.
/// </summary>
public sealed class ReaderStart
{
    public enum StartKind { Beginning, Now, Cursor, Timestamp }

    private ReaderStart(StartKind kind, string? cursor, long timestampNs)
    {
        Kind = kind;
        CursorValue = cursor;
        TimestampNsValue = timestampNs;
    }

    /// <summary>From the oldest retained record.</summary>
    public static ReaderStart Beginning() => new(StartKind.Beginning, null, 0);

    /// <summary>From records appended after creation.</summary>
    public static ReaderStart Now() => new(StartKind.Now, null, 0);

    /// <summary>From an opaque Cursor previously returned by the Riverbed.</summary>
    public static ReaderStart Cursor(string cursor)
        => new(StartKind.Cursor, cursor, 0);

    /// <summary>From a signed Unix epoch nanosecond timestamp.</summary>
    public static ReaderStart TimestampNs(long nanos) => new(StartKind.Timestamp, null, nanos);

    public StartKind Kind { get; }

    /// <summary>Set when <see cref="Kind"/> is <c>Cursor</c>.</summary>
    public string? CursorValue { get; }

    /// <summary>Set when <see cref="Kind"/> is <c>Timestamp</c>.</summary>
    public long TimestampNsValue { get; }

    /// <summary>The WCL <c>START AT</c> operand, e.g. <c>BEGINNING</c>.</summary>
    public string Wcl => Kind switch
    {
        StartKind.Now => "NOW",
        StartKind.Cursor => "CURSOR '" + (CursorValue ?? "").Replace("'", "''") + "'",
        StartKind.Timestamp => $"TIMESTAMP {TimestampNsValue}",
        _ => "BEGINNING",
    };
}
