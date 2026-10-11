namespace Whitewater;

/// <summary>
/// Where a Reader or Subscription starts consuming a Feed — maps to the
/// WCL <c>START AT</c> clause. Build with the static factories.
/// </summary>
public sealed class ReaderStart
{
    private ReaderStart(string wcl) => Wcl = wcl;

    /// <summary>From the oldest retained record.</summary>
    public static ReaderStart Beginning() => new("BEGINNING");

    /// <summary>From records appended after creation.</summary>
    public static ReaderStart Now() => new("NOW");

    /// <summary>From an opaque Cursor previously returned by the Riverbed.</summary>
    public static ReaderStart Cursor(string cursor)
        => new("CURSOR '" + cursor.Replace("'", "''") + "'");

    /// <summary>From a signed Unix epoch nanosecond timestamp.</summary>
    public static ReaderStart TimestampNs(long nanos) => new($"TIMESTAMP {nanos}");

    /// <summary>The WCL <c>START AT</c> operand, e.g. <c>BEGINNING</c>.</summary>
    public string Wcl { get; }
}
