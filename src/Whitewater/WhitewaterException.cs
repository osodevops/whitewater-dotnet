using System.Text.Json.Nodes;

namespace Whitewater;

/// <summary>
/// Whitewater error hierarchy mapped from wire (status, code) pairs.
/// <see cref="Unavailable"/> covers transient refusals the caller may
/// retry after <c>feedback.RetryAfterMs</c> guidance;
/// <see cref="AmbiguousResult"/> means the operation may have committed —
/// idempotent retry via the same request_id is the safe path.
/// </summary>
public class WhitewaterException : Exception
{
    public int Status { get; }
    public string? Code { get; }

    public WhitewaterException(int status, string? code, string message)
        : base(message)
    {
        Status = status;
        Code = code;
    }

    /// <summary>Non-retryable refusal (4xx without a specific code).</summary>
    public sealed class Refused : WhitewaterException
    {
        public Refused(int status, string? code, string message) : base(status, code, message) { }
    }

    /// <summary>409 refusal — fencing, epoch, or sequencing conflict.</summary>
    public class Conflict : WhitewaterException
    {
        public Conflict(int status, string? code, string message) : base(status, code, message) { }
    }

    /// <summary><c>stale_epoch</c> — the Writer/Reader session was superseded.</summary>
    public sealed class StaleSession : Conflict
    {
        public StaleSession(int status, string? code, string message) : base(status, code, message) { }
    }

    /// <summary><c>lease.*</c> — subscription work lease contested, stale, or full.</summary>
    public sealed class LeaseConflict : Conflict
    {
        public LeaseConflict(int status, string? code, string message) : base(status, code, message) { }
    }

    /// <summary>Transient: node unreachable, 503, or frozen-for-cutover.</summary>
    public sealed class Unavailable : WhitewaterException
    {
        public Unavailable(int status, string? code, string message) : base(status, code, message) { }
    }

    /// <summary>The operation may or may not have committed (5xx, timeout).</summary>
    public sealed class AmbiguousResult : WhitewaterException
    {
        public AmbiguousResult(int status, string? code, string message) : base(status, code, message) { }
    }

    private static readonly HashSet<string> LeaseCodes = new()
    {
        "lease.busy", "lease.stale_lease", "lease.capacity",
    };

    internal static WhitewaterException MapError(int status, JsonNode? body)
    {
        var message = body?["error"]?.GetValue<string>() ?? $"http {status}";
        var code = body?["code"]?.GetValue<string>();
        if (code == "stale_epoch")
        {
            return new StaleSession(status, code, message);
        }
        if (code != null && LeaseCodes.Contains(code))
        {
            return new LeaseConflict(status, code, message);
        }
        return status switch
        {
            409 => new Conflict(status, code, message),
            503 or 0 => new Unavailable(status, code, message),
            >= 500 => new AmbiguousResult(status, code, message),
            _ => new Refused(status, code, message),
        };
    }
}
