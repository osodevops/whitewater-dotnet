using Whitewater.Internal;

namespace Whitewater;

/// <summary>Entry point: one transport, four capability facades.</summary>
public sealed class WhitewaterClient
{
    private readonly Transport _transport;

    public AdminClient Admin { get; }

    public WhitewaterClient(string apiKey, IReadOnlyList<Uri> endpoints, TimeSpan? timeout = null)
    {
        _transport = new Transport(endpoints, apiKey, timeout ?? TimeSpan.FromSeconds(60));
        Admin = new AdminClient(_transport);
    }

    /// <summary>Shared transport for low-level calls not yet covered by
    /// a typed facade.</summary>
    internal Transport Transport => _transport;

    public WriterSessionClient Writer(string name, long sessionEpoch)
        => new(_transport, name, sessionEpoch);

    public ReaderSessionClient Reader(string name)
        => new(_transport, name);

    public SubscriptionMemberClient SubscriptionMember(string subscription)
        => new(_transport, subscription);
}
