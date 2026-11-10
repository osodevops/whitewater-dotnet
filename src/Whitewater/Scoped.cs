using System.Text.Json.Nodes;

namespace Whitewater;

/// <summary>
/// A resource reference inside a <see cref="Domain"/>-scoped call —
/// mostly Pipe definitions, whose declared name lives in the defining
/// Domain while the Subscription and output Feed/StateStore may point
/// anywhere:
///
/// <code>
/// // everything inside one Domain — strings compose in-domain:
/// await pipes.DefineForwardPipeAsync("relay", "audit", "orders.copy");
///
/// // cross-Domain: a handle or an explicit absolute name carries the
/// // full dotted name — never silently re-prefixed:
/// await warehouse.DefineForwardPipeAsync(
///     "pull", purchasing.Subscription("fulfil"), warehouse.Feed("inbound"));
/// await warehouse.DefineForwardPipeAsync(
///     "pull2", Scoped.Absolute("purchasing.fulfil"), "inbound");
/// </code>
///
/// A plain <see cref="string"/> resolves inside the owning Domain;
/// <see cref="Feed{TKey,TValue}"/>, <see cref="Store{TKey,TValue}"/>,
/// <see cref="StateStore"/>, and <see cref="SubscriptionRef"/> handles
/// — plus <see cref="Absolute"/> — carry a fully-qualified name.
/// </summary>
public readonly struct Scoped
{
    internal Scoped(string name, bool absolute)
    {
        Name = name;
        IsAbsolute = absolute;
    }

    internal string Name { get; }

    internal bool IsAbsolute { get; }

    /// <summary>An explicit cross-Domain reference —
    /// <c>Scoped.Absolute("warehouse.inbound")</c> stays
    /// <c>warehouse.inbound</c> wherever it is used.</summary>
    public static Scoped Absolute(string name)
        => new(WhitewaterClient.DottedName(name), true);

    /// <summary>A name resolved inside the Domain the call is scoped
    /// to — <c>"orders.copy"</c> inside <c>pipes</c> means
    /// <c>pipes.orders.copy</c>.</summary>
    public static implicit operator Scoped(string relative)
        => new(relative ?? throw new ArgumentNullException(nameof(relative)), false);

    internal string Resolve(Domain domain)
        => IsAbsolute ? Name : domain.Resolve(Name);
}

/// <summary>
/// A handle on one declared Subscription — the durable group-progress
/// declaration Pipes drive from. Obtained via
/// <see cref="Domain.Subscription(string)"/>; the resolved absolute
/// <see cref="Name"/> travels to other Domains (a Pipe in another
/// Domain can drive from this Subscription), while
/// <see cref="Member"/> opens a member session on it.
/// </summary>
public sealed class SubscriptionRef
{
    private readonly WhitewaterClient _client;

    internal SubscriptionRef(WhitewaterClient client, string name)
    {
        _client = client;
        Name = name;
    }

    /// <summary>The declared Subscription's absolute dotted name.</summary>
    public string Name { get; }

    /// <summary>A member-session client on this Subscription — join,
    /// claim work, fetch, commit.</summary>
    public SubscriptionMemberClient Member() => _client.SubscriptionMember(Name);

    /// <summary><c>DESCRIBE SUBSCRIPTION</c>: the catalog row.</summary>
    public Task<ControlExecution> DescribeAsync(CancellationToken ct = default)
        => _client.DescribeSubscriptionAsync(Name, ct);

    /// <summary>The absolute name travels as a cross-Domain reference —
    /// usable as a Pipe's subscription source from any Domain.</summary>
    public static implicit operator Scoped(SubscriptionRef subscription)
        => Scoped.Absolute((subscription ?? throw new ArgumentNullException(nameof(subscription))).Name);
}
