using System.Text.Json.Nodes;
using Whitewater.Internal;

namespace Whitewater;

/// <summary>
/// Entry point: resource management (WCL, typed commands, health) plus the
/// Writer, Reader, Subscription-member, and Feed facades.
/// </summary>
public sealed class WhitewaterClient
{
    private readonly Transport _transport;

    public FeedClient Feeds { get; }

    public WhitewaterClient(string apiKey, IReadOnlyList<Uri> endpoints, TimeSpan? timeout = null)
    {
        _transport = new Transport(endpoints, apiKey, timeout ?? TimeSpan.FromSeconds(60));
        Feeds = new FeedClient(_transport);
        StateStores = new StateStoresClient(_transport);
    }

    /// <summary>Shared transport for low-level calls not yet covered by
    /// a typed facade.</summary>
    internal Transport Transport => _transport;

    /// <summary>Execute a WCL script with a fresh request id; identical
    /// <paramref name="requestId"/> retries return the original committed
    /// execution.</summary>
    public Task<ControlExecution> WclAsync(string script, CancellationToken ct = default)
        => WclAsync(script, Guid.NewGuid(), ct);

    /// <summary>Execute a WCL script under an explicit request identity.</summary>
    public async Task<ControlExecution> WclAsync(
        string script, Guid requestId, CancellationToken ct = default)
    {
        var body = new JsonObject
        {
            ["request_id"] = requestId.ToString(),
            ["script"] = script,
        };
        return Wire.ControlExecution(await _transport.PostAsync("/v1/admin/wcl", body, ct)
            .ConfigureAwait(false));
    }

    /// <summary>
    /// Execute typed control commands, e.g.
    /// <c>[{"command": "show", "kind": "feeds"}]</c>.
    /// </summary>
    public async Task<ControlExecution> CommandsAsync(
        IReadOnlyList<JsonObject> commands, Guid? requestId = null, CancellationToken ct = default)
    {
        var array = new JsonArray();
        foreach (var command in commands)
        {
            array.Add(command.DeepClone());
        }
        var body = new JsonObject
        {
            ["request_id"] = (requestId ?? Guid.NewGuid()).ToString(),
            ["commands"] = array,
        };
        return Wire.ControlExecution(await _transport
            .PostAsync("/v1/admin/commands", body, ct)
            .ConfigureAwait(false));
    }

    /// <summary>Liveness of the contacted Node.</summary>
    public async Task<bool> HealthyAsync(CancellationToken ct = default)
    {
        var health = await _transport.GetAsync("/health", ct).ConfigureAwait(false);
        return health?["status"]?.GetValue<string>() == "ok";
    }

    // ---- Pipe lifecycle ------------------------------------------------------

    /// <summary>Bind a declared Pipe for lifecycle operations:
    /// Describe/Pause/Resume/Drop.</summary>
    public Pipe Pipe(string name) => new(this, name);

    /// <summary><c>SHOW PIPES</c>: the catalog of declared Pipes.</summary>
    public async Task<IReadOnlyList<PipeInfo>> PipesAsync(CancellationToken ct = default)
    {
        var execution = await CommandsAsync(
            new JsonObject[] { new() { ["command"] = "show", ["kind"] = "pipes" } },
            ct: ct).ConfigureAwait(false);
        var data = execution.Results.Count > 0 ? execution.Results[0].Data : null;
        return Operations.DecodePipes(data);
    }

    // ---- Index queries ----------------------------------------------------------

    /// <summary>Query a replicated Index by predicate — exact, prefix, or
    /// range over typed values. <paramref name="strict"/> waits for the
    /// committed target rather than answering stale.</summary>
    public async Task<IndexPage> QueryIndexAsync(
        string index,
        IndexPredicate predicate,
        int? limit = null,
        bool strict = false,
        CancellationToken ct = default)
    {
        var body = predicate.WireFields();
        body["index"] = Dotted(index);
        body["strict"] = strict;
        if (limit is { } bounded)
        {
            body["limit"] = bounded;
        }
        return Operations.DecodeIndexPage(
            await _transport.PostAsync("/v1/indexes/query", body, ct)
                .ConfigureAwait(false));
    }

    // ---- Demand observability ------------------------------------------------------

    private async Task<DemandReport> DemandAsync(
        string surface, string resourceKey, string nameField, CancellationToken ct)
        => Operations.DecodeDemandReport(
            await _transport.GetAsync($"/v1/admin/demand/{surface}", ct)
                .ConfigureAwait(false),
            resourceKey, nameField);

    /// <summary><c>GET /v1/admin/demand/feeds</c> — per-Feed append/read
    /// attribution plus per-domain rollups.</summary>
    public Task<DemandReport> DemandFeedsAsync(CancellationToken ct = default)
        => DemandAsync("feeds", "feeds", "feed", ct);

    /// <summary><c>GET /v1/admin/demand/readers</c> — per-Reader
    /// request/fetch/ack counters.</summary>
    public Task<DemandReport> DemandReadersAsync(CancellationToken ct = default)
        => DemandAsync("readers", "readers", "reader", ct);

    /// <summary><c>GET /v1/admin/demand/subscriptions</c> —
    /// per-Subscription counters.</summary>
    public Task<DemandReport> DemandSubscriptionsAsync(CancellationToken ct = default)
        => DemandAsync("subscriptions", "subscriptions", "subscription", ct);

    /// <summary><c>GET /v1/admin/demand/indexes</c> — per-Index query
    /// counters.</summary>
    public Task<DemandReport> DemandIndexesAsync(CancellationToken ct = default)
        => DemandAsync("indexes", "indexes", "index", ct);

    /// <summary><c>GET /v1/admin/demand/pipes</c> — per-Pipe
    /// drive/apply/failure counters.</summary>
    public Task<DemandReport> DemandPipesAsync(CancellationToken ct = default)
        => DemandAsync("pipes", "pipes", "pipe", ct);

    /// <summary><c>GET /v1/admin/demand/hot-keys</c> — dominant-Key
    /// evidence across Feeds. A strictly ordered hot Key is never
    /// parallelized: the honest signal is to spread or shed load.</summary>
    public async Task<IReadOnlyList<HotKey>> HotKeysAsync(CancellationToken ct = default)
        => Operations.DecodeHotKeys(
            await _transport.GetAsync("/v1/admin/demand/hot-keys", ct)
                .ConfigureAwait(false));

    // ---- Resource management -------------------------------------------------
    // Typed convenience over <see cref="WclAsync(string, CancellationToken)"/>:
    // each method builds one WCL statement and executes it. Names are
    // validated before interpolation, so a bad value fails locally rather
    // than reaching the Control Plane.

    /// <summary><c>CREATE DOMAIN &lt;name&gt;</c></summary>
    public Task<ControlExecution> CreateDomainAsync(string name, CancellationToken ct = default)
        => WclAsync("CREATE DOMAIN " + Dotted(name), ct);

    /// <summary><c>CREATE FEED &lt;dotted.name&gt;</c></summary>
    public Task<ControlExecution> CreateFeedAsync(string name, CancellationToken ct = default)
        => WclAsync("CREATE FEED " + Dotted(name), ct);

    /// <summary><c>CREATE WRITER &lt;name&gt; TO &lt;feed&gt;</c></summary>
    public Task<ControlExecution> CreateWriterAsync(
        string name, string feed, CancellationToken ct = default)
        => WclAsync($"CREATE WRITER {Resource(name)} TO {Dotted(feed)}", ct);

    /// <summary><c>CREATE READER &lt;name&gt; FROM &lt;feed&gt; [START AT &lt;start&gt;]</c>
    /// (default BEGINNING).</summary>
    public Task<ControlExecution> CreateReaderAsync(
        string name, string feed, ReaderStart? start = null, CancellationToken ct = default)
        => CreateFromAsync("READER", name, feed, start ?? ReaderStart.Beginning(), ct);

    /// <summary><c>CREATE SUBSCRIPTION &lt;name&gt; FROM &lt;feed&gt;
    /// [START AT &lt;start&gt;]</c>.</summary>
    public Task<ControlExecution> CreateSubscriptionAsync(
        string name, string feed, ReaderStart? start = null, CancellationToken ct = default)
        => CreateFromAsync("SUBSCRIPTION", name, feed, start ?? ReaderStart.Beginning(), ct);

    private Task<ControlExecution> CreateFromAsync(
        string kind, string name, string feed, ReaderStart start, CancellationToken ct)
        => WclAsync($"CREATE {kind} {Resource(name)} FROM {Dotted(feed)} START AT {start.Wcl}", ct);

    /// <summary><c>CREATE ROLE &lt;name&gt;</c></summary>
    public Task<ControlExecution> CreateRoleAsync(string name, CancellationToken ct = default)
        => WclAsync("CREATE ROLE " + Resource(name), ct);

    /// <summary><c>OPEN WRITER SESSION &lt;name&gt;</c>; the returned epoch feeds
    /// <see cref="Writer"/>.</summary>
    public Task<ControlExecution> OpenWriterSessionAsync(
        string writer, CancellationToken ct = default)
        => WclAsync("OPEN WRITER SESSION " + Resource(writer), ct);

    /// <summary><c>SEEK READER &lt;name&gt; TO &lt;start&gt;</c> — repositions a
    /// durable Reader.</summary>
    public Task<ControlExecution> SeekReaderAsync(
        string reader, ReaderStart start, CancellationToken ct = default)
        => WclAsync($"SEEK READER {Resource(reader)} TO {start.Wcl}", ct);

    /// <summary><c>DESCRIBE DOMAIN &lt;name&gt;</c></summary>
    public Task<ControlExecution> DescribeDomainAsync(string name, CancellationToken ct = default)
        => DescribeAsync("DOMAIN", name, true, ct);

    /// <summary><c>DESCRIBE FEED &lt;dotted.name&gt;</c></summary>
    public Task<ControlExecution> DescribeFeedAsync(string name, CancellationToken ct = default)
        => DescribeAsync("FEED", name, true, ct);

    /// <summary><c>DESCRIBE WRITER &lt;name&gt;</c></summary>
    public Task<ControlExecution> DescribeWriterAsync(string name, CancellationToken ct = default)
        => DescribeAsync("WRITER", name, false, ct);

    /// <summary><c>DESCRIBE READER &lt;name&gt;</c></summary>
    public Task<ControlExecution> DescribeReaderAsync(string name, CancellationToken ct = default)
        => DescribeAsync("READER", name, false, ct);

    /// <summary><c>DESCRIBE SUBSCRIPTION &lt;name&gt;</c></summary>
    public Task<ControlExecution> DescribeSubscriptionAsync(string name, CancellationToken ct = default)
        => DescribeAsync("SUBSCRIPTION", name, false, ct);

    /// <summary><c>DESCRIBE ROLE &lt;name&gt;</c></summary>
    public Task<ControlExecution> DescribeRoleAsync(string name, CancellationToken ct = default)
        => DescribeAsync("ROLE", name, false, ct);

    /// <summary><c>RENAME DOMAIN &lt;current&gt; TO &lt;renamed&gt;</c></summary>
    public Task<ControlExecution> RenameDomainAsync(
        string current, string renamed, CancellationToken ct = default)
        => RenameAsync("DOMAIN", current, renamed, true, ct);

    /// <summary><c>RENAME FEED &lt;current&gt; TO &lt;renamed&gt;</c></summary>
    public Task<ControlExecution> RenameFeedAsync(
        string current, string renamed, CancellationToken ct = default)
        => RenameAsync("FEED", current, renamed, true, ct);

    /// <summary><c>RENAME WRITER &lt;current&gt; TO &lt;renamed&gt;</c></summary>
    public Task<ControlExecution> RenameWriterAsync(
        string current, string renamed, CancellationToken ct = default)
        => RenameAsync("WRITER", current, renamed, false, ct);

    /// <summary><c>RENAME READER &lt;current&gt; TO &lt;renamed&gt;</c></summary>
    public Task<ControlExecution> RenameReaderAsync(
        string current, string renamed, CancellationToken ct = default)
        => RenameAsync("READER", current, renamed, false, ct);

    /// <summary><c>RENAME SUBSCRIPTION &lt;current&gt; TO &lt;renamed&gt;</c></summary>
    public Task<ControlExecution> RenameSubscriptionAsync(
        string current, string renamed, CancellationToken ct = default)
        => RenameAsync("SUBSCRIPTION", current, renamed, false, ct);

    /// <summary><c>RENAME ROLE &lt;current&gt; TO &lt;renamed&gt;</c></summary>
    public Task<ControlExecution> RenameRoleAsync(
        string current, string renamed, CancellationToken ct = default)
        => RenameAsync("ROLE", current, renamed, false, ct);

    /// <summary><c>DROP DOMAIN &lt;name&gt;</c></summary>
    public Task<ControlExecution> DropDomainAsync(string name, CancellationToken ct = default)
        => DropAsync("DOMAIN", name, true, ct);

    /// <summary><c>DROP FEED &lt;dotted.name&gt;</c></summary>
    public Task<ControlExecution> DropFeedAsync(string name, CancellationToken ct = default)
        => DropAsync("FEED", name, true, ct);

    /// <summary><c>DROP WRITER &lt;name&gt;</c></summary>
    public Task<ControlExecution> DropWriterAsync(string name, CancellationToken ct = default)
        => DropAsync("WRITER", name, false, ct);

    /// <summary><c>DROP READER &lt;name&gt;</c></summary>
    public Task<ControlExecution> DropReaderAsync(string name, CancellationToken ct = default)
        => DropAsync("READER", name, false, ct);

    /// <summary><c>DROP SUBSCRIPTION &lt;name&gt;</c></summary>
    public Task<ControlExecution> DropSubscriptionAsync(string name, CancellationToken ct = default)
        => DropAsync("SUBSCRIPTION", name, false, ct);

    /// <summary><c>DROP ROLE &lt;name&gt;</c></summary>
    public Task<ControlExecution> DropRoleAsync(string name, CancellationToken ct = default)
        => DropAsync("ROLE", name, false, ct);

    /// <summary><c>SHOW DOMAINS</c> — rows live in <c>Results[0].Data</c>.</summary>
    public Task<ControlExecution> ListDomainsAsync(CancellationToken ct = default)
        => WclAsync("SHOW DOMAINS", ct);

    /// <summary><c>SHOW FEEDS</c></summary>
    public Task<ControlExecution> ListFeedsAsync(CancellationToken ct = default)
        => WclAsync("SHOW FEEDS", ct);

    /// <summary><c>SHOW WRITERS</c></summary>
    public Task<ControlExecution> ListWritersAsync(CancellationToken ct = default)
        => WclAsync("SHOW WRITERS", ct);

    /// <summary><c>SHOW READERS</c></summary>
    public Task<ControlExecution> ListReadersAsync(CancellationToken ct = default)
        => WclAsync("SHOW READERS", ct);

    /// <summary><c>SHOW SUBSCRIPTIONS</c></summary>
    public Task<ControlExecution> ListSubscriptionsAsync(CancellationToken ct = default)
        => WclAsync("SHOW SUBSCRIPTIONS", ct);

    /// <summary><c>SHOW ROLES</c></summary>
    public Task<ControlExecution> ListRolesAsync(CancellationToken ct = default)
        => WclAsync("SHOW ROLES", ct);

    /// <summary><c>SHOW GRANTS</c></summary>
    public Task<ControlExecution> ListGrantsAsync(CancellationToken ct = default)
        => WclAsync("SHOW GRANTS", ct);

    private Task<ControlExecution> DescribeAsync(
        string kind, string name, bool dottedName, CancellationToken ct)
        => WclAsync($"DESCRIBE {kind} {NameFor(name, dottedName)}", ct);

    private Task<ControlExecution> RenameAsync(
        string kind, string current, string renamed, bool dottedName, CancellationToken ct)
        => WclAsync($"RENAME {kind} {NameFor(current, dottedName)} TO {NameFor(renamed, dottedName)}", ct);

    private Task<ControlExecution> DropAsync(
        string kind, string name, bool dottedName, CancellationToken ct)
        => WclAsync($"DROP {kind} {NameFor(name, dottedName)}", ct);

    private static string NameFor(string name, bool dottedName)
        => dottedName ? Dotted(name) : Resource(name);

    /// <summary>Lowercase dotted names:
    /// <c>^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$</c>, ≤512 bytes.</summary>
    internal static string DottedName(string name) => Dotted(name);

    private static string Dotted(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        if (name.Length > 512 || name.StartsWith('.') || name.EndsWith('.')
            || !System.Text.RegularExpressions.Regex.IsMatch(
                name, @"^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$"))
        {
            throw new ArgumentException($"invalid dotted name: {name}", nameof(name));
        }
        return name;
    }

    /// <summary>Single-segment resource names (writers, readers,
    /// subscriptions, roles).</summary>
    private static string Resource(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        if (!System.Text.RegularExpressions.Regex.IsMatch(name, @"^[A-Za-z][A-Za-z0-9_-]{0,127}$"))
        {
            throw new ArgumentException($"invalid resource name: {name}", nameof(name));
        }
        return name;
    }

    public WriterSessionClient Writer(string name, long sessionEpoch)
        => new(_transport, name, sessionEpoch);

    public ReaderSessionClient Reader(string name)
        => new(_transport, name);

    public SubscriptionMemberClient SubscriptionMember(string subscription)
        => new(_transport, subscription);

    /// <summary>
    /// Subscribe durably to a Feed: ensures a Reader named
    /// <paramref name="reader"/> exists (creating it at
    /// <paramref name="start"/> when missing), opens a session, and invokes
    /// <paramref name="onRecords"/> per fetched page on a background task,
    /// acknowledging after each batch.
    ///
    /// The returned <see cref="Subscription"/> runs until
    /// <see cref="Subscription.DisposeAsync"/>; terminal errors reach
    /// <paramref name="onError"/> and <see cref="Subscription.Failure"/>.
    /// </summary>
    public Subscription Subscribe(
        string reader,
        string feed,
        ReaderStart start,
        Func<IReadOnlyList<FeedRecord>, Task> onRecords,
        Action<WhitewaterException>? onError = null)
        => new(this, reader, feed, start, SubscribeCapacity, onRecords, onError);

    /// <summary>
    /// A codec-bound Feed facade — serialization is bound once so every
    /// write encodes and every read decodes automatically:
    /// <code>
    /// var orders = client.Feed("orders.events", Codecs.Json&lt;Order&gt;());
    /// var writer = await orders.WriterAsync("ingest");   // declares + opens
    /// await writer.AppendAsync("order-1", order);        // JSON-encoded
    /// </code>
    /// <paramref name="valueCodec"/> keeps UTF-8 keys (the common case);
    /// the two-codec overload binds both explicitly — protobuf/msgpack/
    /// custom via <see cref="Codecs.Custom{T}"/>.
    /// </summary>
    public Feed<string, TValue> Feed<TValue>(string name, ICodec<TValue> valueCodec)
        => new(this, DottedName(name), Codecs.Utf8, valueCodec);

    /// <inheritdoc cref="Feed{TValue}(string, ICodec{TValue})"/>
    public Feed<TKey, TValue> Feed<TKey, TValue>(
        string name, ICodec<TKey> keyCodec, ICodec<TValue> valueCodec)
        => new(this, DottedName(name), keyCodec, valueCodec);

    /// <summary>The default binding — UTF-8 string keys and untouched
    /// payload bytes, the escape hatch to the wire.</summary>
    public Feed<string, byte[]> Feed(string name)
        => new(this, DottedName(name), Codecs.Utf8, Codecs.Bytes);

    /// <summary>
    /// A codec-bound StateStore facade — every `Get`/`Put`/`Iterate`
    /// serializes through the bound codecs. The byte-level
    /// <see cref="StateStore(string)"/> facade remains for raw access
    /// and declaration.
    /// </summary>
    public Store<string, TValue> Store<TValue>(string name, ICodec<TValue> valueCodec)
        => Whitewater.Store<string, TValue>.Bind(
            Whitewater.StateStore.Bound(_transport, name), Codecs.Utf8, valueCodec);

    /// <inheritdoc cref="Store{TValue}(string, ICodec{TValue})"/>
    public Store<TKey, TValue> Store<TKey, TValue>(
        string name, ICodec<TKey> keyCodec, ICodec<TValue> valueCodec)
        => Whitewater.Store<TKey, TValue>.Bind(
            Whitewater.StateStore.Bound(_transport, name), keyCodec, valueCodec);

    /// <summary>The default binding — UTF-8 keys and untouched payload
    /// bytes.</summary>
    public Store<string, byte[]> Store(string name)
        => Whitewater.Store<string, byte[]>.Bind(
            Whitewater.StateStore.Bound(_transport, name), Codecs.Utf8, Codecs.Bytes);

    /// <summary>
    /// A thin facade over the node-side StateStore <paramref name="name"/> —
    /// rows live on the Nodes (RF3 + Fjall); this client holds no state.
    /// </summary>
    public StateStore StateStore(string name) => Whitewater.StateStore.Bound(_transport, name);

    /// <summary>Collection-level StateStore operations: declare, drop.</summary>
    public StateStoresClient StateStores { get; }

    private const int SubscribeCapacity = 128;
}
