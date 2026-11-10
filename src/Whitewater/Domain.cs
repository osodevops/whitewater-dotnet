using System.Text.Json.Nodes;
using Whitewater.Internal;

namespace Whitewater;

/// <summary>
/// A strongly-typed handle on one Domain — a Domain owns every Feed it
/// prefixes (<c>shop</c> owns <c>shop.orders</c>), so the friendliest
/// surface navigates domain → feed → reader/writer instead of
/// composing dotted names by hand:
/// <code>
/// var shop = await client.GetDomainAsync("shop");   // verifies + loads now
/// var lazy = client.Domain("shop");                  // no I/O until needed
///
/// await shop.FeedNamesAsync();                       // ['shop.orders', ...]
/// await shop.FeedsAsync("order");                    // prefix-filtered
/// await shop.ReadersAsync(); await shop.WritersAsync();
///
/// await using var reader = await shop
///     .CreateReader("orders")
///     .ReaderId("fulfillment")                       // resolves shop.fulfillment
///     .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning)
///     .MaxPageSize(256)
///     .OpenAsync();
/// </code>
/// The first catalog accessor issues ONE batched control read
/// (<c>describe domain</c> + every <c>show</c> listing), joins rows by
/// <c>space_id</c>/<c>feed_id</c>, and caches the snapshot —
/// <see cref="RefreshAsync"/> re-pulls. <see cref="Feed(string)"/> name
/// composition is free: zero I/O.
/// </summary>
public sealed class Domain
{
    private readonly WhitewaterClient _client;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private Snapshot? _snapshot;

    internal Domain(WhitewaterClient client, string name)
    {
        _client = client;
        Name = WhitewaterClient.DottedName(name);
    }

    /// <summary>The Domain's dotted name.</summary>
    public string Name { get; }

    /// <summary>The Domain's immutable id — survives renames.</summary>
    public async Task<string> DomainIdAsync(CancellationToken ct = default)
        => (await SnapAsync(ct).ConfigureAwait(false)).DomainId;

    /// <summary>The catalog revision the snapshot was taken at.</summary>
    public async Task<long> RevisionAsync(CancellationToken ct = default)
        => (await SnapAsync(ct).ConfigureAwait(false)).Revision;

    /// <summary>The raw <c>DESCRIBE DOMAIN</c> row — status,
    /// created_at_ns, history_policy.</summary>
    public async Task<JsonObject> DescribeAsync(CancellationToken ct = default)
        => (await SnapAsync(ct).ConfigureAwait(false)).Describe;

    /// <summary>The declared History Policy row, or null when unset.</summary>
    public async Task<JsonObject?> HistoryPolicyAsync(CancellationToken ct = default)
        => (await SnapAsync(ct).ConfigureAwait(false)).Describe["history_policy"] as JsonObject;

    /// <summary>The Domain's declared schema default — null when
    /// undeclared. Every Feed without its own declaration inherits it.</summary>
    public async Task<FeedSchema?> SchemaAsync(CancellationToken ct = default)
        => FeedSchema.From(
            (await SnapAsync(ct).ConfigureAwait(false)).Describe["schema"]);

    /// <summary>The effective schema for a Feed — its declared sides
    /// layered over the Domain default. Null when nothing is declared
    /// anywhere (schema-neutral bytes).</summary>
    public async Task<FeedSchema?> FeedSchemaAsync(string feed, CancellationToken ct = default)
    {
        var snap = await SnapAsync(ct).ConfigureAwait(false);
        return snap.FeedSchemas.TryGetValue(Resolve(feed), out var schema) ? schema : null;
    }

    /// <summary>Only the sides the Feed itself declares — null when it
    /// inherits the Domain default wholesale.</summary>
    public async Task<FeedSchema?> DeclaredFeedSchemaAsync(string feed, CancellationToken ct = default)
    {
        var snap = await SnapAsync(ct).ConfigureAwait(false);
        return snap.DeclaredSchemas.TryGetValue(Resolve(feed), out var schema) ? schema : null;
    }

    // ---- scoped resources -------------------------------------------------

    /// <summary>Every Feed in this Domain as typed <see cref="Feed{TKey,TValue}"/>
    /// handles, sorted by name. <paramref name="prefix"/> filters on the
    /// relative name (<c>shop.orders</c> → <c>orders</c>).</summary>
    public async Task<IReadOnlyList<Feed<string, string>>> FeedsAsync(
        string? prefix = null, CancellationToken ct = default)
    {
        var snap = await SnapAsync(ct).ConfigureAwait(false);
        return snap.Feeds
            .Where(full => prefix == null || Relative(full).StartsWith(prefix))
            .Select(full => Feed(Relative(full)))
            .ToList();
    }

    /// <summary>The Feed names in this Domain, sorted.</summary>
    public async Task<IReadOnlyList<string>> FeedNamesAsync(CancellationToken ct = default)
        => (await SnapAsync(ct).ConfigureAwait(false)).Feeds;

    /// <summary>Declared Readers on this Domain's Feeds, sorted.</summary>
    public async Task<IReadOnlyList<string>> ReadersAsync(CancellationToken ct = default)
        => (await SnapAsync(ct).ConfigureAwait(false)).Readers;

    /// <summary>Declared Writers on this Domain's Feeds, sorted.</summary>
    public async Task<IReadOnlyList<string>> WritersAsync(CancellationToken ct = default)
        => (await SnapAsync(ct).ConfigureAwait(false)).Writers;

    /// <summary>Declared Subscriptions in this Domain, sorted.</summary>
    public async Task<IReadOnlyList<string>> SubscriptionsAsync(CancellationToken ct = default)
        => (await SnapAsync(ct).ConfigureAwait(false)).Subscriptions;

    /// <summary>Declared Pipes in this Domain, sorted.</summary>
    public async Task<IReadOnlyList<string>> PipeNamesAsync(CancellationToken ct = default)
        => (await SnapAsync(ct).ConfigureAwait(false)).Pipes;

    /// <summary>Declared Indexes in this Domain, sorted.</summary>
    public async Task<IReadOnlyList<string>> IndexesAsync(CancellationToken ct = default)
        => (await SnapAsync(ct).ConfigureAwait(false)).Indexes;

    /// <summary>Declared StateStores in this Domain, sorted.</summary>
    public async Task<IReadOnlyList<string>> StateStoresAsync(CancellationToken ct = default)
        => (await SnapAsync(ct).ConfigureAwait(false)).StateStores;

    /// <summary>Re-pull the catalog snapshot — the next accessor sees
    /// fresh rows.</summary>
    public async Task<Domain> RefreshAsync(CancellationToken ct = default)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            _snapshot = await LoadAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
        return this;
    }

    // ---- Feed handles ------------------------------------------------------

    /// <summary>
    /// A typed <see cref="Feed{TKey,TValue}"/> handle on
    /// <paramref name="relative"/> inside this Domain:
    /// <c>shop.Feed("orders")</c> binds <c>shop.orders</c>. An
    /// already-qualified name (<c>"shop.orders"</c>) is accepted as-is.
    /// Pure name composition — no I/O.
    /// </summary>
    public Feed<string, string> Feed(string relative)
        => new(_client, Resolve(relative), Codecs.Utf8, Codecs.Utf8, domain: this);

    /// <summary><see cref="Feed(string)"/> with UTF-8 keys and payloads
    /// of <typeparamref name="TValue"/>.</summary>
    public Feed<string, TValue> Feed<TValue>(string relative, ICodec<TValue> valueCodec)
        => new(_client, Resolve(relative), Codecs.Utf8, valueCodec, domain: this);

    /// <summary><see cref="Feed(string)"/> with a typed key codec too.</summary>
    public Feed<TKey, TValue> Feed<TKey, TValue>(
        string relative, ICodec<TKey> keyCodec, ICodec<TValue> valueCodec)
        => new(_client, Resolve(relative), keyCodec, valueCodec, domain: this);

    /// <summary>Alias of <see cref="Feed(string)"/>.</summary>
    public Feed<string, string> GetFeed(string relative) => Feed(relative);

    /// <summary>
    /// <c>CREATE FEED</c> inside this Domain —
    /// <c>shop.CreateFeedAsync("orders")</c> declares
    /// <c>shop.orders</c>. <see cref="RefreshAsync"/> (or a lazy
    /// accessor's first load) surfaces it through
    /// <see cref="FeedsAsync"/>/<see cref="FeedNamesAsync"/>.
    /// </summary>
    public Task<ControlExecution> CreateFeedAsync(
        string relative, CancellationToken ct = default)
        => _client.CreateFeedAsync(Resolve(relative), ct);

    /// <summary>
    /// Fluent Reader declaration on <paramref name="relative"/> inside
    /// this Domain — <c>shop.CreateReader("orders")</c> binds
    /// <c>shop.orders</c>:
    /// <code>
    /// var reader = await shop.CreateReader("orders")
    ///     .ReaderId("fulfillment")              // the Kafka groupId analogue
    ///     .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning)
    ///     .OpenAsync();
    /// </code>
    /// </summary>
    public ReaderBuilder<string, string> CreateReader(string relative)
        => _client.CreateReader(this, relative);

    /// <summary><see cref="CreateReader(string)"/> on an explicit
    /// typed handle.</summary>
    public ReaderBuilder<TKey, TValue> CreateReader<TKey, TValue>(
        Feed<TKey, TValue> feed)
        => _client.CreateReader(this, feed);

    /// <summary>
    /// Fluent Writer declaration on <paramref name="relative"/> inside
    /// this Domain — <c>shop.CreateWriter("orders")</c> binds
    /// <c>shop.orders</c>.
    /// </summary>
    public WriterBuilder<string, string> CreateWriter(string relative)
        => _client.CreateWriter(this, relative);

    /// <summary><see cref="CreateWriter(string)"/> on an explicit
    /// typed handle.</summary>
    public WriterBuilder<TKey, TValue> CreateWriter<TKey, TValue>(
        Feed<TKey, TValue> feed)
        => _client.CreateWriter(this, feed);

    /// <summary>
    /// An absolute cross-Domain reference to a resource inside this
    /// Domain — pass it to another Domain's Pipe definition and it
    /// still points here: <c>warehouse.DefineForwardPipeAsync("pull",
    /// purchasing.Scoped("fulfil"), "inbound")</c>. Handles
    /// (<see cref="Feed{TKey,TValue}"/>, <see cref="Store{TKey,TValue}"/>,
    /// <see cref="StateStore"/>, <see cref="SubscriptionRef"/>) carry
    /// the same weight.
    /// </summary>
    public Scoped Scoped(string relative) => Whitewater.Scoped.Absolute(Resolve(relative));

    /// <summary><c>CREATE WRITER &lt;name&gt; TO &lt;feed&gt;</c> — both
    /// names resolve inside this Domain.</summary>
    public Task<ControlExecution> CreateWriterAsync(
        string writer, string feed, CancellationToken ct = default)
        => _client.CreateWriterAsync(Resolve(writer), Resolve(feed), ct);

    /// <summary><c>CREATE READER &lt;name&gt; FROM &lt;feed&gt;</c> — both
    /// names resolve inside this Domain; <paramref name="start"/>
    /// applies on first declare.</summary>
    public Task<ControlExecution> CreateReaderAsync(
        string reader, string feed,
        ReaderStart? start = null, CancellationToken ct = default)
        => _client.CreateReaderAsync(Resolve(reader), Resolve(feed), start, ct);

    // ---- Subscriptions -------------------------------------------------

    /// <summary><c>CREATE SUBSCRIPTION &lt;name&gt; FROM &lt;feed&gt;</c>
    /// — both names resolve inside this Domain:
    /// <c>pipes.CreateSubscriptionAsync("audit", "orders")</c> declares
    /// <c>pipes.audit</c> on <c>pipes.orders</c>.</summary>
    public Task<ControlExecution> CreateSubscriptionAsync(
        string name, string feed, ReaderStart? start = null, CancellationToken ct = default)
        => _client.CreateSubscriptionAsync(Resolve(name), Resolve(feed), start, ct);

    /// <summary>A <see cref="SubscriptionRef"/> handle on the declared
    /// Subscription <paramref name="relative"/> inside this Domain —
    /// open member sessions with <see cref="SubscriptionRef.Member"/> or
    /// pass it to another Domain's Pipe definition to drive from this
    /// Subscription.</summary>
    public SubscriptionRef Subscription(string relative)
        => new(_client, Resolve(relative));

    /// <summary>A member-session client on the Subscription
    /// <paramref name="relative"/> inside this Domain — join, claim
    /// work, fetch, commit.</summary>
    public SubscriptionMemberClient Member(string relative)
        => _client.SubscriptionMember(Resolve(relative));

    /// <summary>A running feed subscription: ensures the Reader
    /// <paramref name="reader"/> (resolved in-domain) exists on the Feed
    /// <paramref name="feed"/> (also resolved), then pumps fetched
    /// pages to <paramref name="onRecords"/>, acknowledging durable
    /// progress per batch.</summary>
    public Subscription Subscribe(
        string reader,
        string feed,
        ReaderStart start,
        Func<IReadOnlyList<FeedRecord>, Task> onRecords,
        Action<WhitewaterException>? onError = null,
        int maxPageSize = 128)
        => new(_client, Resolve(reader), Resolve(feed), start,
            maxPageSize, onRecords, onError);

    // ---- Reader / Writer sessions and lifecycle -------------------------

    /// <summary>A session client on the Reader <paramref name="relative"/>
    /// inside this Domain — open/fetch/ack/close.</summary>
    public ReaderSessionClient Reader(string relative)
        => _client.Reader(Resolve(relative));

    /// <summary>A session client on the Writer <paramref name="relative"/>
    /// inside this Domain at the fencing <paramref name="sessionEpoch"/>.</summary>
    public WriterSessionClient Writer(string relative, long sessionEpoch)
        => _client.Writer(Resolve(relative), sessionEpoch);

    /// <summary><c>OPEN WRITER SESSION &lt;name&gt;</c> — the returned
    /// epoch fences the session.</summary>
    public Task<ControlExecution> OpenWriterSessionAsync(
        string writer, CancellationToken ct = default)
        => _client.OpenWriterSessionAsync(Resolve(writer), ct);

    /// <summary><c>SEEK READER &lt;name&gt; TO &lt;start&gt;</c> —
    /// repositions a durable Reader.</summary>
    public Task<ControlExecution> SeekReaderAsync(
        string reader, ReaderStart start, CancellationToken ct = default)
        => _client.SeekReaderAsync(Resolve(reader), start, ct);

    /// <summary><c>DESCRIBE FEED &lt;name&gt;</c></summary>
    public Task<ControlExecution> DescribeFeedAsync(
        string relative, CancellationToken ct = default)
        => _client.DescribeFeedAsync(Resolve(relative), ct);

    /// <summary><c>DESCRIBE WRITER &lt;name&gt;</c></summary>
    public Task<ControlExecution> DescribeWriterAsync(
        string relative, CancellationToken ct = default)
        => _client.DescribeWriterAsync(Resolve(relative), ct);

    /// <summary><c>DESCRIBE READER &lt;name&gt;</c></summary>
    public Task<ControlExecution> DescribeReaderAsync(
        string relative, CancellationToken ct = default)
        => _client.DescribeReaderAsync(Resolve(relative), ct);

    /// <summary><c>DESCRIBE SUBSCRIPTION &lt;name&gt;</c></summary>
    public Task<ControlExecution> DescribeSubscriptionAsync(
        string relative, CancellationToken ct = default)
        => _client.DescribeSubscriptionAsync(Resolve(relative), ct);

    /// <summary><c>RENAME FEED &lt;current&gt; TO &lt;renamed&gt;</c> —
    /// both names resolve inside this Domain.</summary>
    public Task<ControlExecution> RenameFeedAsync(
        string current, string renamed, CancellationToken ct = default)
        => _client.RenameFeedAsync(Resolve(current), Resolve(renamed), ct);

    /// <summary><c>RENAME WRITER &lt;current&gt; TO &lt;renamed&gt;</c></summary>
    public Task<ControlExecution> RenameWriterAsync(
        string current, string renamed, CancellationToken ct = default)
        => _client.RenameWriterAsync(Resolve(current), Resolve(renamed), ct);

    /// <summary><c>RENAME READER &lt;current&gt; TO &lt;renamed&gt;</c></summary>
    public Task<ControlExecution> RenameReaderAsync(
        string current, string renamed, CancellationToken ct = default)
        => _client.RenameReaderAsync(Resolve(current), Resolve(renamed), ct);

    /// <summary><c>RENAME SUBSCRIPTION &lt;current&gt; TO &lt;renamed&gt;</c></summary>
    public Task<ControlExecution> RenameSubscriptionAsync(
        string current, string renamed, CancellationToken ct = default)
        => _client.RenameSubscriptionAsync(Resolve(current), Resolve(renamed), ct);

    /// <summary><c>DROP FEED &lt;name&gt;</c></summary>
    public Task<ControlExecution> DropFeedAsync(
        string relative, CancellationToken ct = default)
        => _client.DropFeedAsync(Resolve(relative), ct);

    /// <summary><c>DROP WRITER &lt;name&gt;</c></summary>
    public Task<ControlExecution> DropWriterAsync(
        string relative, CancellationToken ct = default)
        => _client.DropWriterAsync(Resolve(relative), ct);

    /// <summary><c>DROP READER &lt;name&gt;</c></summary>
    public Task<ControlExecution> DropReaderAsync(
        string relative, CancellationToken ct = default)
        => _client.DropReaderAsync(Resolve(relative), ct);

    /// <summary><c>DROP SUBSCRIPTION &lt;name&gt;</c></summary>
    public Task<ControlExecution> DropSubscriptionAsync(
        string relative, CancellationToken ct = default)
        => _client.DropSubscriptionAsync(Resolve(relative), ct);

    // ---- Pipes -------------------------------------------------------------
    // A Pipe is declared inside this Domain — its name resolves here —
    // but its Subscription source and output Feed/StateStore may point at
    // other Domains: pass the other Domain's handles or Scoped.Absolute
    // for those; plain strings resolve inside this Domain.

    /// <summary>A lifecycle handle on the Pipe <paramref name="relative"/>
    /// inside this Domain — Describe/Pause/Resume/Drop.</summary>
    public Pipe Pipe(string relative) => _client.Pipe(Resolve(relative));

    /// <summary>The declared Pipes inside this Domain.</summary>
    public async Task<IReadOnlyList<PipeInfo>> PipesAsync(CancellationToken ct = default)
    {
        var prefix = Name + ".";
        return (await _client.PipesAsync(ct).ConfigureAwait(false))
            .Where(pipe => pipe.Name.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
    }

    /// <summary><c>DEFINE PIPE &lt;name&gt; FROM &lt;subscription&gt; TO FEED
    /// &lt;outputFeed&gt;</c> — the copy Pipe. The Pipe name resolves inside
    /// this Domain; <paramref name="subscription"/> and
    /// <paramref name="outputFeed"/> accept a relative name (in-domain),
    /// a <see cref="Feed{TKey,TValue}"/>/<see cref="SubscriptionRef"/>
    /// handle, or <see cref="Scoped.Absolute"/> for cross-Domain.</summary>
    public Task<ControlExecution> DefineForwardPipeAsync(
        string name, Scoped subscription, Scoped outputFeed, CancellationToken ct = default)
        => _client.DefineForwardPipeAsync(
            Resolve(name), subscription.Resolve(this), outputFeed.Resolve(this), ct);

    /// <summary><c>DEFINE PIPE … ENRICH FROM &lt;store&gt;</c> — point-lookup
    /// enrichment; <paramref name="quarantineFeed"/> resolves the
    /// miss-quarantine target when the operation's miss disposition is
    /// <see cref="EnrichMiss.Quarantine"/>.</summary>
    public Task<ControlExecution> DefineEnrichPipeAsync(
        string name,
        Scoped subscription,
        Scoped outputFeed,
        Scoped store,
        PipeOperation operation,
        Scoped? quarantineFeed = null,
        CancellationToken ct = default)
        => _client.DefineEnrichPipeAsync(
            Resolve(name), subscription.Resolve(this), outputFeed.Resolve(this),
            store.Resolve(this), operation, quarantineFeed?.Resolve(this), ct);

    /// <summary><c>DEFINE PIPE … TO STATESTORE &lt;store&gt;</c> —
    /// rolling-window aggregation; <paramref name="emitFeed"/> optionally
    /// emits closed panes and requires the operation declare a lateness
    /// bound (<c>max_late_ns</c> or <c>watermark_ns</c>).</summary>
    public Task<ControlExecution> DefineWindowPipeAsync(
        string name,
        Scoped subscription,
        Scoped outputStore,
        PipeOperation operation,
        Scoped? emitFeed = null,
        CancellationToken ct = default)
        => _client.DefineWindowPipeAsync(
            Resolve(name), subscription.Resolve(this), outputStore.Resolve(this),
            operation, emitFeed?.Resolve(this), ct);

    // ---- StateStores --------------------------------------------------------

    /// <summary>A byte-level StateStore facade on <paramref name="relative"/>
    /// inside this Domain — rows live on the Nodes (RF3 + Fjall).</summary>
    public StateStore StateStore(string relative)
        => Whitewater.StateStore.Bound(_client.Transport, Resolve(relative));

    /// <summary>A codec-bound <see cref="Store{TKey,TValue}"/> facade on
    /// <paramref name="relative"/> inside this Domain.</summary>
    public Store<string, TValue> Store<TValue>(string relative, ICodec<TValue> valueCodec)
        => Whitewater.Store<string, TValue>.Bind(StateStore(relative), Codecs.Utf8, valueCodec);

    /// <inheritdoc cref="Store{TValue}(string, ICodec{TValue})"/>
    public Store<TKey, TValue> Store<TKey, TValue>(
        string relative, ICodec<TKey> keyCodec, ICodec<TValue> valueCodec)
        => Whitewater.Store<TKey, TValue>.Bind(StateStore(relative), keyCodec, valueCodec);

    /// <summary>The default binding — UTF-8 keys and untouched payload
    /// bytes.</summary>
    public Store<string, string> Store(string relative)
        => Whitewater.Store<string, string>.Bind(StateStore(relative), Codecs.Utf8, Codecs.Utf8);

    /// <summary>
    /// Declare <paramref name="spec"/> inside this Domain — the spec's
    /// name resolves in-domain. <paramref name="source"/> names a
    /// same-Domain Feed the store materializes from (null = manual);
    /// feed-sourced named keys must each declare an
    /// <see cref="Extractor"/>.
    /// </summary>
    public Task<StateStore> DefineStateStoreAsync(
        StateStore spec, Scoped? source = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(spec);
        var resolved = Whitewater.StateStore.Named(Resolve(spec.Name));
        foreach (var key in spec.NamedKeys)
        {
            resolved.AddKey(key.Name, key.Unique, key.Extractor);
        }
        return _client.StateStores.DefineAsync(resolved, source?.Resolve(this), ct);
    }

    /// <summary><c>DROP STATESTORE</c>: tombstone the named store.</summary>
    public Task<ControlExecution> DropStateStoreAsync(
        string relative, CancellationToken ct = default)
        => StateStore(relative).DropAsync(ct);

    // ---- Schemas -------------------------------------------------------------

    /// <summary><c>SET SCHEMA ON DOMAIN &lt;name&gt;</c> — this Domain's
    /// inherited default.</summary>
    public Task<ControlExecution> SetSchemaAsync(FeedSchema schema, CancellationToken ct = default)
        => _client.SetDomainSchemaAsync(Name, schema, ct);

    /// <summary><c>DROP SCHEMA ON DOMAIN &lt;name&gt;</c>.</summary>
    public Task<ControlExecution> ClearSchemaAsync(CancellationToken ct = default)
        => _client.ClearDomainSchemaAsync(Name, ct);

    /// <summary><c>SET SCHEMA ON FEED &lt;feed&gt;</c> — overrides the
    /// Domain default for the named sides.</summary>
    public Task<ControlExecution> SetFeedSchemaAsync(
        string relative, FeedSchema schema, CancellationToken ct = default)
        => _client.SetFeedSchemaAsync(Resolve(relative), schema, ct);

    /// <summary><c>DROP SCHEMA ON FEED &lt;feed&gt;</c> — the Feed reverts
    /// to Domain inheritance.</summary>
    public Task<ControlExecution> ClearFeedSchemaAsync(
        string relative, CancellationToken ct = default)
        => _client.ClearFeedSchemaAsync(Resolve(relative), ct);

    // ---- Indexes ---------------------------------------------------------------

    /// <summary>Query the replicated Index <paramref name="relative"/>
    /// inside this Domain by predicate — exact, prefix, or range over
    /// typed values. <paramref name="strict"/> waits for the committed
    /// target rather than answering stale.</summary>
    public Task<IndexPage> QueryIndexAsync(
        string relative,
        IndexPredicate predicate,
        int? limit = null,
        bool strict = false,
        CancellationToken ct = default)
        => _client.QueryIndexAsync(Resolve(relative), predicate, limit, strict, ct);

    internal string Resolve(string relative)
    {
        ArgumentNullException.ThrowIfNull(relative);
        var full = relative.StartsWith(Name + ".", StringComparison.Ordinal)
            ? relative
            : Name + "." + relative;
        return WhitewaterClient.DottedName(full);
    }

    private string Relative(string full)
        => full.StartsWith(Name + ".", StringComparison.Ordinal)
            ? full[(Name.Length + 1)..]
            : full;

    // ---- snapshot loading --------------------------------------------------

    private async Task<Snapshot> SnapAsync(CancellationToken ct)
    {
        if (_snapshot is { } snap)
        {
            return snap;
        }
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            _snapshot ??= await LoadAsync(ct).ConfigureAwait(false);
            return _snapshot;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// One batched control read: <c>describe domain</c> plus every
    /// <c>show</c> listing. Rows join on <c>space_id</c> — readers/
    /// writers join through their Feed's <c>feed_id</c>. A missing
    /// Domain fails the whole batch server-side.
    /// </summary>
    private async Task<Snapshot> LoadAsync(CancellationToken ct)
    {
        var execution = await _client.CommandsAsync(
            new JsonObject[]
            {
                new() { ["command"] = "describe", ["kind"] = "domain", ["name"] = Name },
                new() { ["command"] = "show", ["kind"] = "feeds" },
                new() { ["command"] = "show", ["kind"] = "readers" },
                new() { ["command"] = "show", ["kind"] = "writers" },
                new() { ["command"] = "show", ["kind"] = "subscriptions" },
                new() { ["command"] = "show", ["kind"] = "pipes" },
                new() { ["command"] = "show", ["kind"] = "indexes" },
                new() { ["command"] = "show", ["kind"] = "state_stores" },
            }, ct: ct).ConfigureAwait(false);

        var results = execution.Results;
        var describe = results.Count > 0 ? results[0].Data as JsonObject : null;
        var domainId = describe?["space_id"]?.GetValue<string>();
        if (string.IsNullOrEmpty(domainId))
        {
            throw new WhitewaterException.Refused(
                404, "domain.unknown", $"Domain '{Name}' is not declared");
        }

        var feeds = new List<string>();
        var feedIds = new HashSet<string>();
        var feedSchemas = new Dictionary<string, FeedSchema?>();
        var declaredSchemas = new Dictionary<string, FeedSchema?>();
        foreach (var row in Rows(results.Count > 1 ? results[1].Data : null))
        {
            if (row["space_id"]?.GetValue<string>() == domainId)
            {
                var name = row["name"]!.GetValue<string>();
                feeds.Add(name);
                feedIds.Add(row["feed_id"]!.GetValue<string>());
                feedSchemas[name] = FeedSchema.From(row["effective_schema"]);
                declaredSchemas[name] = FeedSchema.From(row["schema"]);
            }
        }
        feeds.Sort(StringComparer.Ordinal);

        return new Snapshot(
            domainId,
            describe!,
            execution.Revision,
            feeds,
            feedSchemas,
            declaredSchemas,
            Scoped(results, 2, "feed_id", feedIds),
            Scoped(results, 3, "feed_id", feedIds),
            Scoped(results, 4, "space_id", new HashSet<string> { domainId }),
            Scoped(results, 5, "space_id", new HashSet<string> { domainId }),
            Scoped(results, 6, "space_id", new HashSet<string> { domainId }),
            Scoped(results, 7, "space_id", new HashSet<string> { domainId }));
    }

    private static List<string> Scoped(
        IReadOnlyList<StatementResult> results, int index, string field, HashSet<string> ids)
    {
        var names = Rows(results.Count > index ? results[index].Data : null)
            .Where(row => ids.Contains(row[field]?.GetValue<string>() ?? ""))
            .Select(row => row["name"]?.GetValue<string>())
            .Where(name => name != null)
            .Cast<string>()
            .ToList();
        names.Sort(StringComparer.Ordinal);
        return names;
    }

    private static IEnumerable<JsonObject> Rows(JsonNode? data)
    {
        if (data is JsonArray array)
        {
            foreach (var item in array)
            {
                if (item is JsonObject row)
                {
                    yield return row;
                }
            }
        }
        else if (data is JsonObject obj && obj["rows"] is JsonArray wrapped)
        {
            foreach (var item in wrapped)
            {
                if (item is JsonObject row)
                {
                    yield return row;
                }
            }
        }
    }

    private sealed record Snapshot(
        string DomainId,
        JsonObject Describe,
        long Revision,
        List<string> Feeds,
        Dictionary<string, FeedSchema?> FeedSchemas,
        Dictionary<string, FeedSchema?> DeclaredSchemas,
        List<string> Readers,
        List<string> Writers,
        List<string> Subscriptions,
        List<string> Pipes,
        List<string> Indexes,
        List<string> StateStores);
}

/// <summary>
/// Where a brand-new Reader starts consuming — applies only on first
/// declaration; an existing Reader resumes its acknowledged frontier
/// (the <c>auto.offset.reset</c> mapping).
/// </summary>
public enum FirstTimeReaderStartPosition
{
    /// <summary>From the oldest retained record.</summary>
    Beginning,
    /// <summary>Only records appended after the Reader exists.</summary>
    End,
}

/// <summary>
/// Fluent declaration + session-open of a durable named Reader —
/// returned by <see cref="WhitewaterClient.CreateReader(Domain, Feed{TKey,TValue})"/>.
/// <see cref="ReaderId"/> is required: it is the durable consume
/// identity (the Kafka <c>groupId</c> analogue).
/// <see cref="ReaderStartPoint(FirstTimeReaderStartPosition)"/> applies
/// only when the Reader is first declared — an existing Reader resumes
/// its acknowledged frontier.
/// </summary>
public sealed class ReaderBuilder<TKey, TValue>
{
    private readonly WhitewaterClient _client;
    private readonly Feed<TKey, TValue> _feed;
    private string? _readerId;
    private ReaderStart _start = ReaderStart.Beginning();
    private int _maxPageSize = 128;

    internal ReaderBuilder(WhitewaterClient client, Domain domain, Feed<TKey, TValue> feed)
    {
        _client = client;
        Domain = domain;
        _feed = feed;
    }

    /// <summary>The Domain this Reader is scoped to.</summary>
    public Domain Domain { get; }

    /// <summary>The durable Reader identity. Required before
    /// open/declare.</summary>
    public ReaderBuilder<TKey, TValue> ReaderId(string readerId)
    {
        _readerId = readerId;
        return this;
    }

    /// <summary>First-time start position —
    /// <see cref="FirstTimeReaderStartPosition.Beginning"/> or
    /// <c>End</c>. For cursor/timestamp positions use
    /// <see cref="ReaderStartPoint(ReaderStart)"/>.</summary>
    public ReaderBuilder<TKey, TValue> ReaderStartPoint(FirstTimeReaderStartPosition position)
        => ReaderStartPoint(position == FirstTimeReaderStartPosition.End
            ? ReaderStart.Now()
            : ReaderStart.Beginning());

    /// <summary>First-time start position — the full
    /// <see cref="ReaderStart"/> form (cursor, timestamp).</summary>
    public ReaderBuilder<TKey, TValue> ReaderStartPoint(ReaderStart start)
    {
        _start = start ?? throw new ArgumentNullException(nameof(start));
        return this;
    }

    /// <summary>Max records delivered per fetch page — the default is 128.</summary>
    public ReaderBuilder<TKey, TValue> MaxPageSize(int maxPageSize)
    {
        if (maxPageSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxPageSize), "maxPageSize must be positive");
        }
        _maxPageSize = maxPageSize;
        return this;
    }

    /// <summary>Declare the Reader in the catalog without opening a
    /// session — fails when the name is already declared.</summary>
    public Task<ControlExecution> DeclareAsync(CancellationToken ct = default)
        => _client.CreateReaderAsync(RequireId(), _feed.Name, _start, ct);

    /// <summary>Declare the Reader when missing (an existing
    /// declaration keeps its start and durable progress), open a
    /// session, and bind the Feed's codecs — <c>FetchAsync</c> returns
    /// decoded <see cref="FeedMessage{TKey,TValue}"/>s.</summary>
    public Task<FeedReader<TKey, TValue>> OpenAsync(CancellationToken ct = default)
        => _feed.ReaderAsync(RequireId(), _maxPageSize, _start, ct);

    private string RequireId()
        => Domain.Resolve(_readerId ?? throw new InvalidOperationException(
            "ReaderId(...) is required — the durable consume identity" +
            " (the Kafka groupId analogue)"));
}

/// <summary>
/// Fluent declaration + session-open of a durable named Writer —
/// returned by <see cref="WhitewaterClient.CreateWriter(Domain, Feed{TKey,TValue})"/>.
/// </summary>
public sealed class WriterBuilder<TKey, TValue>
{
    private readonly WhitewaterClient _client;
    private readonly Feed<TKey, TValue> _feed;
    private string? _writerId;

    internal WriterBuilder(WhitewaterClient client, Domain domain, Feed<TKey, TValue> feed)
    {
        _client = client;
        Domain = domain;
        _feed = feed;
    }

    /// <summary>The Domain this Writer is scoped to.</summary>
    public Domain Domain { get; }

    /// <summary>The durable Writer identity. Required before
    /// open/declare.</summary>
    public WriterBuilder<TKey, TValue> WriterId(string writerId)
    {
        _writerId = writerId;
        return this;
    }

    /// <summary>Declare the Writer in the catalog without opening a
    /// session — fails when the name is already declared.</summary>
    public Task<ControlExecution> DeclareAsync(CancellationToken ct = default)
        => _client.CreateWriterAsync(RequireId(), _feed.Name, ct);

    /// <summary>Declare the Writer when missing, open a session (the
    /// fencing epoch stays inside the handle), and bind the Feed's
    /// codecs.</summary>
    public Task<FeedWriter<TKey, TValue>> OpenAsync(CancellationToken ct = default)
        => _feed.WriterAsync(RequireId(), ct);

    private string RequireId()
        => Domain.Resolve(_writerId ?? throw new InvalidOperationException("WriterId(...) is required"));
}
