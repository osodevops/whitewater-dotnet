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
/// await using var reader = await client
///     .CreateReader(shop, shop.Feed("orders"))
///     .ReaderId("fulfillment")                       // the Kafka groupId analogue
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
        return snap.FeedSchemas.TryGetValue(feed, out var schema) ? schema : null;
    }

    /// <summary>Only the sides the Feed itself declares — null when it
    /// inherits the Domain default wholesale.</summary>
    public async Task<FeedSchema?> DeclaredFeedSchemaAsync(string feed, CancellationToken ct = default)
    {
        var snap = await SnapAsync(ct).ConfigureAwait(false);
        return snap.DeclaredSchemas.TryGetValue(feed, out var schema) ? schema : null;
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
        => new(_client, Resolve(relative), Codecs.Utf8, Codecs.Utf8);

    /// <summary><see cref="Feed(string)"/> with UTF-8 keys and payloads
    /// of <typeparamref name="TValue"/>.</summary>
    public Feed<string, TValue> Feed<TValue>(string relative, ICodec<TValue> valueCodec)
        => new(_client, Resolve(relative), Codecs.Utf8, valueCodec);

    /// <summary><see cref="Feed(string)"/> with a typed key codec too.</summary>
    public Feed<TKey, TValue> Feed<TKey, TValue>(
        string relative, ICodec<TKey> keyCodec, ICodec<TValue> valueCodec)
        => new(_client, Resolve(relative), keyCodec, valueCodec);

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
        => _readerId ?? throw new InvalidOperationException(
            "ReaderId(...) is required — the durable consume identity" +
            " (the Kafka groupId analogue)");
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
        => _writerId ?? throw new InvalidOperationException("WriterId(...) is required");
}
