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

    /// <summary><c>DEFINE PIPE &lt;name&gt; FROM &lt;subscription&gt; TO FEED
    /// &lt;outputFeed&gt;</c> — the copy Pipe.</summary>
    public Task<ControlExecution> DefineForwardPipeAsync(
        string name, string subscription, string outputFeed, CancellationToken ct = default)
        => CommandsAsync(
            new JsonObject[]
            {
                new()
                {
                    ["command"] = "define_pipe",
                    ["name"] = Dotted(name),
                    ["subscription"] = Dotted(subscription),
                    ["output_feed"] = Dotted(outputFeed),
                    ["operation"] = PipeOperation.Forward().ToWire(),
                },
            }, ct: ct);

    /// <summary><c>DEFINE PIPE … ENRICH FROM &lt;store&gt;</c> — point-lookup
    /// enrichment; <paramref name="quarantineFeed"/> resolves the
    /// miss-quarantine target when the operation's miss disposition is
    /// <see cref="EnrichMiss.Quarantine"/>.</summary>
    public async Task<ControlExecution> DefineEnrichPipeAsync(
        string name,
        string subscription,
        string outputFeed,
        string store,
        PipeOperation operation,
        string? quarantineFeed = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        if (operation.Operation != "enrich")
        {
            throw new ArgumentException(
                "enrich pipes take PipeOperation.Enrich(...)", nameof(operation));
        }
        var command = new JsonObject
        {
            ["command"] = "define_pipe",
            ["name"] = Dotted(name),
            ["subscription"] = Dotted(subscription),
            ["output_feed"] = Dotted(outputFeed),
            ["output_store"] = Dotted(store),
        };
        if (quarantineFeed != null)
        {
            command["quarantine_feed"] = Dotted(quarantineFeed);
        }
        command["operation"] = operation.ToWire();
        return await CommandsAsync(new JsonObject[] { command }, ct: ct)
            .ConfigureAwait(false);
    }

    /// <summary><c>DEFINE PIPE … TO STATESTORE &lt;store&gt;</c> —
    /// rolling-window aggregation; <paramref name="emitFeed"/> optionally
    /// emits closed panes and requires the operation declare a lateness
    /// bound (<c>max_late_ns</c> or <c>watermark_ns</c>).</summary>
    public async Task<ControlExecution> DefineWindowPipeAsync(
        string name,
        string subscription,
        string outputStore,
        PipeOperation operation,
        string? emitFeed = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        if (operation.Operation != "window")
        {
            throw new ArgumentException(
                "window pipes take PipeOperation.Window(...)", nameof(operation));
        }
        var command = new JsonObject
        {
            ["command"] = "define_pipe",
            ["name"] = Dotted(name),
            ["subscription"] = Dotted(subscription),
            ["output_store"] = Dotted(outputStore),
        };
        if (emitFeed != null)
        {
            command["emit_feed"] = Dotted(emitFeed);
        }
        command["operation"] = operation.ToWire();
        return await CommandsAsync(new JsonObject[] { command }, ct: ct)
            .ConfigureAwait(false);
    }

    // ---- record schemas -----------------------------------------------------

    /// <summary><c>SET SCHEMA ON DOMAIN &lt;name&gt;</c> — the Domain's
    /// inherited default.</summary>
    public Task<ControlExecution> SetDomainSchemaAsync(
        string domain, FeedSchema schema, CancellationToken ct = default)
        => SetSchemaAsync("domain", domain, schema, ct);

    /// <summary><c>SET SCHEMA ON FEED &lt;feed&gt;</c> — overrides the
    /// named sides.</summary>
    public Task<ControlExecution> SetFeedSchemaAsync(
        string feed, FeedSchema schema, CancellationToken ct = default)
        => SetSchemaAsync("feed", feed, schema, ct);

    /// <summary><c>DROP SCHEMA ON DOMAIN &lt;name&gt;</c>.</summary>
    public Task<ControlExecution> ClearDomainSchemaAsync(
        string domain, CancellationToken ct = default)
        => ClearSchemaAsync("domain", domain, ct);

    /// <summary><c>DROP SCHEMA ON FEED &lt;feed&gt;</c> — the Feed
    /// reverts to Domain inheritance.</summary>
    public Task<ControlExecution> ClearFeedSchemaAsync(
        string feed, CancellationToken ct = default)
        => ClearSchemaAsync("feed", feed, ct);

    private async Task<ControlExecution> SetSchemaAsync(
        string kind, string name, FeedSchema schema, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var command = new JsonObject
        {
            ["command"] = "set_schema",
            ["scope"] = new JsonObject { ["kind"] = kind, ["name"] = Dotted(name) },
            ["schema"] = schema.ToWire(),
        };
        return await CommandsAsync(new JsonObject[] { command }, ct: ct)
            .ConfigureAwait(false);
    }

    private async Task<ControlExecution> ClearSchemaAsync(
        string kind, string name, CancellationToken ct)
    {
        var command = new JsonObject
        {
            ["command"] = "clear_schema",
            ["scope"] = new JsonObject { ["kind"] = kind, ["name"] = Dotted(name) },
        };
        return await CommandsAsync(new JsonObject[] { command }, ct: ct)
            .ConfigureAwait(false);
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

    // ---- strongly-typed Domains ---------------------------------------------
    // A Domain owns every Feed it prefixes (shop owns shop.orders). The
    // typed entry point:
    //
    //   var shop = await client.GetDomainAsync("shop");
    //   await using var reader = await client.CreateReader(shop, shop.Feed("orders"))
    //       .ReaderId("fulfillment")
    //       .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning)
    //       .OpenAsync();

    /// <summary>A lazy <see cref="Domain"/> handle — no I/O until a
    /// catalog accessor runs; <see cref="Domain.Feed(string)"/> name
    /// composition stays free.</summary>
    public Domain Domain(string name) => new(this, name);

    /// <summary><see cref="Domain(string)"/> plus an immediate snapshot
    /// load — verifies the Domain exists now.</summary>
    public Task<Domain> GetDomainAsync(string name, CancellationToken ct = default)
        => Domain(name).RefreshAsync(ct);

    /// <summary>
    /// A fluent <see cref="ReaderBuilder{TKey,TValue}"/> bound to
    /// <paramref name="domain"/> + <paramref name="feed"/> —
    /// <c>client.CreateReader(shop, orders).ReaderId("audit")
    /// .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning)
    /// .OpenAsync()</c>. A <see cref="Feed{TKey,TValue}"/> bound to
    /// another Domain is refused locally.
    /// </summary>
    public ReaderBuilder<TKey, TValue> CreateReader<TKey, TValue>(
        Domain domain, Feed<TKey, TValue> feed)
    {
        ArgumentNullException.ThrowIfNull(feed);
        RequireOwnedFeed(domain, feed.Name);
        return new ReaderBuilder<TKey, TValue>(this, domain, feed);
    }

    /// <summary><see cref="CreateReader{TKey,TValue}(Domain, Feed{TKey,TValue})"/>
    /// by relative Feed name — codecs default to UTF-8 keys/byte payloads.</summary>
    public ReaderBuilder<string, byte[]> CreateReader(Domain domain, string feed)
        => CreateReader(domain, domain.Feed(feed));

    /// <summary>
    /// A fluent <see cref="WriterBuilder{TKey,TValue}"/> bound to
    /// <paramref name="domain"/> + <paramref name="feed"/> —
    /// <c>client.CreateWriter(shop, orders).WriterId("ingest").OpenAsync()</c>.
    /// </summary>
    public WriterBuilder<TKey, TValue> CreateWriter<TKey, TValue>(
        Domain domain, Feed<TKey, TValue> feed)
    {
        ArgumentNullException.ThrowIfNull(feed);
        RequireOwnedFeed(domain, feed.Name);
        return new WriterBuilder<TKey, TValue>(this, domain, feed);
    }

    /// <summary><see cref="CreateWriter{TKey,TValue}(Domain, Feed{TKey,TValue})"/>
    /// by relative Feed name — codecs default to UTF-8 keys/byte payloads.</summary>
    public WriterBuilder<string, byte[]> CreateWriter(Domain domain, string feed)
        => CreateWriter(domain, domain.Feed(feed));

    private static void RequireOwnedFeed(Domain domain, string? feed)
    {
        ArgumentNullException.ThrowIfNull(domain);
        ArgumentNullException.ThrowIfNull(feed);
        if (!feed.StartsWith(domain.Name + ".", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"feed '{feed}' is outside Domain '{domain.Name}'", nameof(feed));
        }
    }

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

    /// <summary>Resource names (writers, readers, subscriptions, roles)
    /// share the same dotted grammar the server validates.</summary>
    private static string Resource(string name) => Dotted(name);

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
        Action<WhitewaterException>? onError = null,
        int maxPageSize = SubscribeCapacity)
        => new(this, reader, feed, start, maxPageSize, onRecords, onError);

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
