# whitewater-dotnet

.NET SDK for Whitewater. Implements the versioned `whitewater-protocol`
wire contract defined in the main repository under `sdk/spec/`
(`PROTOCOL.md`, `endpoints.json`, `conformance/`): decimal-string signed
64-bit nanoseconds, base64 binary fields, opaque cursors, UUID request
ids, and the shared structured error codes.

Targets .NET 8. Zero external dependencies: `System.Net.Http` for
transport, `System.Text.Json` for the wire codec.

A complete microservice walkthrough — provision, produce, pump-process,
dead-letter, shutdown — lives in
[`docs/demo-service.md`](docs/demo-service.md).

## Creating a client

```csharp
var client = new WhitewaterClient(
    apiKey: Environment.GetEnvironmentVariable("WHITEWATER_API_KEY")!,
    endpoints: new[] {
        new Uri("https://node1:7070"),
        new Uri("https://node2:7070"),
        new Uri("https://node3:7070"),
    });
```

`await client.HealthyAsync()` checks liveness. The client also carries
the flat administration surface — `WclAsync`/`CommandsAsync`, node
health, Domain and Role lifecycle — while every Feed, Writer, Reader,
Subscription, Member, Pipe, StateStore, and Index is reached **only**
through a `Domain`.

Whitewater's public model is `domain -> feed -> key -> cursor ->
subscription`: partitions, ranges, replicas, and physical positions are
internal implementation details — cursors are opaque tokens, never
parsed.

## Domains — the strongly-typed front door

A Domain is the administrative namespace every business resource lives
inside: a Feed named `shop.orders` belongs to the Domain `shop`, and
nothing outside a Domain can reach it. Writers, Readers, Subscriptions,
StateStores, Indexes, and Pipes resolve their relative names inside the
owning Domain, so a `"fulfillment"` Reader declared on `shop` is
`shop.fulfillment` everywhere it matters: progress, leases, WCL, demand
reports, and access control all scope to it. Cross-Domain references are
possible but always explicit (`Scoped.Absolute` or a handle), which
makes accidental cross-wiring of two tenants' resources a deliberate
act rather than a silent mistake.

```csharp
var shop = await client.CreateDomainAsync("shop");  // CREATE DOMAIN shop;
shop = await client.GetDomainAsync("shop");         // verifies + materializes now
// client.Domain("shop") is the lazy form — zero I/O until needed
await shop.CreateFeedAsync("orders");               // CREATE FEED shop.orders;

await shop.FeedNamesAsync();                          // ['shop.orders', ...]
(await shop.FeedsAsync("order")).Select(f => f.Name); // prefix-filtered
await shop.ReadersAsync(); await shop.WritersAsync(); // scoped catalog views
await shop.SubscriptionsAsync(); await shop.PipesAsync();
await shop.IndexesAsync();       await shop.StateStoresAsync();
await shop.DescribeAsync();      await shop.HistoryPolicyAsync();
await shop.RefreshAsync();                            // re-pull the snapshot

// Everything a service needs hangs off the Domain — relative names
// resolve inside it:
var writer = await shop
    .CreateWriter("orders")
    .WriterId("ingest")                            // durable producer identity — required
    .OpenAsync();

await using var reader = await shop
    .CreateReader("orders")
    .ReaderId("fulfillment")                                // durable consume identity — Kafka groupId
    .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning) // first-declare only — auto.offset.reset
    .MaxPageSize(256)                                         // max records per fetch
    .OpenAsync();
```

`ReaderId` is durable application state: `ReaderStartPoint` applies
only when the Reader is first declared — an existing Reader resumes its
acknowledged frontier. For cursor/timestamp positions the
`ReaderStartPoint(ReaderStart)` overload accepts the full form.
`WriterId` is required because it's the Writer's transactional
identity: the fencing epoch binds to it and the retry-dedup ledger
lives under it. A `Feed` bound to another Domain is refused locally,
before any network call. The first catalog accessor issues ONE batched
control read (`describe domain` + every `show` listing) and caches the
snapshot — `RefreshAsync()` re-pulls.

### Record schemas

Records stay schema-neutral bytes on the wire; a Feed can still declare
schema metadata the catalog tracks for tooling, validation, and
compatibility. The Domain's schema is the default every Feed inherits;
a Feed's own declaration overrides the named sides:

```csharp
await shop.SetSchemaAsync(
    new FeedSchema(Value: new SchemaDecl("json", Subject: "order-v1")));
await shop.SetFeedSchemaAsync("orders",
    new FeedSchema(Key: new SchemaDecl("text")));

await shop.SchemaAsync();                 // the Domain default
await shop.FeedSchemaAsync("orders");     // effective: key=text, value=json/order-v1
await shop.DeclaredFeedSchemaAsync("orders"); // the Feed's own sides only
await shop.ClearFeedSchemaAsync("orders");    // reverts to inheritance
```

`SET SCHEMA ON DOMAIN|FEED <name> (KEY|VALUE <format> [SUBJECT 'x']
[DEFINITION 'y'])+` in WCL does the same; `SHOW SCHEMAS` lists declared
assignments. Formats today: `json`, `jsonschema`, `avro`, `protobuf`,
`text`, `bytes`, or `custom.<name>` — compatibility enforcement is
still a no-op.

## Working with Feeds

`shop.Feed("orders", Codecs.Json<Order>())` binds a feed to key and
payload codecs once; every write encodes and every read decodes through
them — application code never calls `Encoding.UTF8`/`JsonSerializer`
per record. `domain.Feed(name)` binds the default
`Feed<string, string>` — UTF-8 both ways, so plain JSON flows with no
codec ceremony; `feed.View(kc, vc)` rebinds codecs without a wire call.

```csharp
public sealed record Order(string Id, int Total);

var orders = shop.Feed("orders", Codecs.Json<Order>());

Codecs.Utf8            // UTF-8 text (JSON/XML/etc. as strings)
Codecs.Bytes           // raw byte[] identity
Codecs.Json<T>()       // System.Text.Json — web defaults (camelCase)
Codecs.For<T>()        // infers: string→UTF-8, byte[]→raw, else JSON
Codecs.Custom<T>(e,d)  // protobuf, msgpack, application framing
Codecs.TextMetadata(m) // text Metadata map -> wire bytes
```

### Reading — durable Readers and ad-hoc fetches

A **Reader** owns a durable, server-side cursor — the progress identity
that survives restarts, exactly like a committed Kafka group offset but
without the group protocol:

```csharp
await using var reader = await shop
    .CreateReader("orders")
    .ReaderId("fulfillment")                                // durable consume identity
    .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning) // first-declare start position
    .MaxPageSize(256)                                         // max records per fetch
    .OpenAsync();

var page = await reader.FetchAsync();           // up to MaxPageSize records
foreach (var m in page.Records) Console.WriteLine($"{m.Key} {m.Value.Total}");
await reader.AckAsync();                        // commits the delivered frontier
```

Two SDK-owned loops sit on top of the same fetch→ack cycle —
`reader.ForEachAsync(handler)` and the `IAsyncEnumerable`
`reader.MessagesAsync(ct)`:

```csharp
await reader.ForEachAsync(async m => { await handle(m.Value); return true; });
await foreach (var m in reader.MessagesAsync(ct)) await handle(m.Value);
// or stay manual: FetchAsync → handle records → AckAsync
```

For ad-hoc reads without a durable identity, `feed.RecordsAsync`,
`feed.FetchAsync` (`tail`/`wait_ms` long-poll, `after_event_time_ns`
seeks), and `feed.LoadAsync(start, sink)` one-shot replays cover
committed history:

```csharp
var loaded = await hr.Feed("employees").LoadAsync(
    ReaderStart.Beginning(),
    batch => { Export(batch); return Task.CompletedTask; });
```

### Shared progress — subscriptions and members

A named **Subscription** is the durable shared frontier cooperating
readers claim work against — the Kafka consumer-group model, minus the
rebalance storm. `domain.Member(name)` joins a member and runs the
work loop:

```csharp
var member = shop.Member("orders-sub");
await member.JoinAsync("member-1");            // register this member
var work = await member.ClaimAsync();          // RF3-fenced work lease
var page = await member.FetchAsync(work, 256); // records for this member
await member.AckAsync(work, page);             // commits shared progress
await member.ReleaseAsync(work);               // or transfer to another member
```

Leases are fenced — a lost or expired claim is refused, and work
transfers cleanly to another member. `Feed.Subscribe`/`Domain.Subscribe`
runs the managed `Open -> [Fetch -> onRecords -> Ack] -> Close` loop on
a background task:

```csharp
await using var sub = hr.Subscribe("hr-view", "employees",
    ReaderStart.Now(),
    batch => { Export(batch); return Task.CompletedTask; },
    error => Console.Error.WriteLine($"subscription ended: {error}"));
```

### The read loop — consumers

`feed.Consumer(name)` binds the durable named Reader (the
consumer-group equivalent) and owns the fetch → deliver → ack cycle:
transient failures retry with backoff, a fenced session re-opens,
progress resumes from the server-side cursor. Application code only
sees decoded records, one at a time:

```csharp
await foreach (var msg in feed.ConsumeAsync("enricher", stoppingToken))
{
    await ProcessAsync(msg);         // throw -> the page stays unacked
}                                   //          and redelivers (at-least-once)

// Or a handler form that completes only on DisposeAsync / ct:
await feed.Consumer("enricher")
    .StartAt(ReaderStart.Beginning())        // first declaration only
    .WithBackoff(TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(30))
    .OnError(e => log.LogWarning(e, "consumer restarted"))
    .RunAsync(ProcessAsync, stoppingToken);
```

- The named Reader's acknowledged cursor is durable server-side — a
  service restart resumes where it stopped; no offset management.
- Each fetched page is delivered record-by-record; its delivered cursor
  acks only after every record delivered cleanly. A thrown handler (or
  an abandoned `await foreach`) leaves the page unacked, so it
  redelivers — route poison pills to a DLQ feed yourself.
- Transient failures (`Unavailable`, `AmbiguousResult`) retry with
  exponential backoff; a fenced session re-opens and resumes. Startup
  refusals (`Refused`, invalid names, auth) surface on
  `RunAsync`/`Messages` — fail fast, never loop.
- `await DisposeAsync()` (or the cancellation token) finishes the
  in-flight page, acks it, releases the session — the SIGTERM path.
  It is safe to call from inside a handler.

See [examples/EnrichUsers](examples/EnrichUsers/Program.cs) for the full
enrichment service — primary/named-key store lookups, DLQ, counters.

### Writing

```csharp
// Declares + opens the shop.ingest session; each AppendAsync completes
// after majority commit (at least 2 of 3 Nodes):
var writer = await orders.WriterAsync("ingest");
var appended = await writer.AppendAsync("order-42", new Order("order-42", 99));
// appended: MessageId, Cursor, Deduplicated, Durability — the ack payload

// Idempotent events: pin requestId for caller-driven retries —
// the same identity replays its committed result instead of writing twice.
await writer.AppendEventAsync(new FeedEvent<string, Order>(
    order, "order-42", requestId: Guid.NewGuid(),
    metadata: Codecs.TextMetadata(new() { ["source"] = "api" })));

// One commit for a group of records:
await writer.AppendAllAsync(new[] { e1, e2 });
```

Every append carries an idempotent `request_id` — retrying a timed-out
append with the same identity deduplicates; reusing it with different
contents is refused. Writers are epoch-fenced sessions: a second writer
claiming `ingest` fences the first, so an ambiguous timeout can never
double-commit under retry. `UseCompression` opts into LZ4; record
`event_time_ns`/`ingest_time_ns` are `long` epoch nanoseconds,
serialized as decimal strings on the wire; `TimeSpan` covers window and
delay declarations.

The raw `byte[]` session API stays available for full control —
`shop.OpenWriterSessionAsync` + `shop.Writer(name, epoch)` appends with
caller-owned `requestId`/`eventTimeNs`/metadata, and
`shop.Reader(name)` + `OpenAsync`/`FetchAsync`/`AckAsync` runs the
byte-level read loop.

## Working with StateStores

StateStores are durable, replicated key/value views that live on the
Nodes — the SDK holds no local copy. Declare named keys up front and
each becomes a maintained secondary index.

### Declaring a store — named keys

```csharp
var accounts = await client.GetDomainAsync("accounts");
var spec = StateStore.Named("employees")
    .AddUniqueKey("passport")                  // one row per value
    .AddNonUniqueKey("department");            // value → many rows; up to 10 keys
var store = await accounts.DefineStateStoreAsync(spec);  // declares accounts.employees
```

**Unique** (`passport`, `email`, `order_number`) — one value maps to at
most one row; `GetByAsync` returns the single match and a second claim
on the same value arbitrates first-committed-wins. **Non-unique**
(`department`, `status`, `region`) — one value maps to many rows;
`GetByAsync` returns every match as a bounded page.

Two store kinds: **manual** (your app puts rows — no source Feed) and
**feed-sourced** (the Nodes replay a source Feed and project rows +
postings via `Extractor.RecordKey()` / `Extractor.Metadata(k)` /
`Extractor.Json(path)` — required on feed-sourced keys, refused on
manual ones).

### Putting and deleting

Named-key postings take a typed `IStoreKeySet`, a builder lambda, or a
raw dictionary:

```csharp
var profiles = accounts.Store("employees", Codecs.Json<Profile>());

await profiles.PutAsync("emp-1", payload, PassportKeySet.With("P123", "eng"));
await profiles.PutAsync("emp-1", payload,                     // typed key set
    new Dictionary<string, object?> { ["passport"] = "P123", ["department"] = "eng" });
                                                            // raw dictionary — no checking

await store.DeleteAsync("emp-1");   // mutation id generated
await store.ClearAsync();           // one journaled ClearAll
```

Every mutation carries an idempotent `mutation_id` — generated per
call, or pinned for caller-driven retries.

### Reading

```csharp
var row = await store.GetAsync("emp-1");              // StoreRow | null + freshness
var profile = await profiles.FindAsync("user-1")      // value or null — no row wrapper
    ?? await profiles.FindByAsync("passport", "P111");  // named key, first hit
var n = await counters.GetOrDefaultAsync("hits", 0);    // counters stay one line

var rows = await store.GetByAsync("passport", "P123");  // every match
var page = await store.PageAsync(afterKey, 256);        // one bounded page
await foreach (var r in store.IterateAsync()) Export(r); // primary-key order
```

`GetAsync`/`GetByAsync` return rows carrying applied-cursor freshness
evidence — `strict` bounded-waits for the replica to apply its
committed frontier rather than answering stale. The byte-level
`domain.StateStore(name)` facade remains for raw access. See
[docs/stores.md](docs/stores.md) for the full store guide.

## Working with Pipes

Managed Pipes are server-run stream jobs: you declare the business
shape — source Subscription, output Feed, operation — and the server
owns the subscription frontier, compute lease, panes, and the effect
journal. String endpoints resolve inside the declaring Domain; a
`Feed`/`Store`/`SubscriptionRef` handle or `Scoped.Absolute` crosses
Domains explicitly. The Pipe's own name and its input Subscription
always stay in the declaring Domain. See
[docs/pipes.md](docs/pipes.md) for the full pipe guide.

### Simple — forwarding

```csharp
var orders = await client.GetDomainAsync("orders");
await orders.DefineForwardPipeAsync("relay",
    "sub",           // source Subscription — orders.sub
    "relayed");      // output Feed — orders.relayed
```

### Enrichment — StateStore lookups

```csharp
await orders.DefineEnrichPipeAsync("enrich", "sub",
    "enriched", crm.StateStore("customers"),   // cross-Domain lookup store
    PipeOperation.Enrich(Extractor.RecordKey(),  // join on record key
        freshness: EnrichFreshness.Latest,
        onMiss: EnrichMiss.Quarantine),            // misses → DLQ
    quarantineFeed: "enrich.dlq");
```

### Time-based — windowing

```csharp
await orders.DefineWindowPipeAsync("hourly", "sub",
    stats.Scoped("orders.hourly"),            // cross-Domain pane store
    PipeOperation.Window(TimeSpan.FromHours(1),  // 1h panes
        Extractor.RecordKey(), WindowAggregation.Count(),
        time: WindowTime.Event,
        watermark: TimeSpan.FromMinutes(5),  // 5m horizon
        grace: TimeSpan.FromSeconds(30),     // 30s late revisions
        expire: TimeSpan.FromHours(2)),      // pane expiry
    emitFeed: "hourly.emitted");
```

### Indexes and observability

```csharp
// Pipe lifecycle: describe, pause, resume, drop.
var pipe = orders.Pipe("relay");
var info = await pipe.DescribeAsync();   // stage, operation, outputs, status
await pipe.PauseAsync(); await pipe.ResumeAsync(); await pipe.DropAsync();
var pipes = await orders.PipesAsync();   // the domain's declared catalog

// Index queries: exact, prefix, or range predicates over decoded rows.
var page = await crm.QueryIndexAsync(
    "byregion",
    IndexPredicate.Range(
        new object?[] { "emea" },
        IndexBound.InclusiveBound(0L), IndexBound.ExclusiveBound(99L)),
    limit: 50,           // rows per page
    strict: true);       // wait for the committed target rather than stale
foreach (var row in page.Rows) { /* row.Key / row.Payload bytes */ }

// Demand observability: typed rows + per-domain rollups.
var report = await client.DemandFeedsAsync();
var appended = report.Rows[0].Counter("appended_records");
await client.DemandReadersAsync(); await client.DemandSubscriptionsAsync();
await client.DemandIndexesAsync(); await client.DemandPipesAsync();
var hot = await client.HotKeysAsync(); // dominant-key evidence per feed
```

Pipe operation payloads (`PipeInfo.Operation`) and demand fields
(`DemandRow.Fields`, `HotKey.Evidence`) stay opaque JSON so server-side
variants evolve without an SDK bump. `QueryIndexAsync` values accept the
same typed scalars as StateStore named values — floats are refused
because the wire carries exact `i64`/`text`/`bytes`.

## Errors, cursors, and time

Errors surface as `WhitewaterException` subtypes mapped from the wire
`code` (`Code` carries it):

```csharp
catch (WhitewaterException.StaleSession)   { /* re-open the session */ }
catch (WhitewaterException.AmbiguousResult){ /* may have committed — dedup retry */ }
catch (WhitewaterException.Unavailable)    { /* safe retry after backoff */ }
catch (WhitewaterException.LeaseConflict)  { /* lease lost or contended */ }
catch (WhitewaterException.Conflict)       { /* fencing/conflict; inspect Code */ }
catch (WhitewaterException.Refused)        { /* request rejected; fix and retry */ }
```

Cursors are opaque: the SDK never parses, orders, or constructs them.
`event_time_ns`/`ingest_time_ns` are `long` epoch nanoseconds traveling
as decimal strings on the wire; `TimeSpan` covers window and delay
declarations.

## Build

```powershell
dotnet build Whitewater.sln
dotnet test Whitewater.sln   # unit tests; conformance skips without env vars
```

## Unit-test doubles

`Whitewater.Testing.FakeRiverbed` serves the wire contract over a loopback
`HttpListener` — request-id dedupe, ack-gated redelivery, epoch fencing,
modeled StateStores — so application tests exercise the real client.

```csharp
await using var fake = new FakeRiverbed();
var client = new WhitewaterClient(apiKey, fake.Endpoints);
```

See `tests/Whitewater.Tests/FakeRiverbedTests.cs` and the
[testing guide](../whitewater/docs/guides/testing-your-app.md).

## Conformance

The shared fixture corpus runs against a live three-Node Riverbed:

```powershell
$env:WHITEWATER_CONFORMANCE_URLS = "http://localhost:7371,http://localhost:7372,http://localhost:7373"
$env:WHITEWATER_FIXTURE_DIR = "C:\Code\Stackapps\whitewater\sdk\spec\conformance"
$env:FINNSTREAM_ADMIN_API_KEY = "whitewater-local-development-admin-key"
dotnet test tests/Whitewater.Tests --filter "FullyQualifiedName~ConformanceRunner"
```

## Full API surface

Everything the SDK exposes, grouped by capability. The typed surface
(`Domain` → `Feed` → writer/reader/consumer) covers the daily path;
`CommandsAsync`, `WclAsync`, and the `*SessionClient` classes are the
explicit escape hatches for the same wire contract.

| Method | Parameters | Description |
|---|---|---|
| **Client & transport** | | |
| `new WhitewaterClient(apiKey, endpoints, timeout?)` | api key, `Uri[]`, timeout | Client construction |
| `HealthyAsync` `UseCompression` | — | Liveness, LZ4 on feeds/writers |
| **Domains — the only door** | | |
| `CreateDomainAsync` / `Domain(name)` / `GetDomainAsync(name)` | relative name | Declare / lazy / eager handle |
| `Feed(name[, codecs])` `Store(name[, codecs])` `View<TK2,TV2>` | name, `Codecs.*` | Typed `Feed`/`Store` handles |
| `CreateFeedAsync` `CreateReader` `CreateWriter` `CreateSubscriptionAsync` | relative name | Declare + open inside the Domain |
| `FeedsAsync` `FeedNamesAsync` `ReadersAsync` `WritersAsync` `SubscriptionsAsync` `PipesAsync` `IndexesAsync` `StateStoresAsync` | optional prefix | Domain catalog views |
| `DomainIdAsync` `RevisionAsync` `DescribeAsync` `HistoryPolicyAsync` `RefreshAsync` | — | Domain metadata |
| `SetSchemaAsync` `ClearSchemaAsync` `SetFeedSchemaAsync` `ClearFeedSchemaAsync` `SchemaAsync` `FeedSchemaAsync` `DeclaredFeedSchemaAsync` | `FeedSchema`/`SchemaDecl` — `json`, `avro`, `protobuf`, `text`, `bytes`, `custom.<name>` | Record-schema declarations |
| **Feeds** | | |
| `RecordsAsync` `FetchAsync` `LoadAsync(start, sink)` | limit, cursor, `ReaderStart` | Committed history reads |
| `WriterAsync` `ReaderAsync` `Subscribe` `Consumer` | name, options | Bound sessions + loops |
| **Writing** | | |
| `FeedWriter.AppendAsync(k, v)` `AppendEventAsync` `AppendAllAsync` | key, value, `FeedEvent` | Write completing on majority commit — `AppendResult` |
| `SessionEpoch` `UseCompression` | — | Session + lifecycle |
| **Reading** | | |
| `FeedReader.FetchAsync(limit)` `AckAsync([cursor])` `CloseAsync`/`DisposeAsync` `SessionEpoch` | page bound, cursor | Manual durable-Reader loop |
| `ForEachAsync` `MessagesAsync(ct)` | record handler | Pump + `IAsyncEnumerable` read loops |
| `ReaderStart` / `FirstTimeReaderStartPosition` | `Beginning` `End` `Now` cursor timestamp | First-declare start position |
| `Feed.FetchAsync` `LoadAsync` | `after_event_time_ns`, `tail`, `wait_ms` | Ad-hoc + replay reads |
| `feed.Consumer(name)` → `FeedConsumer` | `StartAt` `WithMaxPageSize` `OnRecord` `OnError` `WithBackoff` `LastError` `IsRunning` | Supervised durable consumer: `Messages`/`RunAsync`/`DisposeAsync` |
| **Subscription members** | | |
| `domain.Member(name)` / `domain.Subscription(name)` | `JoinAsync` `ClaimAsync` `RenewAsync` `ReleaseAsync` `FetchAsync` `AckAsync` `TransferAsync` | Shared progress + RF3 work leases |
| `Feed.Subscribe` `Domain.Subscribe` | reader, feed, handler | Managed pump loops (`Subscription.Failure`) |
| **StateStores** | | |
| `StateStore.Named(name)` `DefineStateStoreAsync` `DropStateStoreAsync` `StateStore(name)` `Store(name)` | spec, codecs, source Feed | Store lifecycle + typed handle |
| `PutAsync` `DeleteAsync` `ClearAsync` | key, value, named-key postings | Mutations |
| `GetAsync` `GetByAsync` `FindAsync` `FindByAsync` `GetOrDefaultAsync` `PageAsync` `IterateAsync` | key, named key, `strict`/`limit`/`after` | Row + named-key reads |
| `AddUniqueKey` `AddNonUniqueKey` `NamedKeys` `DropAsync` `Raw` `View` | key name | Spec + administration |
| **Indexes** | | |
| `QueryIndexAsync(name, predicate, limit, strict)` | `IndexPredicate.Exact/Prefix/Range` + `IndexBound` | Decoded index rows |
| **Pipes** | | |
| `DefineForwardPipeAsync` `DefineEnrichPipeAsync` `DefineWindowPipeAsync` | name, `Scoped` (string/handle/`Scoped.Absolute`), `PipeOperation` + `WindowAggregation`/`WindowTime` | Managed jobs (`TimeSpan` durations) |
| `Pipe(name)` `PipesAsync` | — | `DescribeAsync` `PauseAsync` `ResumeAsync` `DropAsync` |
| **Catalog lifecycle** | | |
| `Create*Async` `Describe*Async` `Rename*Async` `Drop*Async` `OpenWriterSessionAsync` `SeekReaderAsync` | names | Feed/Writer/Reader/Subscription/Pipe/StateStore lifecycle + client-level Domain/Role/Grant catalog |
| **Observability** | | |
| `DemandFeedsAsync` `DemandReadersAsync` `DemandSubscriptionsAsync` `DemandIndexesAsync` `DemandPipesAsync` `HotKeysAsync` `DemandHotKeysAsync` | optional feed | `DemandReport`/`DemandRow` + `HotKey` evidence |
| **Escape hatches & errors** | | |
| `WclAsync(script)` `CommandsAsync([...])` `WriterSessionClient` `ReaderSessionClient` `SubscriptionMemberClient` `Store.Raw` | script, raw commands | Byte-level wire access |
| `WhitewaterException` | `Code` | `.Refused` `.StaleSession` `.AmbiguousResult` `.Conflict` `.Unavailable` `.LeaseConflict` |
| `Whitewater.Testing.FakeRiverbed` | — | Loopback wire-contract test double |
