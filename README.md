# whitewater-dotnet

.NET SDK for [Whitewater](https://github.com/osodevops/whitewater) — the
developer-friendly, platform-scalable streaming system written in Rust.

Targets .NET 8. Zero external dependencies: `System.Net.Http` for
transport, `System.Text.Json` for the wire codec.

A complete microservice walkthrough — provision, produce, pump-process, dead-letter, shutdown — lives in [`docs/demo-service.md`](docs/demo-service.md).

## Concepts

Whitewater exposes `domain -> feed -> key -> cursor -> subscription`.
Partitions, ranges, replicas, and physical positions are internal
implementation details — cursors are opaque tokens, never parsed.

- **Feeds** hold an immutable history of records keyed by `key`.
- **Writers** append through epoch-fenced sessions; the same
  `request_id` deduplicates retries.
- **Readers** own independent durable cursors (at-least-once delivery;
  ack to commit).
- **Subscriptions** share durable acknowledged progress across
  cooperating members with lease fencing.

## Usage

### Typed feeds and stores — codecs do the serialization

Bind a feed or store to codecs once; application code then works in its
own types — never `Encoding.UTF8`/`JsonSerializer` on every call:

```csharp
using Whitewater;

public sealed record Order(string Id, int Total);

var client = new WhitewaterClient(
    apiKey: "...",
    endpoints: new[] { new Uri("https://node1:7070"), new Uri("https://node2:7070") });

// UTF-8 keys + JSON payloads (one codec types the payload; two bind
// key and payload explicitly).
var orders = client.Feed("shop.orders", Codecs.Json<Order>());

var writer = await orders.WriterAsync("ingest");   // declares + opens session
await writer.AppendAsync("order-42", new Order("order-42", 99));
await writer.AppendEventAsync(new FeedEvent<string, Order>(
    order, "order-42", requestId: Guid.NewGuid(),
    metadata: Codecs.TextMetadata(new() { ["source"] = "api" })));

await using var reader = await orders.ReaderAsync("audit", maxPageSize: 64);
var page = await reader.FetchAsync();
foreach (var m in page.Records) Console.WriteLine($"{m.Key} {m.Value.Total}");
await reader.AckAsync();

var profiles = client.Store("accounts.employees", Codecs.Json<Profile>());
await profiles.PutAsync("user-1", profile,
    new Dictionary<string, object?> { ["passport"] = "P111" });
var row = await profiles.GetAsync("user-1");        // row.Value is Profile
await foreach (var r in profiles.IterateAsync()) Export(r);
```

Value-level lookups skip the row wrapper when freshness doesn't matter —
`Find*` returns the decoded value or null:

```csharp
var profile = await profiles.FindAsync("user-1")
    ?? await profiles.FindByAsync("passport", "P111");   // named key, first hit
var n = await counters.GetOrDefaultAsync("hits", 0);     // counters stay one line
```

```csharp
Codecs.Utf8            // UTF-8 text (JSON/XML/etc. as strings)
Codecs.Bytes           // raw byte[] identity
Codecs.Json<T>()       // System.Text.Json — web defaults (camelCase)
Codecs.For<T>()        // infers: string→UTF-8, byte[]→raw, else JSON
Codecs.Custom<T>(e,d)  // protobuf, msgpack, application framing
Codecs.TextMetadata(m) // text Metadata map -> wire bytes
```

`client.Feed(name)` binds the default `Feed<string, byte[]>`;
`feed.View(kc, vc)` and `store.View(...)` rebind codecs without a wire
call.

### Typed Domains — navigate, don't concatenate

A Domain owns every Feed it prefixes (`shop` owns `shop.orders`), so
the friendliest surface navigates domain → feed → reader/writer
instead of composing dotted names by hand:

```csharp
var shop = await client.GetDomainAsync("shop");   // verifies + materializes now
// client.Domain("shop") is the lazy form — zero I/O until needed
await shop.CreateFeedAsync("orders");         // CREATE FEED shop.orders;

await shop.FeedNamesAsync();                       // ['shop.orders', ...]
(await shop.FeedsAsync("order")).Select(f => f.Name); // prefix-filtered
await shop.ReadersAsync(); await shop.WritersAsync(); // scoped views

var orders = shop.Feed("orders", Codecs.Json<Order>());

// Everything a service needs hangs off the Domain — relative names
// resolve inside it:
var writer = await shop
    .CreateWriter("orders")
    .WriterId("orders.ingest")
    .OpenAsync();
await writer.AppendAsync("order-1", new Order("order-1", 42));

await using var reader = await shop
    .CreateReader("orders")
    .ReaderId("fulfillment")                              // durable id — the Kafka groupId analogue
    .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning) // first-declare only
    .MaxPageSize(256)
    .OpenAsync();
await reader.ForEachAsync(async m => { await handle(m.Value); return true; });
// or stream: `await foreach (var m in reader.MessagesAsync(ct))` — same
// ack-per-page semantics; or stay manual: FetchAsync → handle → AckAsync
var page = await reader.FetchAsync();
await reader.AckAsync();
```

`ReaderId` is durable application state: `ReaderStartPoint` applies
only when the Reader is first declared — an existing Reader resumes its
acknowledged frontier (the `auto.offset.reset` mapping). For
cursor/timestamp positions the `ReaderStartPoint(ReaderStart)` overload
accepts the full form. A `Feed` bound to another Domain is refused
locally, before any network call. The first catalog accessor issues ONE
batched control read (`describe domain` + every `show` listing) and
caches the snapshot — `RefreshAsync()` re-pulls.

### Record schemas

Records stay schema-neutral bytes on the wire; a Feed can still declare
schema metadata the catalog tracks for tooling, validation, and
compatibility. The Domain's schema is the default every Feed inherits;
a Feed's own declaration overrides the named sides:

```csharp
await client.SetDomainSchemaAsync("shop",
    new FeedSchema(Value: new SchemaDecl("json", Subject: "order-v1")));
await client.SetFeedSchemaAsync("shop.orders",
    new FeedSchema(Key: new SchemaDecl("text")));
await shop.RefreshAsync();

await shop.SchemaAsync();                      // the Domain default
await shop.FeedSchemaAsync("shop.orders");     // effective: key=text, value=json/order-v1
await shop.DeclaredFeedSchemaAsync("shop.orders");
await client.ClearFeedSchemaAsync("shop.orders");  // reverts to inheritance
```

`SET SCHEMA ON DOMAIN|FEED <name> (KEY|VALUE <format> [SUBJECT 'x']
[DEFINITION 'y'])+` in WCL does the same; `SHOW SCHEMAS` lists declared
assignments. Formats today: `json`, `jsonschema`, `avro`, `protobuf`, `text`, `bytes`,
or `custom.<name>` — compatibility enforcement is still a no-op.

### Wire-level API

The raw `byte[]` API stays available for full control:

```csharp
// Provision a Domain, Feed, Writer, and session — either typed
// (each method builds its WCL statement for you):
await client.CreateDomainAsync("shop");
var shopDomain = await client.GetDomainAsync("shop");
await shopDomain.CreateFeedAsync("orders");   // = CreateFeedAsync("shop.orders")
await client.CreateWriterAsync("orders-writer", "shop.orders");
var setup = await client.OpenWriterSessionAsync("orders-writer");
var sessionEpoch = setup.Results[0].Data["session_epoch"]!.GetValue<long>();
//
// …or raw WCL when a script is clearer:
//   await client.WclAsync("CREATE DOMAIN shop; CREATE FEED shop.orders; ...");

// Idempotent append — retrying with the same request id is safe.
var writer = client.Writer("orders-writer", sessionEpoch);
var appended = await writer.AppendAsync(
    key: "order-42"u8.ToArray(),
    payload: "created"u8.ToArray(),
    requestId: Guid.NewGuid(),
    eventTimeNs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L,
    metadata: new Dictionary<string, byte[]> { ["source"] = "api"u8.ToArray() });

// Independent durable reader.
var reader = client.Reader("orders-reader");
var session = await reader.OpenAsync(maxPageSize: 64);
var page = await reader.FetchAsync(session);
foreach (var record in page.Records)
{
    Process(record);
}
if (page.DeliveredCursor is { } frontier)
{
    await reader.AckAsync(session, frontier);
}
```

### Managed consume: `LoadAsync` + `Subscribe`

```csharp
// One-shot bulk read of committed history, delivered per page.
var loaded = await client.Feeds.LoadAsync("hr.employees",
    ReaderStart.Beginning(),
    batch => { Export(batch); return Task.CompletedTask; });

// ...or keep receiving: provisions the Reader, runs
// Open -> [Fetch -> onRecords -> Ack] -> Close on a background task.
await using var sub = client.Subscribe("hr-view", "hr.employees",
    ReaderStart.Now(),
    batch => { Export(batch); return Task.CompletedTask; },
    error => Console.Error.WriteLine($"subscription ended: {error}"));
```

### Permanent consumer: `feed.Consumer()` — supervision inside

For the Kafka-style "consume forever" loop, `Consumer(reader)` wraps
`Subscribe` with a supervisor — reconnects, retry backoff, session
re-open on fencing, and graceful drain are all internal. Application
code only sees decoded records, one at a time:

```csharp
var feed = client.Feed("users.update.request", Codecs.Json<UpdateRequest>());

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

### Server-side StateStores

```csharp
var spec = StateStore.Named("accounts.employees")
    .AddKey("passport", unique: true).AddKey("department");   // up to 10 named keys
var store = await client.StateStores.DefineAsync(spec);       // durable store on the Nodes

await store.PutAsync("emp-1"u8.ToArray(), payload,
    new Dictionary<string, object?> { ["passport"] = "P123", ["department"] = "eng" });
var row = await store.GetAsync("emp-1"u8.ToArray());            // StoreRow | null + freshness
var rows = await store.GetByAsync("passport", "P123");          // named key — a list
await foreach (var r in store.IterateAsync()) Export(r);
```

The store lives on the Nodes — the SDK holds no local copy. See
[docs/stores.md](docs/stores.md) for the full store guide and
[docs/pipes.md](docs/pipes.md) for managed stream jobs
(forward/enrich/window) built on subscriptions and stores.

### Error handling

```csharp
catch (WhitewaterException.StaleSession)   { /* re-open the session */ }
catch (WhitewaterException.AmbiguousResult){ /* may have committed — dedup retry */ }
catch (WhitewaterException.Unavailable)    { /* safe retry after backoff */ }
catch (WhitewaterException.LeaseConflict)  { /* lease lost or contended */ }
catch (WhitewaterException.Conflict)       { /* fencing/conflict; inspect Code */ }
catch (WhitewaterException.Refused)        { /* request rejected; fix and retry */ }
```

### Pipes, indexes, and operations

Managed Pipes and replicated Indexes share the same topology-free model
— names, never partitions or Nodes:

```csharp
// Pipe declaration: the client names the business key, source and
// outputs — the server owns panes, leases and the effect journal.
await client.DefineForwardPipeAsync("orders.relay", "orders-sub",
    "orders.relayed");
await client.DefineEnrichPipeAsync("orders.enrich", "orders-sub",
    "orders.enriched", "crm.customers",
    PipeOperation.Enrich(Extractor.RecordKey(),
        freshness: EnrichFreshness.Latest,
        onMiss: EnrichMiss.Quarantine),
    quarantineFeed: "orders.enrich-dlq");
await client.DefineWindowPipeAsync("orders.hourly", "orders-sub",
    "stats.orders-hourly",
    PipeOperation.Window(TimeSpan.FromHours(1),  // 1h panes
        Extractor.RecordKey(), WindowAggregation.Count(),
        time: WindowTime.Event,
        watermark: TimeSpan.FromMinutes(5),  // 5m horizon
        grace: TimeSpan.FromSeconds(30),     // 30s late revisions
        expire: TimeSpan.FromHours(2)),
    emitFeed: "orders.hourly-emitted");

// Pipe lifecycle: describe, pause, resume, drop.
var pipe = client.Pipe("orders.relay");
var info = await pipe.DescribeAsync();   // stage, operation, outputs, status
await pipe.PauseAsync(); await pipe.ResumeAsync(); await pipe.DropAsync();
var pipes = await client.PipesAsync();   // the declared catalog

// Index queries: exact, prefix, or range predicates over typed values.
var page = await client.QueryIndexAsync(
    "crm.byregion",
    IndexPredicate.Range(
        new object?[] { "emea" },
        IndexBound.InclusiveBound(0L), IndexBound.ExclusiveBound(99L)),
    limit: 50, strict: true);
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

## API surface

Everything the SDK exposes, grouped by capability. The typed surface
(`Domain` → `Feed` → writer/reader/consumer) covers the daily path;
`CommandsAsync`, `WclAsync`, and the `*SessionClient` classes are the
explicit escape hatches for the same wire contract.

| Area | API |
|---|---|
| Client & transport | `new WhitewaterClient(apiKey, endpoints, timeout?)` · `HealthyAsync` · `UseCompression` on feeds/writers |
| Typed Domains | `Domain(name)` lazy / `GetDomainAsync(name)` eager · `DomainIdAsync` `RevisionAsync` `DescribeAsync` `HistoryPolicyAsync` `RefreshAsync` · `FeedsAsync(prefix)` `FeedNamesAsync` `ReadersAsync` `WritersAsync` `SubscriptionsAsync` `PipeNamesAsync` `IndexesAsync` `StateStoresAsync` · `Feed(relative[, codec])` `GetFeed` · `CreateFeedAsync` `CreateReader` `CreateWriter` (relative/Feed) |
| Schemas | `SetDomainSchemaAsync` `SetFeedSchemaAsync` `ClearDomainSchemaAsync` `ClearFeedSchemaAsync` · `Domain.SchemaAsync` `FeedSchemaAsync` `DeclaredFeedSchemaAsync` · `SchemaDecl`/`FeedSchema` — `json`, `jsonschema`, `avro`, `protobuf`, `text`, `bytes`, `custom.<name>` |
| Feeds | `Feed(name)` / `Feed<TKey,TValue>` · `UseCompression` · `RecordsAsync` `FetchAsync` `LoadAsync` · `WriterAsync` `AppendAsync` `ReaderAsync` `Subscribe` `Consumer` `View<TK2,TV2>` |
| Writing | `FeedWriter.AppendAsync(k, v)` `AppendEventAsync` `AppendAllAsync` · `UseCompression` `SessionEpoch` · idempotent `request_id` · `FeedEvent` carries event time + Metadata |
| Reading | `FeedReader.ForEachAsync`/`MessagesAsync(ct)` pump · `FetchAsync(limit)` `AckAsync([cursor])` `CloseAsync`/`DisposeAsync` · `SessionEpoch` · durable `readerId` · `ReaderStart` / `FirstTimeReaderStartPosition` (`Beginning`, `End`, `Now`, cursor, timestamp) |
| Temporary fetches | `Feed.FetchAsync` / `Feeds.TemporaryFetchAsync` — `after_event_time_ns` seeks, `new_only`, `tail` + `wait_ms` long-poll · `LoadAsync` |
| Consumers | `feed.Consumer(reader)` → `FeedConsumer`: `StartAt` `WithMaxPageSize` `OnRecord` `OnError` `WithBackoff` `LastError` `Failure` `IsRunning` · `Messages` (IAsyncEnumerable) / `RunAsync` / `DisposeAsync` |
| Subscription members | `SubscriptionMember(name)` → `JoinAsync` `ClaimAsync` `RenewAsync` `ReleaseAsync` `FetchAsync` `AckAsync` — shared progress + RF3 work leases · `Feed.Subscribe` managed loop (`Subscription.Failure`) |
| StateStores | `StateStore(name)` / typed `Store<TKey,TValue>`: `PutAsync` `GetAsync` `GetByAsync` `FindAsync` `FindByAsync` `GetOrDefaultAsync` `DeleteAsync` `PageAsync` `IterateAsync` (IAsyncEnumerable) `ClearAsync` `DropAsync` `Raw` `View` · `StateStores.DefineAsync` `DropAsync` |
| Indexes | `QueryIndexAsync(name, IndexPredicate)` → `IndexPage` · `IndexPredicate.Exact/Prefix/Range` + `IndexBound.InclusiveBound/ExclusiveBound` |
| Pipes | `DefineForwardPipeAsync` `DefineEnrichPipeAsync` `DefineWindowPipeAsync` · `Pipe(name)` → `DescribeAsync` `PauseAsync` `ResumeAsync` `DropAsync` · `PipesAsync` · `PipeOperation` `WindowAggregation` `WindowTime` · durations are `TimeSpan` |
| Catalog lifecycle | `Create*Async` `Describe*Async` `Rename*Async` `Drop*Async` `List*Async` for Domain, Feed, Writer, Reader, Subscription, Role · `ListGrantsAsync` · `OpenWriterSessionAsync` `SeekReaderAsync` |
| Observability | `DemandFeedsAsync` `DemandReadersAsync` `DemandSubscriptionsAsync` `DemandIndexesAsync` `DemandPipesAsync` → `DemandReport`/`DemandRow` · `HotKeysAsync` → `HotKey` |
| Escape hatches | `WclAsync(script)` · `CommandsAsync([...])` raw command batch · `WriterSessionClient` `ReaderSessionClient` `SubscriptionMemberClient` byte-level sessions · `Feeds` raw `FeedClient` |
| Errors | `WhitewaterException` — `.Refused` `.StaleSession` `.AmbiguousResult` `.Conflict` `.Unavailable` `.LeaseConflict` · `Code` carries the wire `code` |
| Testing | `FakeRiverbed` — in-process Riverbed implementing the wire contract (dedup, epochs, at-least-once reads, stores, WCL/commands, schemas) |

## Contract parity

This SDK implements the versioned wire contract in
[`whitewater/sdk/spec`](https://github.com/osodevops/whitewater/tree/main/sdk/spec):
decimal-string signed 64-bit nanoseconds, base64 binary fields, opaque
cursors, UUID request ids, and the shared structured error codes.

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

## Development

```powershell
dotnet build Whitewater.sln
dotnet test Whitewater.sln   # unit tests; conformance skips without env vars
```
