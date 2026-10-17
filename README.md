# whitewater-dotnet

.NET SDK for [Whitewater](https://github.com/osodevops/whitewater) — the
developer-friendly, platform-scalable streaming system written in Rust.

Targets .NET 8. Zero external dependencies: `System.Net.Http` for
transport, `System.Text.Json` for the wire codec.

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

await using var reader = await orders.ReaderAsync("audit", capacity: 64);
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

### Wire-level API

The raw `byte[]` API stays available for full control:

```csharp
// Provision a Domain, Feed, Writer, and session — either typed
// (each method builds its WCL statement for you):
await client.CreateDomainAsync("shop");
await client.CreateFeedAsync("shop.orders");
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
var session = await reader.OpenAsync(capacity: 64);
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
[docs/stores.md](docs/stores.md) for the full store guide.

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

## Contract parity

This SDK implements the versioned wire contract in
[`whitewater/sdk/spec`](https://github.com/osodevops/whitewater/tree/main/sdk/spec):
decimal-string signed 64-bit nanoseconds, base64 binary fields, opaque
cursors, UUID request ids, and the shared structured error codes.

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
