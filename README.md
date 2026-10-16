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
