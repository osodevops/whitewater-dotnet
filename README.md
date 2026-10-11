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

```csharp
using Whitewater;

var client = new WhitewaterClient(
    apiKey: "...",
    endpoints: new[] { new Uri("https://node1:7070"), new Uri("https://node2:7070") });

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
var store = RecordStore.Named("employee-store")
    .AddKey("passport").AddKey("department");              // up to 10 named keys

// One-shot bulk read of committed history, projected per row.
await client.LoadStore(store).FromFeedAsync("hr.employees",
    ReaderStart.Beginning(),
    (s, record) => s.Upsert(record)
        .WithKey(record.Key)
        .WithNamedKey("passport", record.Metadata["pp"]));

var rec = store.Get("emp-1");                    // by primary key
var rows = store.Get("passport", "P123");        // by named key — non-unique, returns a list
store.ForEachParallel(3, entry => Export(entry.Record));

// ...then keep it warm: provisions the Reader, runs
// Open -> [Fetch -> onRecords -> Ack] -> Close on a background task.
await using var sub = client.Subscribe("hr-view", "hr.employees",
    ReaderStart.Now(),
    batch => { store.PutAll(batch); return Task.CompletedTask; },
    error => Console.Error.WriteLine($"subscription ended: {error}"));
```

`client.Feeds.LoadAsync` remains the lighter one-shot read when you
don't need named-key projections. See [docs/stores.md](docs/stores.md)
for the full store guide.

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
