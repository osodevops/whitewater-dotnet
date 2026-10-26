# Demo: order-fulfillment service

A complete microservice shape: provision a Domain + Feed, append
orders, and pump them through a processor with at-least-once
semantics, a dead-letter path, and graceful shutdown.

The contract in one line: **return = processed, throw = replay** — the
SDK acks a page only after every record in it has been handled.

```csharp
using System.Text;
using Whitewater;

var client = new WhitewaterClient(
    apiKey: Environment.GetEnvironmentVariable("WHITEWATER_API_KEY")!,
    endpoints: new[] { new Uri("http://localhost:7371"), new Uri("http://localhost:7372") });

// ---- provision ------------------------------------------------------
await client.CreateDomainAsync("shop");
var shop = await client.GetDomainAsync("shop");
await shop.CreateFeedAsync("orders");
await shop.CreateFeedAsync("orders.dlq");       // dead-letter feed

// ---- produce (usually a different service) ---------------------------
// The default Feed binds UTF-8 strings — the SDK serializes, never
// GetBytes; plain JSON flows with no codec binding.
var orders = shop.Feed("orders");
var writer = await shop.CreateWriter(orders).WriterId("orders.ingest").OpenAsync();
await writer.AppendAsync("order-1", "{\"total\":42}");

// ---- consume ----------------------------------------------------------
var dlq = shop.Feed("orders.dlq");
var deadLetter = await shop.CreateWriter(dlq)
    .WriterId("fulfillment.dlq").OpenAsync();
await using var reader = await shop.CreateReader(orders)
    .ReaderId("fulfillment")                            // durable id — survives restarts
    .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning) // first-declare only
    .MaxPageSize(250)                                   // max records per fetch page
    .OpenAsync();

var stopping = new CancellationTokenSource();

// The async runtime owns the pump — the Task is your failure surface.
var pump = reader.ForEachAsync(async m =>
{
    try
    {
        await ordersDb.UpsertAsync(orderId(m), order(m));   // idempotent side effect
        return true;
    }
    catch (Exception e) when (Permanent(e))
    {
        await deadLetter.AppendAsync(m.Key ?? "", m.Value); // quarantine → continue → acked
        return true;
    }
    // transient errors rethrow → pump stops → page replays
}, stopping.Token);

_ = pump.ContinueWith(t =>
{
    // A faulted pump surfaces here — restart or alert.
    if (t.IsFaulted) Console.Error.WriteLine($"reader pump died: {t.Exception}");
}, TaskContinuationOptions.OnlyOnFaulted);

// ---- shutdown ----------------------------------------------------------
// Cancelling the token ends the pump; the in-flight page stays unacked
// and replays on the next open. CloseAsync releases the session fence.
Console.CancelKeyPress += (_, e) => { e.Cancel = true; stopping.Cancel(); };
try { await pump; } catch (OperationCanceledException) { /* clean stop */ }
await reader.CloseAsync();
```

## Notes

- `ReaderId` is durable application state — the Kafka `groupId`
  analogue. `ReaderStartPoint` applies only on first declare; restarts
  resume the acknowledged frontier.
- Acks are per fetched page. A crash mid-page replays the whole page —
  that's what makes it at-least-once. Process idempotently.
- `MaxPageSize(250)` bounds both the page and the replay window.
- `await foreach (var m in reader.MessagesAsync(ct))` iterates with
  the same ack-per-page semantics. Manual mode: `FetchAsync` →
  process → `AckAsync`. Never ack before processing — that's
  at-most-once.
