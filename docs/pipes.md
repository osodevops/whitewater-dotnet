# Pipes

A **Pipe** is a managed stream job: it drives one declared **Subscription**
and applies an operation to every record the Subscription delivers, writing
results to a Feed or a StateStore. Pipes run server-side — your process
does not need to stay alive for a Pipe to keep going.

| Operation | Reads from | Writes to | Purpose |
|---|---|---|---|
| `forward` | Subscription | Feed | copy records verbatim |
| `enrich` | Subscription + StateStore | Feed (+ optional quarantine Feed) | point-lookup enrichment |
| `window` | Subscription | StateStore (+ optional emit Feed) | fixed-window aggregation |

```csharp
var client = new WhitewaterClient("localhost:7371", apiKey);
```

## 1. Prerequisites: subscriptions and stores

Every Pipe names a **Subscription** — the durable delivery lease it pulls
input through. Declare one per Pipe:

```csharp
await client.CreateSubscriptionAsync("orders.sub", "ops.orders");
// start defaults to ReaderStart.Beginning(); pass ReaderStart.Now()/Cursor()/TimestampNs()
```

`enrich` and `window` pipes need a **StateStore** (see `docs/stores.md`):

```csharp
var spec = StateStore.Named("ops.customers")
    .AddKey("email", unique: true, extractor: Extractor.Json("$.email"))
    .AddKey("tier", extractor: Extractor.Json("$.tier"));
var store = await client.StateStores.DefineAsync(spec, "ops.customer.changes");
```

## 2. Forward pipes

```csharp
await client.DefineForwardPipeAsync(
    "ops.orders.relay",   // pipe name (dotted)
    "orders.sub",         // source subscription
    "ops.orders.relayed"); // output feed
```

## 3. Enrich pipes

Point-lookup enrichment: project a key with an `Extractor`, resolve it
against the store, and copy the matched row into output metadata
(`enrich.payload`, `enrich.matched`). Resolution rides inside the effect
journal — replayed outputs are byte-identical.

```csharp
await client.DefineEnrichPipeAsync(
    "ops.orders.enrich",
    "orders.sub",
    "ops.enriched",
    "ops.customers",                  // lookup store
    PipeOperation.Enrich(
        Extractor.Json("$.customerId"),
        freshness: EnrichFreshness.Latest,   // or EnrichFreshness.Strict
        named: "email",                      // named key; null = primary
        onMiss: EnrichMiss.Quarantine),      // Forward | Drop | Quarantine
    quarantineFeed: "ops.enrich.misses");    // required for Quarantine
```

## 4. Window pipes

A window Pipe's output store must be declared before the Pipe — and it
must carry non-unique named keys `key` and `pane` so pane rows stay
enumerable without composite row keys:

```csharp
await client.StateStores.DefineAsync(
    StateStore.Named("ops.order.counts").AddKey("key").AddKey("pane"));

await client.DefineWindowPipeAsync(
    "ops.orders.hourly",
    "orders.sub",
    "ops.order.counts",               // pane store (declared above)
    PipeOperation.Window(
        window: TimeSpan.FromHours(1),   // 1h panes
        key: Extractor.Json("$.region"),
        aggregation: WindowAggregation.Sum(Extractor.Json("$.total")),
        time: WindowTime.Event,          // or WindowTime.Ingest
        watermark: TimeSpan.FromMinutes(5), // event-time close horizon
        grace: TimeSpan.FromMinutes(1),  // revise emitted panes up to 1min late
        expire: TimeSpan.FromDays(1),    // keep panes 24h
        keyLimit: 10_000L),
    emitFeed: "ops.hourly.emits");     // optional closed-pane output
```

Every window duration is a `TimeSpan` — the SDK extracts nanoseconds
internally for the wire; callers never spell raw nanosecond counts.

### Lateness models — pick exactly one

- `maxLate` — ingest-clock bound; later arrivals quarantine into a
  countable `late` row.
- `watermark` — event-time horizon `max(event_time) − watermark`
  closes a pane once it passes the pane end (refused under
  `WindowTime.Ingest`); `grace` lets late events revise an emitted pane
  before counting into `late`.

An emit feed requires a declared lateness model.

### Aggregations

```csharp
WindowAggregation.Count()
WindowAggregation.Sum(Extractor.Json("$.total"))
WindowAggregation.Average(Extractor.Json("$.rtt"))
```

## 5. Lifecycle

```csharp
var pipe = client.Pipe("ops.orders.relay");

var info = await pipe.DescribeAsync(); // stage, operation, outputs, status
await pipe.PauseAsync();               // cooperative; the frontier stays put
await pipe.ResumeAsync();              // restarts from the durable frontier
await pipe.DropAsync();

var all = await client.PipesAsync();
```

`Pause` is durable and idempotent — nothing is dropped while paused.
`Describe` returns `stage` (`declared`/`paused`), the opaque `Operation`
node, and output Feed/Store IDs.

## 6. Watching demand

```csharp
var demand = await client.DemandPipesAsync();
foreach (var row in demand.Rows) Console.WriteLine($"{row.Name} {row.Stage}");
```

## 7. Failure semantics

- Declarations commit atomically inside a `CommandsAsync` batch — submit
  subscription + `define_pipe` in one batch to apply both or neither.
- Pipe delivery is at-least-once; effects journal lookups/accumulation so
  replayed outputs are byte-identical.
