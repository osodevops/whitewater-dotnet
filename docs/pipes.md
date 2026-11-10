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
var ops = await client.GetDomainAsync("ops");
// Everything below resolves inside ops — "orders.sub" means
// ops.orders.sub. For cross-Domain endpoints, see §8.
```

## 1. Prerequisites: subscriptions and stores

Every Pipe names a **Subscription** — the durable delivery lease it pulls
input through. Declare one per Pipe:

```csharp
await ops.CreateSubscriptionAsync("orders.sub", "orders");
// → CREATE SUBSCRIPTION ops.orders.sub FROM ops.orders
// start defaults to ReaderStart.Beginning(); pass ReaderStart.Now()/Cursor()/TimestampNs()
```

`enrich` and `window` pipes need a **StateStore** (see `docs/stores.md`):

```csharp
var spec = StateStore.Named("customers")
    .AddKey("email", unique: true, extractor: Extractor.Json("$.email"))
    .AddKey("tier", extractor: Extractor.Json("$.tier"));
var store = await ops.DefineStateStoreAsync(spec);   // declares ops.customers
```

## 2. Forward pipes

```csharp
await ops.DefineForwardPipeAsync(
    "orders.relay",    // pipe name → ops.orders.relay
    "orders.sub",      // source subscription → ops.orders.sub
    "orders.relayed"); // output feed → ops.orders.relayed
```

## 3. Enrich pipes

Point-lookup enrichment: project a key with an `Extractor`, resolve it
against the store, and copy the matched row into output metadata
(`enrich.payload`, `enrich.matched`). Resolution rides inside the effect
journal — replayed outputs are byte-identical.

```csharp
await ops.DefineEnrichPipeAsync(
    "orders.enrich",
    "orders.sub",
    "enriched",
    "customers",                      // lookup store → ops.customers
    PipeOperation.Enrich(
        Extractor.Json("$.customerId"),
        freshness: EnrichFreshness.Latest,   // or EnrichFreshness.Strict
        named: "email",                      // named key; null = primary
        onMiss: EnrichMiss.Quarantine),      // Forward | Drop | Quarantine
    quarantineFeed: "enrich.misses");        // required for Quarantine
```

## 4. Window pipes

A window Pipe's output store must be declared before the Pipe — and it
must carry non-unique named keys `key` and `pane` so pane rows stay
enumerable without composite row keys:

```csharp
await ops.DefineStateStoreAsync(
    StateStore.Named("order.counts").AddKey("key").AddKey("pane"));

await ops.DefineWindowPipeAsync(
    "orders.hourly",
    "orders.sub",
    "order.counts",                   // pane store (declared above)
    PipeOperation.Window(
        window: TimeSpan.FromHours(1),   // 1h panes
        key: Extractor.Json("$.region"),
        aggregation: WindowAggregation.Sum(Extractor.Json("$.total")),
        time: WindowTime.Event,          // or WindowTime.Ingest
        watermark: TimeSpan.FromMinutes(5), // event-time close horizon
        grace: TimeSpan.FromMinutes(1),  // revise emitted panes up to 1min late
        expire: TimeSpan.FromDays(1),    // keep panes 24h
        keyLimit: 10_000L),
    emitFeed: "hourly.emits");         // optional closed-pane output
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
var pipe = ops.Pipe("orders.relay");

var info = await pipe.DescribeAsync(); // stage, operation, outputs, status
await pipe.PauseAsync();               // cooperative; the frontier stays put
await pipe.ResumeAsync();              // restarts from the durable frontier
await pipe.DropAsync();

var all = await ops.PipesAsync();
```

`Pause` is durable and idempotent — nothing is dropped while paused.
`Describe` returns `stage` (`declared`/`paused`), the opaque `Operation`
node, and output Feed/Store IDs.

## 6. Pipes that cross Domains

A Pipe is always declared inside one Domain — but its Subscription
source, output Feed, lookup Store, and quarantine Feed may live in
**other** Domains. Plain string arguments resolve inside the Pipe's
owning Domain; a cross-Domain endpoint is passed as a **handle** — a
`Feed`/`Store`/`StateStore`/`SubscriptionRef` from the other Domain,
or `Scoped.Absolute("name")`. Handles carry their absolute name, so a
cross-Domain reference is explicit in the code, never guessed from a
dotted string:

```csharp
var purchasing = await client.GetDomainAsync("purchasing");
var warehouse = await client.GetDomainAsync("warehouse");

// purchasing declares the Pipe; its source is in-domain ("fulfil" →
// purchasing.fulfil) while the output feed lives in warehouse.
await purchasing.DefineForwardPipeAsync(
    "relay", "fulfil", warehouse.Feed("inbound"));
// → DEFINE PIPE purchasing.relay FROM purchasing.fulfil
//     TO FEED warehouse.inbound

// Or the pull shape — warehouse owns the Pipe and draws from
// purchasing's Subscription:
await warehouse.DefineForwardPipeAsync(
    "pull", purchasing.Subscription("fulfil"), "inbound");
// → DEFINE PIPE warehouse.pull FROM purchasing.fulfil
//     TO FEED warehouse.inbound

// Scoped.Absolute is the handle-free explicit form:
await purchasing.DefineEnrichPipeAsync(
    "enrich", "fulfil", "orders.enriched",
    Scoped.Absolute("crm.customers"),
    PipeOperation.Enrich(Extractor.RecordKey(), onMiss: EnrichMiss.Drop),
    quarantineFeed: null);
```

## 7. Watching demand

```csharp
var demand = await client.DemandPipesAsync();
foreach (var row in demand.Rows) Console.WriteLine($"{row.Name} {row.Stage}");
```

## 8. Failure semantics

- Declarations commit atomically inside a `CommandsAsync` batch — submit
  subscription + `define_pipe` in one batch to apply both or neither.
- Pipe delivery is at-least-once; effects journal lookups/accumulation so
  replayed outputs are byte-identical.
