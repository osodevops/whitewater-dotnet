using Whitewater;

// The typed Pipe declaration surface: declare forward, enrich, and
// window Pipes over one Subscription, then drive the lifecycle.
//
//   domain.DefineForwardPipeAsync(name, subscription, outputFeed)
//   domain.DefineEnrichPipeAsync(name, subscription, outputFeed, store, op, quarantineFeed)
//   domain.DefineWindowPipeAsync(name, subscription, outputStore, op, emitFeed)
//   domain.Pipe(name).DescribeAsync() / PauseAsync() / ResumeAsync()
//
// PipeOperation carries the processing step: Forward(), Enrich(key,
// freshness, named, onMiss), or Window(TimeSpan, key, aggregation, ...)
// with Extractor projections naming the business Key — no sliding step,
// panes, or partitions leak into the public model.
//
// Every name resolves inside the Domain — "audit" means pipes.audit,
// "orders.copy" means pipes.orders.copy. For a Pipe whose input or
// output crosses Domains, see the CrossDomain example.

var client = new WhitewaterClient(
    "whitewater-local-development-admin-key",
    new[]
    {
        new Uri("http://localhost:7371"),
        new Uri("http://localhost:7372"),
        new Uri("http://localhost:7373"),
    });

var pipes = await client.CreateDomainAsync("pipes");
await pipes.CreateFeedAsync("orders");
await pipes.CreateFeedAsync("orders.copy");
await pipes.CreateFeedAsync("orders.enriched");
await pipes.CreateFeedAsync("orders.quarantine");
await pipes.CreateFeedAsync("orders.hourlyemit");
await pipes.CreateSubscriptionAsync("audit", "orders");

// The enrichment lookup store plus the window-aggregation pane view.
// A window Pipe's output store must declare non-unique 'key' and 'pane'
// named keys so the view stays enumerable without composite row keys.
await pipes.DefineStateStoreAsync(
    StateStore.Named("customers").AddUniqueKey("by_id"));
await pipes.DefineStateStoreAsync(
    StateStore.Named("hourlyspend").AddNonUniqueKey("key").AddNonUniqueKey("pane"));

// The copy Pipe: every delivered record lands on the output Feed
// unchanged.
await pipes.DefineForwardPipeAsync("relay", "audit", "orders.copy");

// Point-lookup enrichment: resolve the record's "customer_id" Metadata
// against pipes.customers, strictly fresh, quarantining misses.
await pipes.DefineEnrichPipeAsync(
    "enrich", "audit", "orders.enriched", "customers",
    PipeOperation.Enrich(
        Extractor.Metadata("customer_id"),
        EnrichFreshness.Strict,
        named: "by_id",
        onMiss: EnrichMiss.Quarantine),
    quarantineFeed: "orders.quarantine");

// Rolling hourly aggregation: sum the JSON "total" projection into
// pipes.hourlyspend panes; emit closed panes to pipes.orders.hourlyemit
// (emit requires a declared lateness bound — here maxLate).
await pipes.DefineWindowPipeAsync(
    "hourly", "audit", "hourlyspend",
    PipeOperation.Window(
        TimeSpan.FromHours(1),
        Extractor.RecordKey(),
        WindowAggregation.Sum(Extractor.Json("total")),
        WindowTime.Ingest,
        maxLate: TimeSpan.FromMinutes(1),
        expire: TimeSpan.FromDays(1)),
    emitFeed: "orders.hourlyemit");

// Lifecycle: describe reports the catalog row; pause halts driving
// cooperatively and resume restarts from the durable frontier.
foreach (var name in new[] { "relay", "enrich", "hourly" })
{
    var pipe = pipes.Pipe(name);
    var info = await pipe.DescribeAsync();
    Console.WriteLine($"{info.Name} stage={info.Stage} status={info.Status}");
    await pipe.PauseAsync();
    await pipe.ResumeAsync();
}

foreach (var pipe in await pipes.PipesAsync())
{
    Console.WriteLine($"{pipe.Name} stage={pipe.Stage}");
}
