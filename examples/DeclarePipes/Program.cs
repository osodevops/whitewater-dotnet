using Whitewater;

// The typed Pipe declaration surface: declare forward, enrich, and
// window Pipes over one Subscription, then drive the lifecycle.
//
//   client.DefineForwardPipeAsync(name, subscription, outputFeed)
//   client.DefineEnrichPipeAsync(name, subscription, outputFeed, store, op, quarantineFeed)
//   client.DefineWindowPipeAsync(name, subscription, outputStore, op, emitFeed)
//   client.Pipe(name).DescribeAsync() / PauseAsync() / ResumeAsync()
//
// PipeOperation carries the processing step: Forward(), Enrich(key,
// freshness, named, onMiss), or Window(TimeSpan, key, aggregation, ...)
// with Extractor projections naming the business Key — no sliding step,
// panes, or partitions leak into the public model.

var client = new WhitewaterClient(
    "whitewater-local-development-admin-key",
    new[]
    {
        new Uri("http://localhost:7371"),
        new Uri("http://localhost:7372"),
        new Uri("http://localhost:7373"),
    });

await client.CreateDomainAsync("pipes");
await client.CreateFeedAsync("pipes.orders");
await client.CreateFeedAsync("pipes.orders.copy");
await client.CreateFeedAsync("pipes.orders.enriched");
await client.CreateFeedAsync("pipes.orders.quarantine");
await client.CreateFeedAsync("pipes.orders.hourlyemit");
await client.CreateSubscriptionAsync("audit", "pipes.orders");

// The enrichment lookup store plus the window-aggregation pane view.
// A window Pipe's output store must declare non-unique 'key' and 'pane'
// named keys so the view stays enumerable without composite row keys.
await client.StateStores.DefineAsync(
    StateStore.Named("pipes.customers").AddKey("by_id", unique: true));
await client.StateStores.DefineAsync(
    StateStore.Named("pipes.hourlyspend").AddKey("key").AddKey("pane"));

// The copy Pipe: every delivered record lands on the output Feed
// unchanged.
await client.DefineForwardPipeAsync(
    "pipes.relay", "audit", "pipes.orders.copy");

// Point-lookup enrichment: resolve the record's "customer_id" Metadata
// against pipes.customers, strictly fresh, quarantining misses.
await client.DefineEnrichPipeAsync(
    "pipes.enrich", "audit", "pipes.orders.enriched", "pipes.customers",
    PipeOperation.Enrich(
        Extractor.Metadata("customer_id"),
        EnrichFreshness.Strict,
        named: "by_id",
        onMiss: EnrichMiss.Quarantine),
    quarantineFeed: "pipes.orders.quarantine");

// Rolling hourly aggregation: sum the JSON "total" projection into
// pipes.hourlyspend panes; emit closed panes to pipes.orders.hourlyemit
// (emit requires a declared lateness bound — here maxLate).
await client.DefineWindowPipeAsync(
    "pipes.hourly", "audit", "pipes.hourlyspend",
    PipeOperation.Window(
        TimeSpan.FromHours(1),
        Extractor.RecordKey(),
        WindowAggregation.Sum(Extractor.Json("total")),
        WindowTime.Ingest,
        maxLate: TimeSpan.FromMinutes(1),
        expire: TimeSpan.FromDays(1)),
    emitFeed: "pipes.orders.hourlyemit");

// Lifecycle: describe reports the catalog row; pause halts driving
// cooperatively and resume restarts from the durable frontier.
foreach (var name in new[] { "pipes.relay", "pipes.enrich", "pipes.hourly" })
{
    var pipe = client.Pipe(name);
    var info = await pipe.DescribeAsync();
    Console.WriteLine($"{info.Name} stage={info.Stage} status={info.Status}");
    await pipe.PauseAsync();
    await pipe.ResumeAsync();
}

foreach (var pipe in await client.PipesAsync())
{
    Console.WriteLine($"{pipe.Name} stage={pipe.Stage}");
}
