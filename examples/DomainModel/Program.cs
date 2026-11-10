using Whitewater;

// The strongly-typed Domain facade — the only front door. A Domain owns
// every Feed it prefixes (shop owns shop.orders), so names compose
// inside it instead of being scattered dotted strings:
//
//   var shop = await client.CreateDomainAsync("shop");  // declares + returns the handle
//   var orders = shop.Feed("orders");                   // typed Feed, zero I/O
//   var reader = await shop.CreateReader("orders")
//       .ReaderId("fulfillment")                       // resolves shop.fulfillment
//       .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning)
//       .OpenAsync();
//
// client.GetDomainAsync("shop") verifies + materializes an existing
// Domain's catalog view; client.Domain("shop") is the lazy handle —
// no I/O until a catalog accessor runs.
//
// Run a Riverbed (scripts/test-*.py bring one up on 7371–7373), then:
//
//   dotnet run --project examples/DomainModel

var client = new WhitewaterClient(
    "whitewater-local-development-admin-key",
    new[]
    {
        new Uri("http://localhost:7371"),
        new Uri("http://localhost:7372"),
        new Uri("http://localhost:7373"),
    });

var shop = await client.CreateDomainAsync("shop");
await shop.CreateFeedAsync("orders");
await shop.CreateFeedAsync("returns");

// GetDomainAsync() verifies + materializes now; the lazy handle would
// pull the same view on first catalog access.
await shop.RefreshAsync();

Console.WriteLine("feeds: " + string.Join(", ", await shop.FeedNamesAsync()));
Console.WriteLine(
    "order-prefix feeds: "
        + string.Join(", ", (await shop.FeedsAsync("order")).Select(f => f.Name)));

var orders = shop.Feed("orders"); // Feed<string,string> — UTF-8 by default

var writer = await shop
    .CreateWriter("orders")
    .WriterId("orders.ingest")   // deeper relative names compose too
    .OpenAsync();
await writer.AppendAsync("order-1", "{\"total\":42}");

await using var reader = await shop
    .CreateReader("orders")
    .ReaderId("fulfillment") // durable id — the Kafka groupId analogue
    .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning) // first-declare only
    .MaxPageSize(256)
    .OpenAsync();

var page = await reader.FetchAsync();
foreach (var message in page.Records)
{
    Console.WriteLine($"{message.Key} -> {message.Value}");
}
await reader.AckAsync();
