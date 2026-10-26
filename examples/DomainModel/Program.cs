using Whitewater;

// The strongly-typed Domain facade — navigate the namespace instead of
// composing dotted names by hand.
//
//   var shop = await client.GetDomainAsync("shop");  // materialized view
//   var orders = shop.Feed("orders");                // typed Feed, zero I/O
//   var reader = await client.CreateReader(shop, orders)
//       .ReaderId("fulfillment")
//       .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning)
//       .OpenAsync();
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

await client.CreateDomainAsync("shop");
await client.CreateFeedAsync("shop.orders");
await client.CreateFeedAsync("shop.returns");

// GetDomainAsync() verifies + materializes now; Domain() is the lazy handle.
var shop = await client.GetDomainAsync("shop");

Console.WriteLine("feeds: " + string.Join(", ", await shop.FeedNamesAsync()));
Console.WriteLine(
    "order-prefix feeds: "
        + string.Join(", ", (await shop.FeedsAsync("order")).Select(f => f.Name)));

var orders = shop.Feed("orders", Codecs.Utf8); // Feed<string,string> — codec-bound

var writer = await client
    .CreateWriter(shop, orders)
    .WriterId("orders.ingest")
    .OpenAsync();
await writer.AppendAsync("order-1", "{\"total\":42}");

await using var reader = await client
    .CreateReader(shop, orders)
    .ReaderId("fulfillment") // durable id — the Kafka groupId analogue
    .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning) // first-declare only
    .MaxPageSize(256)
    .OpenAsync();
var page = await reader.FetchAsync();
foreach (var record in page.Records)
{
    Console.WriteLine($"read {record.Key} {System.Text.Encoding.UTF8.GetString(record.Value)}");
}
await reader.AckAsync(); // durable progress

await shop.RefreshAsync(); // re-pull the catalog snapshot
Console.WriteLine("readers: " + string.Join(", ", await shop.ReadersAsync()));
Console.WriteLine("writers: " + string.Join(", ", await shop.WritersAsync()));
