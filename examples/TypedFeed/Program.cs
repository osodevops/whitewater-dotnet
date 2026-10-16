using Whitewater;

// The codec-bound Feed<TKey,TValue> flow: bind a feed to key/payload
// codecs once, then AppendAsync(key, value) encodes and every
// FetchAsync/RecordsAsync decodes — application code never touches
// bytes.
//
//   orders.WriterAsync("ingest")    — declares the Writer + opens a session
//   orders.ReaderAsync("audit", 64) — declares the Reader + opens a session
//   writer.AppendAsync / reader.FetchAsync / reader.AckAsync

var client = new WhitewaterClient(
    "whitewater-local-development-admin-key",
    new[]
    {
        new Uri("http://localhost:7371"),
        new Uri("http://localhost:7372"),
        new Uri("http://localhost:7373"),
    });

await client.CreateDomainAsync("demo");
await client.CreateFeedAsync("demo.orders");

// UTF-8 keys + JSON payloads — the payload codec is the one argument.
var orders = client.Feed("demo.orders", Codecs.Json<Order>());

// Declares "ingest" when missing and opens its epoch-fenced session.
var writer = await orders.WriterAsync("ingest");
await writer.AppendAsync("order-1", new Order("order-1", 42));
await writer.AppendAsync("order-2", new Order("order-2", 7));

// Declares "audit" at BEGINNING when missing and opens a session;
// FetchAsync returns decoded messages — Order, not byte[].
await using var reader = await orders.ReaderAsync("audit", 64);
while (true)
{
    var page = await reader.FetchAsync();
    foreach (var m in page.Records)
    {
        Console.WriteLine($"{m.Cursor.Value} seq={m.Sequence} {m.Key} total={m.Value.Total}");
    }
    if (page.Records.Count == 0)
    {
        break;
    }
    await reader.AckAsync();
}

public sealed record Order(string Id, int Total);
