using System.Text;
using Whitewater;

// Ephemeral consumption: an anonymous temporary Reader with server-side
// tail + wait — the "subscribe" pattern for live views, UIs, and REPLs.
// Nothing is provisioned; the reader vanishes when the process exits.
//
// The loop:
//
//   TemporaryFetchAsync(Tail, WaitMs) -> records -> NextCursor -> repeat
//
// WaitMs makes each call a long-poll: the Node holds the request until
// records commit or the wait expires, then answers — so the loop does
// NOT burn a request per poll interval; it sits inside the fetch.

var client = new WhitewaterClient(
    "whitewater-local-development-admin-key",
    new[]
    {
        new Uri("http://localhost:7371"),
        new Uri("http://localhost:7372"),
        new Uri("http://localhost:7373"),
    });

await client.CreateDomainAsync("demo");
await client.CreateFeedAsync("demo.tail");

Cursor? after = null; // null -> feed tail position

while (true)
{
    // Blocks up to WaitMs server-side; returns early when records land.
    var page = await client.Feeds.TemporaryFetchAsync("demo.tail",
        TemporaryFetchOptions.Tailed(after, 50, 2500));

    foreach (var record in page.Records)
    {
        Console.WriteLine($"{Encoding.UTF8.GetString(record.Payload)} @ {record.IngestTimeNs}");
    }

    // Continue from the PAGE frontier, not the last record's cursor —
    // the page can be filtered or empty while the frontier advanced.
    after = page.NextCursor;
}
