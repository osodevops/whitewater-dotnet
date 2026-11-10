using System.Text;
using Whitewater;

// Durable consumption: a named Reader owns an independent cursor on the
// Riverbed, so progress survives client restarts — the "subscribe"
// pattern for applications, versus the ephemeral tail in TailFeed.
//
// For a managed loop — auto-provisioning, retry policy, and an
// IAsyncDisposable handle — use feed.ConsumeAsync(...) instead of this
// explicit form.
//
// The loop below is explicit — Whitewater Readers pull rather than push:
//
//   Open -> [ Fetch -> process -> Ack ] -> Close
//
// Ack is the commit point: acknowledged progress is durable, so a
// restarted client resumes after the last acked position. Process
// records BEFORE acking them — ack-then-crash loses those records.
//
// Everything hangs off the Domain: feeds, readers, writers,
// subscriptions, pipes, stores. Names compose inside the Domain —
// demo.Feed("tail") binds demo.tail; demo.Reader("reader") binds
// demo.reader.

var client = new WhitewaterClient(
    "whitewater-local-development-admin-key",
    new[]
    {
        new Uri("http://localhost:7371"),
        new Uri("http://localhost:7372"),
        new Uri("http://localhost:7373"),
    });

// One-time provisioning: CreateDomainAsync returns the Domain handle;
// feed and Reader declarations are durable Control Plane state, not
// per-session objects.
var demo = await client.CreateDomainAsync("demo");
await demo.CreateFeedAsync("tail");
await demo.CreateReader("tail")
    .ReaderId("reader")
    .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning)
    .DeclareAsync();

var reader = demo.Reader("reader"); // resolves demo.reader

// OpenAsync fences this client as the session owner. Another client
// opening the same Reader supersedes this session (stale_epoch on the
// next FetchAsync).
var session = await reader.OpenAsync(64);

try
{
    while (true)
    {
        // FetchAsync returns immediately with whatever is committed — a
        // Reader is not a long-poll. For "sit and wait" behavior, use
        // TemporaryFetchAsync(Tail, WaitMs) or pace empty fetches.
        var page = await reader.FetchAsync(session, 50);

        foreach (var record in page.Records)
        {
            Console.WriteLine($"{Encoding.UTF8.GetString(record.Payload)} @ {record.IngestTimeNs}");
        }

        // Commit durable progress AFTER processing the page. The page's
        // DeliveredCursor covers every range the fetch drew from — ack
        // it, not the last record's cursor.
        if (page.DeliveredCursor is { } delivered)
        {
            session = await reader.AckAsync(session, delivered);
        }

        if (page.Records.Count == 0)
        {
            await Task.Delay(500); // poll cadence for live reads
        }
    }
}
finally
{
    await reader.CloseAsync(session); // release the session fence
}
