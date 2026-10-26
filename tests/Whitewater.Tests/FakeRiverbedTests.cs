using System.Text;
using Whitewater;
using Whitewater.Testing;
using Xunit;

namespace Whitewater.Tests;

/// <summary>
/// The shipped test double exercised end-to-end through the public
/// client surface — real loopback HTTP, real wire semantics.
/// </summary>
public class FakeRiverbedTests
{
    private static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);

    [Fact]
    public async Task ReportsHealthyAndAnswersTheCompatSurface()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);

        Assert.True(await client.HealthyAsync());

        // Unknown wire paths still decode into the typed error surface.
        var refused = await Assert.ThrowsAsync<WhitewaterException.Refused>(
            () => client.QueryIndexAsync("accounts.unknown", IndexPredicate.Exact(1)));
        Assert.Equal(404, refused.Status);
    }

    [Fact]
    public async Task AppendFetchAckCommitsProgressWithoutRedelivery()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);
        var feed = client.Feed("orders.events");

        var writer = await feed.WriterAsync("ingest");
        await writer.AppendAsync("order-1", "alpha");
        await writer.AppendAsync("order-2", "beta");

        var reader = await feed.ReaderAsync("audit");
        var page = await reader.FetchAsync();
        Assert.Equal(
            new[] { "alpha", "beta" },
            page.Records.Select(m => m.Value));
        Assert.Equal(new[] { "order-1", "order-2" }, page.Records.Select(m => m.Key));
        await reader.AckAsync();
        await reader.CloseAsync();

        // A reopened session resumes past the acknowledged frontier.
        await using var resumed = await feed.ReaderAsync("audit");
        var empty = await resumed.FetchAsync();
        Assert.Empty(empty.Records);

        // Temporary fetches see committed history with a continuation.
        var temporary = await feed.FetchAsync(new TemporaryFetchOptions(Limit: 1));
        var first = Assert.Single(temporary.Records);
        Assert.Equal("alpha", first.Value);
        Assert.NotNull(temporary.NextCursor);
        var rest = await feed.FetchAsync(
            new TemporaryFetchOptions(After: temporary.NextCursor));
        Assert.Equal("beta", Assert.Single(rest.Records).Value);
    }

    [Fact]
    public async Task UnackedCloseRedeliversThePage()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);
        var feed = client.Feed("orders.events");

        var writer = await feed.WriterAsync("ingest");
        await writer.AppendAsync("order-1", "alpha");
        await writer.AppendAsync("order-2", "beta");

        await using var first = await feed.ReaderAsync("audit");
        var page = await first.FetchAsync();
        Assert.Equal(2, page.Records.Count);
        await first.CloseAsync();              // page never acknowledged

        // The replayed page carries the same records — at-least-once.
        await using var second = await feed.ReaderAsync("audit");
        var replayed = await second.FetchAsync();
        Assert.Equal(
            new[] { "alpha", "beta" },
            replayed.Records.Select(m => m.Value));
        await second.AckAsync();
        await second.CloseAsync();

        await using var third = await feed.ReaderAsync("audit");
        Assert.Empty((await third.FetchAsync()).Records);
    }

    [Fact]
    public async Task ConsumerDeliversAcksAndClosesThroughThePublicSurface()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);
        var feed = client.Feed("orders.events");
        var writer = await feed.WriterAsync("ingest");
        await writer.AppendAsync("order-1", "alpha");

        var seen = new List<string>();
        await using (var consumer = feed.Consumer("enricher")
            .WithBackoff(TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(50)))
        {
            await foreach (var message in consumer.Messages())
            {
                seen.Add(message.Value!);
                break;                      // drain this page, then stop
            }
        }

        Assert.Equal(new[] { "alpha" }, seen);
        Assert.Contains(riverbed.Requests, r => r.Path == "/v1/readers/ack");
        Assert.Contains(riverbed.Requests, r => r.Path == "/v1/readers/close");
    }

    [Fact]
    public async Task RequestIdRetriesDedupeToTheCommittedReceipt()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);
        var feed = client.Feed("orders.events");
        var writer = await feed.WriterAsync("ingest");

        var requestId = Guid.NewGuid();
        var first = await writer.AppendEventAsync(
            new FeedEvent<string, string>("alpha", "order-1", requestId));
        var retry = await writer.AppendEventAsync(
            new FeedEvent<string, string>("alpha", "order-1", requestId));

        Assert.False(first.Deduplicated);
        Assert.True(retry.Deduplicated);
        Assert.Equal(first.MessageId, retry.MessageId);
        Assert.Equal(first.Cursor, retry.Cursor);
        Assert.Single(await feed.RecordsAsync(10));
    }

    [Fact]
    public async Task SupersededWriterEpochIsFencedAsStaleSession()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);
        var feed = client.Feed("orders.events");

        var writer = await feed.WriterAsync("ingest");      // session epoch 1
        await client.OpenWriterSessionAsync("ingest");       // epoch 2 supersedes

        var stale = await Assert.ThrowsAsync<WhitewaterException.StaleSession>(
            () => writer.AppendAsync("order-1", "alpha"));
        Assert.Equal("stale_epoch", stale.Code);

        // A session bound at the current epoch still appends.
        var current = await feed.WriterAsync("ingest");
        await current.AppendAsync("order-1", "alpha");
        Assert.Single(await feed.RecordsAsync(10));
    }

    [Fact]
    public async Task StateStoreRoundTripsRowsAndNamedKeys()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);

        var spec = StateStore.Named("accounts.employees").AddKey("department");
        var store = await client.StateStores.DefineAsync(spec);

        var mutationId = Guid.NewGuid();
        var put = await store.PutAsync(Utf8("u1"), Utf8("alice"),
            new Dictionary<string, object?> { ["department"] = "eng" }, mutationId);
        var replay = await store.PutAsync(Utf8("u1"), Utf8("alice"),
            new Dictionary<string, object?> { ["department"] = "eng" }, mutationId);
        Assert.False(put.Deduplicated);
        Assert.True(replay.Deduplicated);              // mutation_id dedups
        Assert.Equal(put.Sequence, replay.Sequence);
        await store.PutAsync(Utf8("u2"), Utf8("bob"),
            new Dictionary<string, object?> { ["department"] = "eng" });
        await store.PutAsync(Utf8("u3"), Utf8("cara"),
            new Dictionary<string, object?> { ["department"] = "ops" });

        var row = await store.GetAsync(Utf8("u1"));
        Assert.NotNull(row);
        Assert.Equal("alice", Encoding.UTF8.GetString(row.Payload));
        Assert.Null(await store.GetAsync(Utf8("ghost")));

        var eng = await store.GetByAsync("department", "eng");
        Assert.Equal(2, eng.Rows.Count);
        Assert.False(eng.Freshness.Behind);

        var keys = new List<string>();
        await foreach (var entry in store.IterateAsync(limit: 2))
        {
            keys.Add(Encoding.UTF8.GetString(entry.Key));
        }
        Assert.Equal(new[] { "u1", "u2", "u3" }, keys);   // primary-key order

        await store.DeleteAsync(Utf8("u2"));
        Assert.Null(await store.GetAsync(Utf8("u2")));
        Assert.Single((await store.GetByAsync("department", "eng")).Rows);

        await store.ClearAsync();
        Assert.Empty((await store.PageAsync()).Rows);
    }
}
