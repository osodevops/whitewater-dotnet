using System.Text;
using Xunit;

namespace Whitewater.Tests;

/// <summary>
/// Live store smoke against a running Riverbed — create Domain + Feed,
/// append keyed records with metadata, then <c>LoadStore</c> a projected
/// history read and named-key lookups.
///
/// Runs only when <c>WHITEWATER_ENDPOINTS</c> is set (comma-separated),
/// e.g. <c>http://localhost:7371,http://localhost:7372,http://localhost:7373</c>;
/// <c>WHITEWATER_API_KEY</c> defaults to the documented development key.
/// </summary>
public class LiveStoreTests
{
    private static readonly string? Endpoints =
        Environment.GetEnvironmentVariable("WHITEWATER_ENDPOINTS");

    [Fact]
    public async Task CreateWriteLoadAndQueryAStore()
    {
        if (Endpoints is null)
        {
            return; // env-gated: no Riverbed configured
        }

        var client = new WhitewaterClient(
            Environment.GetEnvironmentVariable("WHITEWATER_API_KEY")
                ?? "whitewater-local-development-admin-key",
            Endpoints.Split(',').Select(raw => new Uri(raw.Trim())).ToArray());

        var domain = $"smoke.net{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds():x}";
        var feed = $"{domain}.events";
        Assert.True(await client.HealthyAsync());
        await client.CreateDomainAsync(domain);
        await client.CreateFeedAsync(feed);

        var session = Guid.NewGuid();
        var rows = new (string Key, string Payload, string Department)[]
        {
            ("emp-1", "alice", "eng"),
            ("emp-2", "bob", "eng"),
            ("emp-3", "cara", "ops"),
        };
        for (var i = 0; i < rows.Length; i++)
        {
            await client.Feeds.AppendAsync(feed, new FeedAppend(
                Guid.NewGuid(), session, 1L, i + 1L,
                Encoding.UTF8.GetBytes(rows[i].Key),
                Encoding.UTF8.GetBytes(rows[i].Payload),
                Metadata: new Dictionary<string, byte[]>
                {
                    ["department"] = Encoding.UTF8.GetBytes(rows[i].Department),
                }));
        }

        var store = RecordStore.Named("employee-store").AddKey("department");
        var loaded = await client.LoadStore(store).FromFeedAsync(
            feed, ReaderStart.Beginning(),
            (s, record) => s.Upsert(record)
                .WithKey(record.Key!)
                .WithNamedKey("department", record.Metadata["department"]));

        Assert.Equal(3, loaded);
        Assert.Equal(3, store.Count);
        Assert.Equal("alice", Encoding.UTF8.GetString(store.Get("emp-1")!.Payload));
        Assert.Equal(2, store.Get("department", "eng").Count);
        Assert.Single(store.Get("department", "ops"));

        var copy = RecordStore.Named("employee-copy");
        store.Copy(copy);
        Assert.Equal(2, copy.Get("department", "eng").Count);
        Assert.True(store.Delete("emp-1"));
        Assert.Single(store.Get("department", "eng"));

        var visited = 0;
        store.ForEachParallel(2, entry =>
        {
            Assert.NotEmpty(entry.PrimaryKey);
            Assert.NotEmpty(entry.Record.Payload);
            Interlocked.Increment(ref visited);
        });
        Assert.Equal(2, visited);
    }
}
