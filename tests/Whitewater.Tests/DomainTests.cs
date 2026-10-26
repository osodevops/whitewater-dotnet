using System.Text;
using Whitewater;
using Whitewater.Testing;
using Xunit;

namespace Whitewater.Tests;

/// <summary>
/// <see cref="WhitewaterClient.Domain(string)"/> /
/// <see cref="WhitewaterClient.GetDomainAsync"/> — the strongly-typed
/// Domain facade: scoped catalog views, typed Feed handles, and fluent
/// Reader/Writer builders bound to the Domain, exercised end to end
/// through the real client against <see cref="FakeRiverbed"/>.
/// </summary>
public class DomainTests
{
    private static async Task<Domain> Seeded(WhitewaterClient client)
    {
        await client.CreateDomainAsync("shop");
        await client.CreateFeedAsync("shop.orders");
        await client.CreateFeedAsync("shop.returns");
        await client.CreateFeedAsync("shop.orders.daily");
        await client.CreateFeedAsync("other.events");
        return await client.GetDomainAsync("shop");
    }

    [Fact]
    public async Task DomainHandleIsLazyUntilACatalogAccessor()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);

        var domain = client.Domain("unmade");
        Assert.Equal("unmade.orders", domain.Feed("orders").Name);
        await Assert.ThrowsAsync<WhitewaterException.Refused>(
            () => domain.FeedNamesAsync());
    }

    [Fact]
    public async Task CreateFeedScopesInsideTheDomain()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);

        await client.CreateDomainAsync("shop");
        var domain = await client.GetDomainAsync("shop");
        await domain.CreateFeedAsync("orders");
        await domain.RefreshAsync();

        Assert.Equal("shop.orders", domain.Feed("orders").Name);
        Assert.Contains("shop.orders", await domain.FeedNamesAsync());
        // Relative names compose under the Domain — they can't escape
        // into a sibling namespace.
        Assert.Equal("shop.x.y", domain.Feed("x.y").Name);
    }

    [Fact]
    public async Task DomainHangingBuildersComposeAndOpen()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);

        await client.CreateDomainAsync("shop");
        var shop = await client.GetDomainAsync("shop");
        await shop.CreateFeedAsync("orders");

        var writer = await shop.CreateWriter("orders").WriterId("ingest").OpenAsync();
        await writer.AppendAsync("order-1", Encoding.UTF8.GetBytes("1"));

        await using var reader = await shop.CreateReader("orders")
            .ReaderId("fulfillment")
            .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning)
            .OpenAsync();
        var page = await reader.FetchAsync();
        Assert.Equal("order-1", page.Records[0].Key);
    }

    [Fact]
    public async Task ReaderForEachAcksHandledPagesAndReplaysTheTail()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);
        await client.CreateFeedAsync("shop.orders");
        var shop = client.Domain("shop");

        var writer = await shop.CreateWriter("orders").WriterId("w").OpenAsync();
        await writer.AppendAsync("a", Encoding.UTF8.GetBytes("1"));
        await writer.AppendAsync("b", Encoding.UTF8.GetBytes("2"));

        var reader = await shop.CreateReader("orders")
            .ReaderId("pump")
            .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning)
            .Capacity(1)
            .OpenAsync();
        var seen = new List<string?>();
        await reader.ForEachAsync(m =>
        {
            seen.Add(m.Key);
            return ValueTask.FromResult(seen.Count < 2);
        });
        Assert.Equal(new string?[] { "a", "b" }, seen);
        var page = await reader.FetchAsync();           // cursor state echoes the commit
        Assert.NotNull(page.AcknowledgedCursor);        // page 1 committed
        await reader.CloseAsync();

        // The unacked tail replays on the next open.
        await using var again = await shop.CreateReader("orders").ReaderId("pump").OpenAsync();
        Assert.Equal("b", (await again.FetchAsync()).Records[0].Key);
    }

    [Fact]
    public async Task ReaderMessagesAsyncStreamsPerPage()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);
        await client.CreateFeedAsync("shop.orders");
        var shop = client.Domain("shop");

        var writer = await shop.CreateWriter("orders").WriterId("w").OpenAsync();
        await writer.AppendAsync("a", Encoding.UTF8.GetBytes("1"));

        await using var reader = await shop.CreateReader("orders")
            .ReaderId("iter")
            .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning)
            .OpenAsync();
        await foreach (var m in reader.MessagesAsync())
        {
            Assert.Equal("a", m.Key);
            break;                      // page unacked → replays later
        }
    }

    [Fact]
    public async Task GetDomainMaterializesTheCatalogView()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);

        var domain = await Seeded(client);
        Assert.Equal("shop", domain.Name);
        Assert.False(string.IsNullOrEmpty(await domain.DomainIdAsync()));
        Assert.True(await domain.RevisionAsync() > 0);
        Assert.Equal("active", (await domain.DescribeAsync())["status"]!.GetValue<string>());
    }

    [Fact]
    public async Task GetDomainRefusesMissing()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);

        await Assert.ThrowsAsync<WhitewaterException.Refused>(
            () => client.GetDomainAsync("ghost"));
    }

    [Fact]
    public async Task FeedsScopeToTheDomainAndPrefixFilter()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);

        var domain = await Seeded(client);
        Assert.Equal(
            new[] { "shop.orders", "shop.orders.daily", "shop.returns" },
            await domain.FeedNamesAsync());
        Assert.Equal(
            new[] { "shop.orders", "shop.orders.daily" },
            (await domain.FeedsAsync("order")).Select(f => f.Name));
        Assert.Equal(
            new[] { "shop.returns" },
            (await domain.FeedsAsync("returns")).Select(f => f.Name));
    }

    [Fact]
    public async Task RefreshRepullsTheSnapshot()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);

        var domain = await Seeded(client);
        Assert.DoesNotContain("shop.payments", await domain.FeedNamesAsync());
        await client.CreateFeedAsync("shop.payments");
        await domain.RefreshAsync();
        Assert.Contains("shop.payments", await domain.FeedNamesAsync());
    }

    [Fact]
    public async Task ScopedListingsJoinOnFeedAndSpaceIds()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);

        var domain = await Seeded(client);
        await client.CreateWriterAsync("orders.ingest", "shop.orders");
        await client.CreateReaderAsync("fulfillment", "shop.orders");
        await client.CreateSubscriptionAsync("shop.subs.orders", "shop.orders");
        await client.CreateWriterAsync("foreign.writer", "other.events");
        await domain.RefreshAsync();

        Assert.Equal(new[] { "orders.ingest" }, await domain.WritersAsync());
        Assert.Equal(new[] { "fulfillment" }, await domain.ReadersAsync());
        Assert.Equal(new[] { "shop.subs.orders" }, await domain.SubscriptionsAsync());
    }

    [Fact]
    public void FeedComposesRelativeAndQualifiedNames()
    {
        var client = new WhitewaterClient("test-key", new[] { new Uri("http://localhost:1") });
        var domain = client.Domain("shop");
        Assert.Equal("shop.orders", domain.Feed("orders").Name);
        Assert.Equal("shop.orders", domain.Feed("shop.orders").Name);
        Assert.Equal("shop.orders", domain.GetFeed("orders").Name);
    }

    [Fact]
    public async Task ReaderBuilderDeclaresOpensAndReads()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);
        await client.CreateFeedAsync("shop.orders");

        var shop = client.Domain("shop");
        var writer = await client
            .CreateWriter(shop, "orders")
            .WriterId("orders.ingest")
            .OpenAsync();
        await writer.AppendAsync("order-1", Encoding.UTF8.GetBytes("{\"total\":9}"));

        await using var reader = await client
            .CreateReader(shop, shop.Feed("orders"))
            .ReaderId("fulfillment")
            .ReaderStartPoint(FirstTimeReaderStartPosition.Beginning)
            .OpenAsync();
        var page = await reader.FetchAsync();
        Assert.NotEmpty(page.Records);
        Assert.Equal("order-1", page.Records[0].Key);
    }

    [Fact]
    public async Task ReaderIdIsRequiredBeforeOpenOrDeclare()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);
        await client.CreateFeedAsync("shop.orders");
        var shop = client.Domain("shop");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => client.CreateReader(shop, "orders").OpenAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => client.CreateReader(shop, "orders").DeclareAsync());
    }

    [Fact]
    public void CrossDomainFeedIsRefusedLocally()
    {
        var client = new WhitewaterClient("test-key", new[] { new Uri("http://localhost:1") });
        var foreign = client.Domain("other").Feed("events");
        var shop = client.Domain("shop");

        Assert.Throws<ArgumentException>(() => client.CreateReader(shop, foreign));
        Assert.Throws<ArgumentException>(() => client.CreateWriter(shop, foreign));
    }

    [Fact]
    public async Task ReaderStartPointAppliesOnFirstDeclaration()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);
        await client.CreateFeedAsync("shop.orders");
        var shop = client.Domain("shop");

        await client
            .CreateReader(shop, "orders")
            .ReaderId("r1")
            .ReaderStartPoint(FirstTimeReaderStartPosition.End)
            .DeclareAsync();

        var described = await client.DescribeReaderAsync("r1");
        Assert.Equal(
            "now",
            described.Results[0].Data["start"]!["kind"]!.GetValue<string>());
    }

    [Fact]
    public async Task DeclareFailsWhenAlreadyDeclared()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);
        await client.CreateFeedAsync("shop.orders");
        var shop = client.Domain("shop");

        await client.CreateReader(shop, "orders").ReaderId("r1").DeclareAsync();
        await Assert.ThrowsAsync<WhitewaterException.Refused>(
            () => client.CreateReader(shop, "orders").ReaderId("r1").DeclareAsync());
    }

    [Fact]
    public async Task WriterIdIsRequiredBeforeOpenOrDeclare()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);
        await client.CreateFeedAsync("shop.orders");
        var shop = client.Domain("shop");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => client.CreateWriter(shop, "orders").OpenAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => client.CreateWriter(shop, "orders").DeclareAsync());
    }

    [Fact]
    public async Task FeedSchemaLayersTheFeedOverrideOverTheDomainDefault()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);
        var shop = await Seeded(client);

        await client.SetDomainSchemaAsync("shop",
            new FeedSchema(Value: new SchemaDecl("json", Subject: "order-v1")));
        await client.SetFeedSchemaAsync("shop.orders",
            new FeedSchema(Key: new SchemaDecl("text")));
        await shop.RefreshAsync();

        var domain = await shop.SchemaAsync();
        Assert.Equal("json", domain!.Value!.Format);
        Assert.Equal("order-v1", domain.Value.Subject);

        var effective = await shop.FeedSchemaAsync("shop.orders");
        Assert.Equal("text", effective!.Key!.Format);
        Assert.Equal("json", effective.Value!.Format);
        var declared = await shop.DeclaredFeedSchemaAsync("shop.orders");
        Assert.Equal("text", declared!.Key!.Format);
        Assert.Null(declared.Value);

        var inherited = await shop.FeedSchemaAsync("shop.returns");
        Assert.Equal("json", inherited!.Value!.Format);
        Assert.Null(inherited.Key);
        Assert.Null(await shop.DeclaredFeedSchemaAsync("shop.returns"));
    }

    [Fact]
    public async Task ClearFeedSchemaRevertsToDomainInheritance()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);
        var shop = await Seeded(client);

        await client.SetDomainSchemaAsync("shop",
            new FeedSchema(Value: new SchemaDecl("json")));
        await client.SetFeedSchemaAsync("shop.orders",
            new FeedSchema(Value: new SchemaDecl("avro")));
        await shop.RefreshAsync();
        Assert.Equal("avro",
            (await shop.FeedSchemaAsync("shop.orders"))!.Value!.Format);

        await client.ClearFeedSchemaAsync("shop.orders");
        await shop.RefreshAsync();
        Assert.Equal("json",
            (await shop.FeedSchemaAsync("shop.orders"))!.Value!.Format);
        Assert.Null(await shop.DeclaredFeedSchemaAsync("shop.orders"));
    }

    [Fact]
    public async Task SetSchemaOverWclShowSchemasAndDescribeFeedAgree()
    {
        await using var riverbed = new FakeRiverbed();
        var client = new WhitewaterClient("test-key", riverbed.Endpoints);
        var shop = await Seeded(client);

        await client.WclAsync(
            "SET SCHEMA ON DOMAIN shop VALUE json SUBJECT 'order-v1';" +
            " SET SCHEMA ON FEED shop.orders KEY protobuf DEFINITION 'syntax = proto3'");
        var rows = (await client.WclAsync("SHOW SCHEMAS")).Results[0].Data
            as System.Text.Json.Nodes.JsonArray;
        Assert.NotNull(rows);
        var byScope = rows!.OfType<System.Text.Json.Nodes.JsonObject>()
            .ToDictionary(r => r["scope_kind"]!.GetValue<string>() + ":" + r["name"]!.GetValue<string>(),
                          r => r["schema"] as System.Text.Json.Nodes.JsonObject);
        Assert.Equal("order-v1",
            byScope["domain:shop"]!["value"]!["subject"]!.GetValue<string>());
        Assert.Equal("protobuf",
            byScope["feed:shop.orders"]!["key"]!["format"]!.GetValue<string>());

        var described = (await client.CommandsAsync(new System.Text.Json.Nodes.JsonObject[]
        {
            new() { ["command"] = "describe", ["kind"] = "feed", ["name"] = "shop.orders" },
        })).Results[0].Data as System.Text.Json.Nodes.JsonObject;
        Assert.Equal("protobuf",
            described!["schema"]!["key"]!["format"]!.GetValue<string>());
        Assert.Equal("json",
            described["effective_schema"]!["value"]!["format"]!.GetValue<string>());

        await shop.RefreshAsync();
        var effective = await shop.FeedSchemaAsync("shop.orders");
        Assert.Equal("protobuf", effective!.Key!.Format);
        Assert.Equal("json", effective.Value!.Format);
    }
}
