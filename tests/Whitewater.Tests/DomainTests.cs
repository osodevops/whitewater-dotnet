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
}
