using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using Whitewater;
using Xunit;

namespace Whitewater.Tests;

/// <summary>
/// The Domain is the only front door: every resource name passed to a
/// Domain method resolves inside that Domain ("orders" inside "pipes"
/// is pipes.orders), and pipe endpoints can cross Domains only through
/// an explicit absolute reference — a handle
/// (Feed/Store/StateStore/SubscriptionRef) or Scoped.Absolute. These
/// tests pin the exact wire shapes both directions.
/// </summary>
public class DomainSurfaceTests : IDisposable
{
    private readonly HttpListener _listener;
    private readonly WhitewaterClient _client;
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentQueue<(string Path, string Body)> _calls = new();

    public DomainSurfaceTests()
    {
        for (var attempt = 0; ; attempt++)
        {
            var probe = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
            probe.Start();
            var port = ((IPEndPoint)probe.LocalEndpoint).Port;
            probe.Stop();
            try
            {
                _listener = new HttpListener();
                _listener.Prefixes.Add($"http://127.0.0.1:{port}/");
                _listener.Start();
                _client = new WhitewaterClient(
                    "test-key", new[] { new Uri($"http://127.0.0.1:{port}") });
                break;
            }
            catch (HttpListenerException) when (attempt < 8)
            {
            }
        }
        _ = Task.Run(Serve);
    }

    public void Dispose()
    {
        _stop.Cancel();
        _listener.Stop();
        _listener.Close();
    }

    private async Task Serve()
    {
        while (!_stop.IsCancellationRequested)
        {
            HttpListenerContext context;
            try
            {
                context = await _listener.GetContextAsync();
            }
            catch
            {
                return;
            }
            using var reader = new StreamReader(context.Request.InputStream, Encoding.UTF8);
            _calls.Enqueue((context.Request.Url?.AbsolutePath ?? "", await reader.ReadToEndAsync()));
            var response = context.Request.Url?.AbsolutePath switch
            {
                "/v1/readers/open" => """
                    {"reader_id":"018f0000-0000-7000-8000-000000000001",
                     "feed_id":"018f0000-0000-7000-8000-000000000002",
                     "session_epoch":3,"capacity":64,
                     "delivered_cursor":null,"acknowledged_cursor":null}
                    """,
                _ => """
                    {"request_id":"019a0000-0000-7000-8000-000000000006",
                     "revision":9,"authority":"control_plane","warning":null,
                     "results":[{"statement":"s","message":"ok",
                                 "data":{"session_epoch":3}}]}
                    """,
            };
            var bytes = Encoding.UTF8.GetBytes(response);
            context.Response.ContentType = "application/json";
            context.Response.ContentLength64 = bytes.Length;
            await context.Response.OutputStream.WriteAsync(bytes);
            context.Response.Close();
        }
    }

    private string ScriptAt(int index)
    {
        var (path, body) = _calls.ElementAt(index);
        Assert.Equal("/v1/admin/wcl", path);
        return JsonNode.Parse(body)!["script"]!.GetValue<string>();
    }

    private JsonObject CommandAt(int index)
    {
        var (path, body) = _calls.ElementAt(index);
        Assert.Equal("/v1/admin/commands", path);
        return JsonNode.Parse(body)!["commands"]![0]!.AsObject();
    }

    [Fact]
    public async Task CreateDomainReturnsAUsableDomainHandle()
    {
        var pipes = await _client.CreateDomainAsync("pipes");
        Assert.Equal("CREATE DOMAIN pipes", ScriptAt(0));

        Assert.Equal("pipes", pipes.Name);
        Assert.Equal("pipes.orders", pipes.Feed("orders").Name);
        Assert.Equal("pipes.orders", pipes.Feed("pipes.orders").Name);
    }

    [Fact]
    public async Task DomainScopedResourcesResolveInsideTheDomain()
    {
        var pipes = _client.Domain("pipes");

        await pipes.CreateFeedAsync("orders");
        Assert.Equal("CREATE FEED pipes.orders", ScriptAt(0));

        await pipes.CreateFeedAsync("pipes.orders.deep");
        Assert.Equal("CREATE FEED pipes.orders.deep", ScriptAt(1));

        await pipes.CreateSubscriptionAsync("audit", "orders");
        Assert.Equal(
            "CREATE SUBSCRIPTION pipes.audit FROM pipes.orders START AT BEGINNING",
            ScriptAt(2));

        await pipes.DescribeReaderAsync("audit");
        Assert.Equal("DESCRIBE READER pipes.audit", ScriptAt(3));

        await pipes.RenameFeedAsync("orders", "orders.v2");
        Assert.Equal("RENAME FEED pipes.orders TO pipes.orders.v2", ScriptAt(4));

        await pipes.DropSubscriptionAsync("audit");
        Assert.Equal("DROP SUBSCRIPTION pipes.audit", ScriptAt(5));

        await pipes.SeekReaderAsync("audit", ReaderStart.Now());
        Assert.Equal("SEEK READER pipes.audit TO NOW", ScriptAt(6));

        await pipes.OpenWriterSessionAsync("ingest");
        Assert.Equal("OPEN WRITER SESSION pipes.ingest", ScriptAt(7));
    }

    [Fact]
    public async Task ReaderWriterAndMemberHandlesBindResolvedNames()
    {
        var shop = _client.Domain("shop");

        var session = await shop.Reader("fulfil").OpenAsync(8);
        var open = JsonNode.Parse(_calls.ElementAt(0).Body)!;
        Assert.Equal("shop.fulfil", open["reader"]!.GetValue<string>());
        Assert.Equal(3, session.SessionEpoch);

        Assert.Equal("shop.fulfil", shop.Subscription("fulfil").Name);
        Assert.Equal("shop.fulfil", shop.Scoped("fulfil").Name);
    }

    [Fact]
    public async Task ForwardPipeResolvesEveryNameInsideTheDomain()
    {
        var pipes = _client.Domain("pipes");

        await pipes.DefineForwardPipeAsync("relay", "audit", "orders.copy");
        var command = CommandAt(0);
        Assert.Equal("define_pipe", command["command"]!.GetValue<string>());
        Assert.Equal("pipes.relay", command["name"]!.GetValue<string>());
        Assert.Equal("pipes.audit", command["subscription"]!.GetValue<string>());
        Assert.Equal("pipes.orders.copy", command["output_feed"]!.GetValue<string>());
    }

    [Fact]
    public async Task CrossDomainPipeKeepsAbsoluteReferences()
    {
        var purchasing = _client.Domain("purchasing");
        var warehouse = _client.Domain("warehouse");

        // A SubscriptionRef from another Domain travels with its
        // absolute name — the Pipe lives in warehouse but drives from
        // purchasing.fulfil.
        await warehouse.DefineForwardPipeAsync(
            "pull", purchasing.Subscription("fulfil"), "inbound");
        var pull = CommandAt(0);
        Assert.Equal("warehouse.pull", pull["name"]!.GetValue<string>());
        Assert.Equal("purchasing.fulfil", pull["subscription"]!.GetValue<string>());
        Assert.Equal("warehouse.inbound", pull["output_feed"]!.GetValue<string>());

        // A Feed handle does the same for the output side.
        await purchasing.DefineForwardPipeAsync(
            "relay", "fulfil", warehouse.Feed("inbound"));
        var relay = CommandAt(1);
        Assert.Equal("purchasing.relay", relay["name"]!.GetValue<string>());
        Assert.Equal("purchasing.fulfil", relay["subscription"]!.GetValue<string>());
        Assert.Equal("warehouse.inbound", relay["output_feed"]!.GetValue<string>());

        // Scoped.Absolute is the handle-free explicit form.
        await purchasing.DefineForwardPipeAsync(
            "relay2", "fulfil", Scoped.Absolute("warehouse.inbound"));
        var relay2 = CommandAt(2);
        Assert.Equal("warehouse.inbound", relay2["output_feed"]!.GetValue<string>());
    }

    [Fact]
    public async Task EnrichAndWindowPipeEndpointsScopeTheSameWay()
    {
        var ops = _client.Domain("ops");
        var accounts = _client.Domain("accounts");

        await ops.DefineEnrichPipeAsync(
            "enrich", "audit", "orders.enriched", accounts.StateStore("customers"),
            PipeOperation.Enrich(Extractor.Metadata("customer_id"),
                onMiss: EnrichMiss.Quarantine),
            quarantineFeed: "orders.quarantine");
        var enrich = CommandAt(0);
        Assert.Equal("ops.enrich", enrich["name"]!.GetValue<string>());
        Assert.Equal("ops.audit", enrich["subscription"]!.GetValue<string>());
        Assert.Equal("ops.orders.enriched", enrich["output_feed"]!.GetValue<string>());
        Assert.Equal("accounts.customers", enrich["output_store"]!.GetValue<string>());
        Assert.Equal("ops.orders.quarantine", enrich["quarantine_feed"]!.GetValue<string>());

        await ops.DefineWindowPipeAsync(
            "hourly", "audit", "hourlyspend",
            PipeOperation.Window(
                TimeSpan.FromHours(1), Extractor.RecordKey(), WindowAggregation.Count()),
            emitFeed: accounts.Feed("emitted"));
        var window = CommandAt(1);
        Assert.Equal("ops.hourly", window["name"]!.GetValue<string>());
        Assert.Equal("ops.hourlyspend", window["output_store"]!.GetValue<string>());
        Assert.Equal("accounts.emitted", window["emit_feed"]!.GetValue<string>());
    }

    [Fact]
    public async Task StateStoreDeclarationResolvesTheSpecNameInsideTheDomain()
    {
        var app = _client.Domain("app");

        await app.DefineStateStoreAsync(
            StateStore.Named("profiles").AddUniqueKey("user_id"));
        var define = CommandAt(0);
        Assert.Equal("define_state_store", define["command"]!.GetValue<string>());
        Assert.Equal("app.profiles", define["name"]!.GetValue<string>());
        Assert.Equal("manual", define["source"]!["kind"]!.GetValue<string>());

        Assert.Equal("app.profiles", app.StateStore("profiles").Name);
        Assert.Equal("app.profiles", app.Store("profiles").Name);
    }

    [Fact]
    public async Task SchemaAndIndexCallsResolveInsideTheDomain()
    {
        var app = _client.Domain("app");

        await app.SetFeedSchemaAsync(
            "orders", new FeedSchema(Value: new SchemaDecl("json")));
        var setSchema = CommandAt(0);
        Assert.Equal("set_schema", setSchema["command"]!.GetValue<string>());
        Assert.Equal("app.orders", setSchema["scope"]!["name"]!.GetValue<string>());

        await app.QueryIndexAsync("bycustomer", IndexPredicate.Exact("u-1"));
        var (path, body) = _calls.ElementAt(1);
        Assert.Equal("/v1/indexes/query", path);
        Assert.Equal("app.bycustomer", JsonNode.Parse(body)!["index"]!.GetValue<string>());
    }
}
