using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using Whitewater;
using Xunit;

namespace Whitewater.Tests;

/// <summary>
/// The typed resource methods on WhitewaterClient are WCL builders: each
/// must serialize to exactly one statement on POST /v1/admin/wcl, and
/// invalid names must fail locally before any HTTP call.
/// </summary>
public class WclBuildersTests : IDisposable
{
    private readonly HttpListener _listener;
    private readonly WhitewaterClient _client;
    private readonly CancellationTokenSource _stop = new();
    private volatile string _capturedPath = "";
    private volatile string _capturedBody = "";

    public WclBuildersTests()
    {
        _listener = new HttpListener();
        _listener.Prefixes.Add("http://127.0.0.1:0/");
        // HttpListener needs an explicit port; pick a free one.
        var port = FreePort();
        _listener.Prefixes.Clear();
        _listener.Prefixes.Add($"http://127.0.0.1:{port}/");
        _listener.Start();
        _client = new WhitewaterClient(
            "test-key", new[] { new Uri($"http://127.0.0.1:{port}") });
        _ = Task.Run(Serve);
    }

    private static int FreePort()
    {
        var probe = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
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
            _capturedPath = context.Request.Url?.AbsolutePath ?? "";
            using var reader = new StreamReader(context.Request.InputStream, Encoding.UTF8);
            _capturedBody = await reader.ReadToEndAsync();
            var payload = Encoding.UTF8.GetBytes(
                "{\"request_id\": \"00000000-0000-0000-0000-000000000000\", \"results\": []}");
            context.Response.ContentType = "application/json";
            context.Response.ContentLength64 = payload.Length;
            await context.Response.OutputStream.WriteAsync(payload);
            context.Response.Close();
        }
    }

    public void Dispose()
    {
        _stop.Cancel();
        _listener.Stop();
        _listener.Close();
    }

    private string Script()
    {
        Assert.Equal("/v1/admin/wcl", _capturedPath);
        return JsonNode.Parse(_capturedBody)!["script"]!.GetValue<string>();
    }

    [Fact]
    public async Task Creates()
    {
        await _client.CreateDomainAsync("orders");
        Assert.Equal("CREATE DOMAIN orders", Script());

        await _client.CreateFeedAsync("orders.events");
        Assert.Equal("CREATE FEED orders.events", Script());

        await _client.CreateWriterAsync("checkout", "orders.events");
        Assert.Equal("CREATE WRITER checkout TO orders.events", Script());

        await _client.CreateRoleAsync("ops");
        Assert.Equal("CREATE ROLE ops", Script());
    }

    [Fact]
    public async Task ReadersAndSubscriptions()
    {
        await _client.CreateReaderAsync("audit", "orders.events");
        Assert.Equal("CREATE READER audit FROM orders.events START AT BEGINNING", Script());

        await _client.CreateReaderAsync("live", "orders.events", ReaderStart.Now());
        Assert.Equal("CREATE READER live FROM orders.events START AT NOW", Script());

        await _client.CreateReaderAsync(
            "rewind", "orders.events", ReaderStart.TimestampNs(1700000000000000000L));
        Assert.Equal(
            "CREATE READER rewind FROM orders.events START AT TIMESTAMP 1700000000000000000",
            Script());

        await _client.CreateReaderAsync(
            "resumed", "orders.events", ReaderStart.Cursor("abc'def"));
        Assert.Equal(
            "CREATE READER resumed FROM orders.events START AT CURSOR 'abc''def'", Script());

        await _client.CreateSubscriptionAsync("team", "orders.events", ReaderStart.Now());
        Assert.Equal("CREATE SUBSCRIPTION team FROM orders.events START AT NOW", Script());
    }

    [Fact]
    public async Task SessionsAndSeek()
    {
        await _client.OpenWriterSessionAsync("checkout");
        Assert.Equal("OPEN WRITER SESSION checkout", Script());

        await _client.SeekReaderAsync("audit", ReaderStart.Now());
        Assert.Equal("SEEK READER audit TO NOW", Script());
    }

    [Fact]
    public async Task DescribeRenameDropShow()
    {
        await _client.DescribeFeedAsync("orders.events");
        Assert.Equal("DESCRIBE FEED orders.events", Script());

        await _client.DescribeSubscriptionAsync("team");
        Assert.Equal("DESCRIBE SUBSCRIPTION team", Script());

        await _client.RenameDomainAsync("orders", "billing");
        Assert.Equal("RENAME DOMAIN orders TO billing", Script());

        await _client.RenameFeedAsync("orders.events", "orders.accepted");
        Assert.Equal("RENAME FEED orders.events TO orders.accepted", Script());

        await _client.DropWriterAsync("checkout");
        Assert.Equal("DROP WRITER checkout", Script());

        await _client.ListFeedsAsync();
        Assert.Equal("SHOW FEEDS", Script());

        await _client.ListDomainsAsync();
        Assert.Equal("SHOW DOMAINS", Script());
    }

    [Fact]
    public async Task InvalidNamesFailLocally()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => _client.CreateFeedAsync("UPPER.case"));
        await Assert.ThrowsAsync<ArgumentException>(() => _client.CreateFeedAsync(".leading"));
        await Assert.ThrowsAsync<ArgumentException>(() => _client.CreateDomainAsync("two words"));
        await Assert.ThrowsAsync<ArgumentException>(
            () => _client.CreateWriterAsync("bad;name", "orders.events"));
        await Assert.ThrowsAsync<ArgumentException>(
            () => _client.CreateReaderAsync("a'b", "orders.events"));
        await Assert.ThrowsAsync<ArgumentException>(
            () => _client.RenameDomainAsync("orders", "INJECTION; DROP"));
        Assert.Equal("", _capturedPath);
    }
}
