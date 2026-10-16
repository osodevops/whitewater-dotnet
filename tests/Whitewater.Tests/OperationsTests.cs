using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Whitewater;
using Xunit;

namespace Whitewater.Tests;

/// <summary>
/// Scripted stub covering Pipe lifecycle, index queries, and demand
/// observability — the operational surfaces parity-tested against
/// recorded wire shapes.
/// </summary>
public class OperationsTests : IDisposable
{
    private readonly HttpListener _listener;
    private readonly WhitewaterClient _client;
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentQueue<(string Path, string Body)> _calls = new();

    private const string PipeRow = """
        {
          "name": "orders.relay",
          "stage": "paused",
          "subscription_id": "019a0000-0000-7000-8000-000000000020",
          "operation": {"operation": "forward"},
          "output_feed_id": "019a0000-0000-7000-8000-000000000021",
          "output_store_id": null,
          "status": "active",
          "created_at_ns": 42
        }
        """;

    private static string CommandsWith(string data) => $$"""
        {
          "request_id": "019a0000-0000-7000-8000-000000000006",
          "revision": 9, "authority": "control_plane", "warning": null,
          "results": [{"statement": "s", "message": "ok", "data": {{data}}}]
        }
        """;

    private static string B64(string value)
        => Convert.ToBase64String(Encoding.UTF8.GetBytes(value));

    public OperationsTests()
    {
        _listener = new HttpListener();
        var probe = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        _listener.Prefixes.Add($"http://127.0.0.1:{port}/");
        _listener.Start();
        _client = new WhitewaterClient(
            "test-key", new[] { new Uri($"http://127.0.0.1:{port}") });
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
            var path = context.Request.Url?.AbsolutePath ?? "";
            using var reader = new StreamReader(context.Request.InputStream, Encoding.UTF8);
            var body = await reader.ReadToEndAsync();
            _calls.Enqueue((path, body));
            var response = Route(path, body);
            var bytes = Encoding.UTF8.GetBytes(response);
            context.Response.ContentType = "application/json";
            context.Response.ContentLength64 = bytes.Length;
            await context.Response.OutputStream.WriteAsync(bytes);
            context.Response.Close();
        }
    }

    private string Route(string path, string body)
    {
        if (path == "/v1/admin/commands")
        {
            var verb = JsonNode.Parse(body)?["commands"]?[0]?["command"]?.GetValue<string>();
            return verb switch
            {
                "describe" => CommandsWith(PipeRow),
                "show" => CommandsWith($$"""{"rows": [{{PipeRow}}]}"""),
                _ => CommandsWith("{}"),
            };
        }
        return path switch
        {
            "/v1/admin/demand/feeds" => """
                {
                  "feeds": [{
                    "feed": "orders.events", "domain": "orders",
                    "appended_records": 1500, "appended_bytes": 2048000,
                    "read_pages": 42
                  }],
                  "domains": {"orders": {"feeds": 1, "appended_records": 1500}}
                }
                """,
            "/v1/admin/demand/hot-keys" => """
                {
                  "hot_keys": [{
                    "feed": "orders.events", "share_bps": 9000,
                    "window_records": 900, "window_bytes": 12000
                  }]
                }
                """,
            "/v1/admin/demand/readers" or "/v1/admin/demand/subscriptions"
                or "/v1/admin/demand/indexes" or "/v1/admin/demand/pipes"
                => "{\"" + path[(path.LastIndexOf('/') + 1)..] + "\": [], \"domains\": {}}",
            "/v1/indexes/query" => $$"""
                {
                  "index": "crm.byregion",
                  "index_id": "019a0000-0000-7000-8000-000000000030",
                  "generation": 2,
                  "rows": [{
                    "key_base64": "{{B64("acct-1")}}",
                    "payload_base64": "{{B64("{\"tier\":\"gold\"}")}}"
                  }],
                  "applied_cursor": "idx-cursor",
                  "target_cursor": "tip-cursor",
                  "behind": false, "truncated": false,
                  "served_by": "node-1"
                }
                """,
            _ => "{}",
        };
    }

    private JsonObject PostedCommand(int index)
    {
        var body = JsonNode.Parse(_calls.ElementAt(index).Body)!.AsObject();
        return body["commands"]![0]!.AsObject();
    }

    // ---- Pipe lifecycle --------------------------------------------------------

    [Fact]
    public async Task PipeDescribeDecodesTheCatalogRow()
    {
        var info = await _client.Pipe("orders.relay").DescribeAsync();
        Assert.Equal("orders.relay", info.Name);
        Assert.Equal("paused", info.Stage);
        Assert.Equal(
            Guid.Parse("019a0000-0000-7000-8000-000000000020"), info.SubscriptionId);
        Assert.Equal("forward", info.Operation?["operation"]?.GetValue<string>());
        Assert.Equal(
            Guid.Parse("019a0000-0000-7000-8000-000000000021"), info.OutputFeedId);
        Assert.Null(info.OutputStoreId);
        Assert.Equal("active", info.Status);
        Assert.Equal(42, info.CreatedAtNs);
        var command = PostedCommand(0);
        Assert.Equal("describe", command["command"]?.GetValue<string>());
        Assert.Equal("pipe", command["kind"]?.GetValue<string>());
    }

    [Fact]
    public async Task PipeLifecyclePostsTheTypedVerbs()
    {
        var pipe = _client.Pipe("orders.relay");
        await pipe.PauseAsync();
        await pipe.ResumeAsync();
        await pipe.DropAsync();
        Assert.Equal(3, _calls.Count);
        Assert.Equal("pause_pipe", PostedCommand(0)["command"]?.GetValue<string>());
        Assert.Equal("resume_pipe", PostedCommand(1)["command"]?.GetValue<string>());
        var drop = PostedCommand(2);
        Assert.Equal("drop", drop["command"]?.GetValue<string>());
        Assert.Equal("pipe", drop["kind"]?.GetValue<string>());
        Assert.All(_calls, call => Assert.Equal("/v1/admin/commands", call.Path));
    }

    [Fact]
    public async Task PipesListsTheCatalog()
    {
        var pipes = await _client.PipesAsync();
        var info = Assert.Single(pipes);
        Assert.Equal("orders.relay", info.Name);
        Assert.Equal("paused", info.Stage);
        var command = PostedCommand(0);
        Assert.Equal("show", command["command"]?.GetValue<string>());
        Assert.Equal("pipes", command["kind"]?.GetValue<string>());
    }

    // ---- Demand observability ----------------------------------------------------

    [Fact]
    public async Task DemandFeedsDecodesRowsAndDomainRollups()
    {
        var report = await _client.DemandFeedsAsync();
        var row = Assert.Single(report.Rows);
        Assert.Equal("orders.events", row.Name);
        Assert.Equal("orders", row.Domain);
        Assert.Equal(1500, row.Counter("appended_records"));
        Assert.Equal(42, row.Counter("read_pages"));
        Assert.Null(row.Counter("nonsense"));
        Assert.Equal(1, report.Domain("orders")?["feeds"]?.GetValue<int>());
        Assert.Equal("/v1/admin/demand/feeds", _calls.First().Path);
    }

    [Fact]
    public async Task DemandEndpointsCoverEveryResourceKind()
    {
        Assert.Empty((await _client.DemandReadersAsync()).Rows);
        Assert.Empty((await _client.DemandSubscriptionsAsync()).Rows);
        Assert.Empty((await _client.DemandIndexesAsync()).Rows);
        Assert.Empty((await _client.DemandPipesAsync()).Rows);
        var paths = _calls.Select(call => call.Path).ToArray();
        Assert.Equal(
            new[]
            {
                "/v1/admin/demand/readers",
                "/v1/admin/demand/subscriptions",
                "/v1/admin/demand/indexes",
                "/v1/admin/demand/pipes",
            },
            paths);
    }

    [Fact]
    public async Task HotKeysDecodeDominantKeyEvidence()
    {
        var keys = await _client.HotKeysAsync();
        var key = Assert.Single(keys);
        Assert.Equal("orders.events", key.Feed);
        Assert.Equal(9000, key.ShareBps);
        Assert.Equal(900, key.WindowRecords);
        Assert.Equal(12000, key.WindowBytes);
        Assert.Equal("/v1/admin/demand/hot-keys", _calls.First().Path);
    }

    // ---- Index queries ------------------------------------------------------------

    [Fact]
    public async Task QueryIndexRangeFlattensBoundsAndDecodesRows()
    {
        var page = await _client.QueryIndexAsync(
            "crm.byregion",
            IndexPredicate.Range(
                new object?[] { "emea" },
                IndexBound.InclusiveBound(0L),
                IndexBound.ExclusiveBound(99L)),
            limit: 50,
            strict: true);
        Assert.False(page.Behind);
        Assert.Equal(2, page.Generation);
        var row = Assert.Single(page.Rows);
        Assert.Equal("acct-1", Encoding.UTF8.GetString(row.Key));
        Assert.Equal("node-1", page.ServedBy);
        Assert.Equal("idx-cursor", page.AppliedCursor);
        var body = JsonNode.Parse(_calls.First().Body)!.AsObject();
        Assert.Equal("range", body["mode"]?.GetValue<string>());
        Assert.True(body["strict"]?.GetValue<bool>());
        Assert.Equal(50, body["limit"]?.GetValue<int>());
        Assert.False(body["upper"]?["inclusive"]?.GetValue<bool>());
        Assert.Equal("/v1/indexes/query", _calls.First().Path);
    }

    [Fact]
    public async Task QueryIndexExactCarriesEveryTypedKind()
    {
        await _client.QueryIndexAsync(
            "crm.byregion",
            IndexPredicate.Exact("emea", 42L, true, null, new byte[] { 1, 2 }));
        var body = JsonNode.Parse(_calls.First().Body)!.AsObject();
        Assert.Equal("exact", body["mode"]?.GetValue<string>());
        var values = body["values"]!.AsArray();
        Assert.Equal(5, values.Count);
        Assert.Equal("text", values[0]?["kind"]?.GetValue<string>());
        Assert.Equal("i64", values[1]?["kind"]?.GetValue<string>());
        Assert.Equal("bool", values[2]?["kind"]?.GetValue<string>());
        Assert.Equal("null", values[3]?["kind"]?.GetValue<string>());
        Assert.Equal("bytes", values[4]?["kind"]?.GetValue<string>());
    }

    [Fact]
    public async Task QueryIndexPrefixOmitsPartialTextWhenAbsent()
    {
        await _client.QueryIndexAsync(
            "crm.byregion", IndexPredicate.Prefix(new object?[] { "emea" }));
        var first = JsonNode.Parse(_calls.ElementAt(0).Body)!.AsObject();
        Assert.Equal("prefix", first["mode"]?.GetValue<string>());
        Assert.Null(first["partial_text"]);

        await _client.QueryIndexAsync(
            "crm.byregion",
            IndexPredicate.Prefix(new object?[] { "emea" }, "gold"));
        var second = JsonNode.Parse(_calls.ElementAt(1).Body)!.AsObject();
        Assert.Equal("gold", second["partial_text"]?.GetValue<string>());
    }
}
