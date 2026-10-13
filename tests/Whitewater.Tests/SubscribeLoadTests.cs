using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Whitewater;
using Xunit;

namespace Whitewater.Tests;

/// <summary>
/// Scripted multi-path stub covering Subscribe()'s session loop and
/// LoadAsync()'s paging.
/// </summary>
public class SubscribeLoadTests : IDisposable
{
    private readonly HttpListener _listener;
    private readonly WhitewaterClient _client;
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentQueue<(string Path, string Body)> _calls = new();
    private readonly ConcurrentQueue<string> _fetchPages = new();
    private readonly ConcurrentQueue<string> _temporaryPages = new();

    private const string Session = """
        {
          "reader_id": "00000000-0000-7000-8000-000000000004",
          "feed_id": "00000000-0000-7000-8000-000000000005",
          "session_epoch": 3,
          "capacity": 128,
          "delivered_cursor": null,
          "acknowledged_cursor": null
        }
        """;

    private const string WclOk = """
        {
          "request_id": "00000000-0000-7000-8000-000000000006",
          "revision": 9,
          "authority": "control_plane",
          "warning": null,
          "results": [{"statement": "s", "message": "ok", "data": {}}]
        }
        """;

    public SubscribeLoadTests()
    {
        _listener = new HttpListener();
        var port = FreePort();
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

    private static string RecordJson(string cursor, string keyBase64) => $$"""
        {
          "cursor": "{{cursor}}",
          "message_id": "00000000-0000-7000-8000-000000000002",
          "producer_id": "00000000-0000-7000-8000-000000000003",
          "sequence": 7,
          "event_time_ns": "1697040000123456789",
          "ingest_time_ns": "1697040000223456789",
          "key_base64": "{{keyBase64}}",
          "payload_base64": "dmFs",
          "metadata_base64": {}
        }
        """;

    private static string PageJson(string? delivered, params string[] cursors)
    {
        var records = string.Join(",", cursors.Select(c =>
            RecordJson(c, Convert.ToBase64String(Encoding.UTF8.GetBytes("k-" + c)))));
        return $$"""
            {"records": [{{records}}], "delivered_cursor": {{Json(delivered)}},
             "acknowledged_cursor": null, "recommended_capacity": 0, "retry_after_ms": 0}
            """;
    }

    private static string TemporaryPageJson(string? next, params string[] cursors)
    {
        var records = string.Join(",", cursors.Select(c =>
            RecordJson(c, Convert.ToBase64String(Encoding.UTF8.GetBytes("k-" + c)))));
        return $$"""{"records": [{{records}}], "next_cursor": {{Json(next)}}}""";
    }

    private static string Json(string? value) =>
        value is null ? "null" : $"\"{value}\"";

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
            var response = Route(path);
            var bytes = Encoding.UTF8.GetBytes(response);
            context.Response.ContentType = "application/json";
            context.Response.ContentLength64 = bytes.Length;
            await context.Response.OutputStream.WriteAsync(bytes);
            context.Response.Close();
        }
    }

    private string Route(string path) => path switch
    {
        "/v1/admin/wcl" => WclOk,
        "/v1/feeds/append" or "/v1/writers/append" => "{\"message_id\":\"00000000-0000-4000-8000-000000000001\",\"cursor\":\"opaque\",\"deduplicated\":false}",
        "/v1/writers/append-batch" => "{\"results\":[],\"feedback\":{}}",
        "/v1/readers/open" or "/v1/readers/ack" or "/v1/readers/close" => Session,
        "/v1/readers/fetch" => _fetchPages.TryDequeue(out var page)
            ? page
            : PageJson(null),
        "/v1/readers/temporary/fetch" => _temporaryPages.TryDequeue(out var page)
            ? page
            : TemporaryPageJson(null),
        _ => "{}",
    };

    private List<string> TemporaryBodies() => _calls
        .Where(c => c.Path == "/v1/readers/temporary/fetch")
        .Select(c => c.Body)
        .ToList();

    public void Dispose()
    {
        _stop.Cancel();
        _listener.Stop();
        _listener.Close();
        _stop.Dispose();
    }

    [Fact]
    public async Task CompressionCanBeToggledForFeedAndWriterWithoutChangingPayloads()
    {
        var feeds = _client.Feeds;
        var writer = _client.Writer("orders.writer", 7);
        var id = Guid.NewGuid();
        var record = new FeedAppend(id, Guid.NewGuid(), 1, 1, new byte[] { 1 }, new byte[] { 0, 255 });
        foreach (var enabled in new[] { false, true, false, true })
        {
            feeds = feeds.UseCompression(enabled);
            await feeds.AppendAsync("orders.events", record);
            writer = writer.UseCompression(enabled);
            await writer.AppendAsync(record.Key, record.Payload, id);
            await writer.AppendBatchAsync(new[] { new WriterSessionClient.AppendInput(record.Key, record.Payload, id) });
            foreach (var call in _calls.ToArray().TakeLast(3))
            {
                var body = JsonNode.Parse(call.Body)!;
                if (call.Path == "/v1/writers/append-batch") body = body["records"]![0]!;
                Assert.Equal(enabled, body["use_compression"]?.GetValue<bool>() ?? false);
                Assert.Equal("AP8=", body["payload_base64"]!.GetValue<string>());
                Assert.Equal(id.ToString(), body["request_id"]!.GetValue<string>());
            }
        }
        await _client.Feeds.AppendAsync("orders.events", record);
        Assert.Null(JsonNode.Parse(_calls.ToArray().Last().Body)!["use_compression"]);
    }

    [Fact]
    public async Task LoadPagesUntilEmptyAndFillsTheStore()
    {
        _temporaryPages.Enqueue(TemporaryPageJson("c1", "r1"));
        _temporaryPages.Enqueue(TemporaryPageJson("c2", "r2"));

        var sink = new List<IReadOnlyList<FeedRecord>>();
        var loaded = await _client.Feeds.LoadAsync(
            "hr.employees", ReaderStart.Beginning(),
            records =>
            {
                sink.Add(records);
                return Task.CompletedTask;
            });

        Assert.Equal(2, loaded);
        Assert.Equal(new[] { "r1", "r2" }, sink.SelectMany(r => r).Select(r => r.Cursor.Value));
        var bodies = TemporaryBodies();
        Assert.Equal(3, bodies.Count);
        var first = JsonNode.Parse(bodies[0])!;
        var second = JsonNode.Parse(bodies[1])!;
        Assert.Null(first["after"]);
        Assert.Equal("c1", second["after"]!.GetValue<string>());
    }

    [Fact]
    public async Task LoadSeeksByEventTimeThenContinuesOnCursor()
    {
        _temporaryPages.Enqueue(TemporaryPageJson("c1", "r1"));

        var sink = new List<IReadOnlyList<FeedRecord>>();
        await _client.Feeds.LoadAsync(
            "hr.employees", ReaderStart.TimestampNs(1697040000000000000L),
            records =>
            {
                sink.Add(records);
                return Task.CompletedTask;
            });

        var bodies = TemporaryBodies();
        var first = JsonNode.Parse(bodies[0])!;
        Assert.Equal("1697040000000000000", first["after_event_time_ns"]!.GetValue<string>());
        Assert.Null(first["after"]);
        Assert.Equal("c1", JsonNode.Parse(bodies[1])!["after"]!.GetValue<string>());
        Assert.Single(sink);
    }

    [Fact]
    public async Task SubscribeOpensDeliversAcksAndCloses()
    {
        _fetchPages.Enqueue(PageJson("d1", "r1"));
        var delivered = new TaskCompletionSource();
        var batches = new ConcurrentQueue<IReadOnlyList<FeedRecord>>();

        await using var sub = _client.Subscribe(
            "hr-view", "hr.employees", ReaderStart.Beginning(),
            records =>
            {
                batches.Enqueue(records);
                delivered.TrySetResult();
                return Task.CompletedTask;
            });

        await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        // The ack posts after the handler returns — wait for it before close.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (!_calls.Any(c => c.Path == "/v1/readers/ack") && DateTime.UtcNow < deadline)
        {
            await Task.Delay(25);
        }
        await sub.DisposeAsync();

        Assert.Single(batches);
        Assert.Single(_calls.Where(c => c.Path == "/v1/admin/wcl"));
        var ack = _calls.FirstOrDefault(c => c.Path == "/v1/readers/ack");
        Assert.NotEqual(default, ack);
        Assert.Equal("d1", JsonNode.Parse(ack.Body)!["cursor"]!.GetValue<string>());
        Assert.Contains(_calls, c => c.Path == "/v1/readers/close");
        Assert.False(sub.Running);
    }

}
