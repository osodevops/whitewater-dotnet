using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Whitewater;
using Xunit;

namespace Whitewater.Tests;

/// <summary>Scripted stub covering the codec-bound Feed facade endpoints.</summary>
public class FeedTests : IDisposable
{
    private sealed record Order(string Id, int Total);

    private readonly HttpListener _listener;
    private readonly WhitewaterClient _client;
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentQueue<(string Path, string Body)> _calls = new();
    private readonly ConcurrentQueue<string> _fetchPages = new();
    private readonly ConcurrentQueue<string> _temporaryPages = new();
    private string _recordsResponse = "[]";

    private const string WclSession = """
        {
          "request_id": "00000000-0000-7000-8000-000000000010",
          "revision": 1, "authority": "control_plane", "warning": null,
          "results": [{"statement": "s", "message": "ok",
                       "data": {"session_epoch": 42}}]
        }
        """;

    private const string SessionJson = """
        {
          "reader_id": "00000000-0000-7000-8000-000000000004",
          "feed_id": "00000000-0000-7000-8000-000000000005",
          "session_epoch": 3, "capacity": 128,
          "delivered_cursor": null, "acknowledged_cursor": null
        }
        """;

    private const string AppendOk = """
        {"message_id": "00000000-0000-7000-8000-000000000009",
         "cursor": "c1", "deduplicated": false,
         "feedback": {"recommended_batch_count": 8, "pressure": 0.1}}
        """;

    private const string BatchOk = """
        {"results": [{"message_id": "00000000-0000-7000-8000-000000000011",
                      "cursor": "c1", "deduplicated": false}],
         "feedback": {"recommended_batch_count": 8}}
        """;

    private static string B64(string value)
        => Convert.ToBase64String(Encoding.UTF8.GetBytes(value));

    private static string RecordJson(
        string cursor, string? key, string payload,
        IDictionary<string, string>? metadata = null)
    {
        var meta = metadata is null
            ? "{}"
            : JsonSerializer.Serialize(
                metadata.ToDictionary(kv => kv.Key, kv => B64(kv.Value)));
        return $$"""
            {"cursor": "{{cursor}}",
             "message_id": "00000000-0000-7000-8000-000000000002",
             "producer_id": "00000000-0000-7000-8000-000000000003",
             "sequence": 7,
             "event_time_ns": "1697040000123456789",
             "ingest_time_ns": "1697040000223456789",
             {{(key is null ? "" : $"\"key_base64\": \"{B64(key)}\",")}}
             "payload_base64": "{{B64(payload)}}",
             "metadata_base64": {{meta}}}
            """;
    }

    public FeedTests()
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
        "/v1/admin/wcl" => WclSession,
        "/v1/writers/append" or "/v1/feeds/append" => AppendOk,
        "/v1/writers/append-batch" => BatchOk,
        "/v1/readers/open" or "/v1/readers/ack" or "/v1/readers/close" => SessionJson,
        "/v1/readers/fetch" => _fetchPages.TryDequeue(out var page)
            ? page
            : """{"records": [], "delivered_cursor": null}""",
        "/v1/readers/temporary/fetch" => _temporaryPages.TryDequeue(out var page)
            ? page
            : """{"records": [], "next_cursor": null}""",
        "/v1/feeds/records" => _recordsResponse,
        _ => "{}",
    };

    private JsonNode[] Bodies(string path)
        => _calls.Where(c => c.Path == path)
            .Select(c => JsonNode.Parse(c.Body)!).ToArray();

    private int Count(string path) => _calls.Count(c => c.Path == path);

    private static string Text(JsonNode? node, string name)
        => Encoding.UTF8.GetString(Convert.FromBase64String(node![name]!.GetValue<string>()));

    [Fact]
    public void FeedRefusesInvalidDottedNames()
    {
        Assert.Throws<ArgumentException>(
            () => _client.Feed<Order>("Bad Name", Codecs.Json<Order>()));
        Assert.Equal("orders.events",
            _client.Feed<Order>("orders.events", Codecs.Json<Order>()).Name);
    }

    [Fact]
    public async Task RecordsDecodesJsonAndPreservesTheEnvelope()
    {
        _recordsResponse = $$"""
            [{{RecordJson("c1", "order-1", """{"id":"order-1","total":42}""",
                new Dictionary<string, string> { ["tenant"] = "acme" })}}]
            """;
        var orders = _client.Feed("orders.events", Codecs.Json<Order>());
        var messages = await orders.RecordsAsync(10);
        var m = Assert.Single(messages);
        Assert.Equal("order-1", m.Key);
        Assert.Equal(new Order("order-1", 42), m.Value);
        Assert.Equal("c1", m.Cursor.Value);
        Assert.Equal(7, m.Sequence);
        Assert.Equal(1697040000123456789, m.EventTimeNs);
        Assert.Equal("acme", m.MetadataText("tenant"));
    }

    [Fact]
    public async Task FetchReturnsDecodedTemporaryPageWithNextCursor()
    {
        _temporaryPages.Enqueue($$"""
            {"records": [{{RecordJson("c1", "o1", """{"id":"o1","total":1}""")}}],
             "next_cursor": "c9"}
            """);
        var orders = _client.Feed("orders.events", Codecs.Json<Order>());
        var page = await orders.FetchAsync(new TemporaryFetchOptions(Limit: 4));
        Assert.Equal(1, Assert.Single(page.Records).Value.Total);
        Assert.Equal("c9", page.NextCursor?.Value);
    }

    [Fact]
    public async Task LoadDeliversDecodedBatchesUntilDrained()
    {
        _temporaryPages.Enqueue($$"""
            {"records": [
               {{RecordJson("c1", "k1", """{"id":"a","total":1}""")}},
               {{RecordJson("c2", "k2", """{"id":"b","total":2}""")}}],
             "next_cursor": "c2"}
            """);
        var orders = _client.Feed("orders.events", Codecs.Json<Order>());
        var seen = new List<Order>();
        var loaded = await orders.LoadAsync(
            ReaderStart.Beginning(),
            messages =>
            {
                seen.AddRange(messages.Select(m => m.Value));
                return Task.CompletedTask;
            });
        Assert.Equal(2, loaded);
        Assert.Equal(new[] { "a", "b" }, seen.Select(o => o.Id).ToArray());
    }

    [Fact]
    public async Task WriterDeclaresOpensAndEncodesThroughCodecs()
    {
        var orders = _client.Feed("orders.events", Codecs.Json<Order>());
        var writer = await orders.WriterAsync("ingest");
        Assert.Equal(42, writer.SessionEpoch);
        await writer.AppendAsync("order-1", new Order("order-1", 7));

        var wcl = Bodies("/v1/admin/wcl")
            .Select(b => b["script"]!.GetValue<string>()).ToArray();
        Assert.Equal(
            new[] { "CREATE WRITER ingest TO orders.events",
                    "OPEN WRITER SESSION ingest" },
            wcl);
        var appends = Bodies("/v1/writers/append");
        var body = Assert.Single(appends);
        Assert.Equal("ingest", body["writer"]!.GetValue<string>());
        Assert.Equal(42, body["session_epoch"]!.GetValue<long>());
        Assert.Equal("order-1", Text(body, "key_base64"));
        Assert.Equal("""{"id":"order-1","total":7}""", Text(body, "payload_base64"));
        Assert.False(string.IsNullOrEmpty(body["request_id"]!.GetValue<string>()));
    }

    [Fact]
    public async Task AppendAllBatchesEventsIncludingKeylessPinnedTimesMetadata()
    {
        var orders = _client.Feed("orders.events", Codecs.Json<Order>());
        var writer = await orders.WriterAsync("ingest");
        var requestId = Guid.NewGuid();
        var result = await writer.AppendAllAsync(new[]
        {
            new FeedEvent<string, Order>(new Order("order-9", 9), "order-9",
                requestId, 1697040000000000000,
                Codecs.TextMetadata(new Dictionary<string, string> { ["tenant"] = "acme" })),
            new FeedEvent<string, Order>(new Order("keyless", 0)),
        });
        _ = result;
        var batches = Bodies("/v1/writers/append-batch");
        var records = Assert.Single(batches)["records"]!.AsArray();
        Assert.Equal(2, records.Count);
        Assert.Equal(requestId.ToString(), records[0]!["request_id"]!.GetValue<string>());
        Assert.Equal("1697040000000000000", records[0]!["event_time_ns"]!.GetValue<string>());
        Assert.Equal(B64("acme"),
            records[0]!["metadata_base64"]!["tenant"]!.GetValue<string>());
        Assert.Null(records[1]!["key_base64"]);
    }

    [Fact]
    public async Task ReaderFetchesDecodesAndAcksDeliveredCursor()
    {
        _fetchPages.Enqueue($$"""
            {"records": [{{RecordJson("c1", "k1", """{"id":"o1","total":3}""")}}],
             "delivered_cursor": "c1", "acknowledged_cursor": null,
             "recommended_capacity": 8}
            """);
        var orders = _client.Feed("orders.events", Codecs.Json<Order>());
        var reader = await orders.ReaderAsync("audit", 64);
        Assert.Equal(3, reader.SessionEpoch);
        var page = await reader.FetchAsync();
        Assert.Equal("o1", Assert.Single(page.Records).Value.Id);
        Assert.Equal("c1", page.DeliveredCursor?.Value);
        await reader.AckAsync();
        Assert.Equal("c1",
            Assert.Single(Bodies("/v1/readers/ack"))["cursor"]!.GetValue<string>());
        await reader.CloseAsync();
        Assert.Equal(1, Count("/v1/readers/close"));
    }

    [Fact]
    public async Task AdHocAppendPostsEncodedFields()
    {
        var orders = _client.Feed("orders.events", Codecs.Json<Order>());
        var sessionId = Guid.NewGuid();
        await orders.AppendAsync(sessionId, 7, 3, "order-2", new Order("order-2", 11));
        var body = Assert.Single(Bodies("/v1/feeds/append"));
        Assert.Equal("orders.events", body["feed"]!.GetValue<string>());
        Assert.Equal(sessionId.ToString(), body["writer_session_id"]!.GetValue<string>());
        Assert.Equal(7, body["writer_epoch"]!.GetValue<long>());
        Assert.Equal(3, body["sequence"]!.GetValue<long>());
        Assert.Equal("order-2", Text(body, "key_base64"));
    }

    [Fact]
    public async Task ViewRebindsCodecsWithoutAWireCall()
    {
        _recordsResponse = $$"""[{{RecordJson("c1", "k1", "raw-bytes")}}]""";
        var orders = _client.Feed("orders.events", Codecs.Json<Order>());
        var raw = orders.View(Codecs.Utf8, Codecs.Bytes);
        var messages = await raw.RecordsAsync(1);
        Assert.Equal("raw-bytes",
            Encoding.UTF8.GetString(Assert.Single(messages).Value));
        Assert.Equal("orders.events", raw.Name);
    }

    [Fact]
    public async Task CustomCodecFeedsThroughEncodeAndDecode()
    {
        var xmlish = Codecs.Custom<string>(
            s => Encoding.UTF8.GetBytes($"<v>{s}</v>"),
            b => Encoding.UTF8.GetString(b).TrimStart('<', 'v', '>')
                .TrimEnd('<', '/', 'v', '>'));
        _recordsResponse = $$"""[{{RecordJson("c1", "k1", "<v>hello</v>")}}]""";
        var feed = _client.Feed("orders.events", Codecs.Utf8, xmlish);
        var messages = await feed.RecordsAsync(1);
        Assert.Equal("hello", Assert.Single(messages).Value);
    }

    [Fact]
    public async Task DecodeFailuresSurface()
    {
        _recordsResponse = $$"""[{{RecordJson("c1", "k1", "not-json")}}]""";
        var orders = _client.Feed("orders.events", Codecs.Json<Order>());
        await Assert.ThrowsAnyAsync<JsonException>(() => orders.RecordsAsync(1));
    }

    [Fact]
    public async Task DefaultFeedBindsUtf8KeysAndRawPayloads()
    {
        _recordsResponse = $$"""[{{RecordJson("c1", "k1", "raw-payload")}}]""";
        var feed = _client.Feed("orders.events");
        var m = Assert.Single(await feed.RecordsAsync(1));
        Assert.Equal("k1", m.Key);
        Assert.Equal("raw-payload", m.Value);
    }

    [Fact]
    public async Task KeylessRecordsDecodeKeyAsNull()
    {
        _recordsResponse = $$"""
            [{{RecordJson("c1", null, """{"id":"x","total":1}""")}}]
            """;
        var orders = _client.Feed("orders.events", Codecs.Json<Order>());
        var m = Assert.Single(await orders.RecordsAsync(1));
        Assert.Null(m.Key);
        Assert.Equal("x", m.Value.Id);
    }

    [Fact]
    public void CodecForInfersByType()
    {
        Assert.Same(Codecs.Utf8, Codecs.For<string>());
        Assert.Same(Codecs.Bytes, Codecs.For<byte[]>());
        Assert.Equal("héllo",
            Codecs.Utf8.Decode(Codecs.Utf8.Encode("héllo")));
        Assert.Equal(new byte[] { 1, 2, 3 },
            Codecs.Bytes.Decode(new byte[] { 1, 2, 3 }));
        var order = new Order("o", 5);
        Assert.Equal(order, Codecs.For<Order>().Decode(Codecs.For<Order>().Encode(order)));
    }
}
