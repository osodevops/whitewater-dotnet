using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using Whitewater;
using Xunit;

namespace Whitewater.Tests;

/// <summary>
/// Exact wire shapes for the typed Pipe declaration surface —
/// <see cref="PipeOperation"/>/<see cref="Extractor"/>/
/// <see cref="WindowAggregation"/> serialization plus the
/// <c>define_pipe</c> commands the client posts — and the local
/// validation that refuses malformed input before it reaches the
/// Control Plane.
/// </summary>
public class PipeDeclarationTests : IDisposable
{
    private readonly HttpListener _listener;
    private readonly WhitewaterClient _client;
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentQueue<(string Path, string Body)> _calls = new();

    public PipeDeclarationTests()
    {
        // Probe a free port, then bind — retrying the handful of times a
        // parallel test class claims the port in between.
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
            var response = """
                {
                  "request_id": "019a0000-0000-7000-8000-000000000006",
                  "revision": 9, "authority": "control_plane", "warning": null,
                  "results": [{"statement": "s", "message": "ok", "data": {}}]
                }
                """;
            var bytes = Encoding.UTF8.GetBytes(response);
            context.Response.ContentType = "application/json";
            context.Response.ContentLength64 = bytes.Length;
            await context.Response.OutputStream.WriteAsync(bytes);
            context.Response.Close();
        }
    }

    private JsonObject PostedCommand(int index)
    {
        var body = JsonNode.Parse(_calls.ElementAt(index).Body)!.AsObject();
        return body["commands"]![0]!.AsObject();
    }

    // ---- Extractor / aggregation wire shapes --------------------------------

    [Fact]
    public void ExtractorWireShapesAreExact()
    {
        Assert.Equal(
            """{"kind":"key"}""",
            Extractor.RecordKey().ToWire().ToJsonString());
        Assert.Equal(
            """{"kind":"metadata","name":"customer_id"}""",
            Extractor.Metadata("customer_id").ToWire().ToJsonString());
        Assert.Equal(
            """{"kind":"projection","extractor":"json","version":1,"field":"total"}""",
            Extractor.Json("total").ToWire().ToJsonString());
    }

    [Fact]
    public void WindowAggregationWireShapesAreExact()
    {
        Assert.Equal(
            """{"aggregation":"count"}""",
            WindowAggregation.Count().ToWire().ToJsonString());
        Assert.Equal(
            """{"aggregation":"sum","value":{"kind":"key"}}""",
            WindowAggregation.Sum(Extractor.RecordKey()).ToWire().ToJsonString());
        Assert.Equal(
            """{"aggregation":"average","value":{"kind":"metadata","name":"cost"}}""",
            WindowAggregation.Average(Extractor.Metadata("cost")).ToWire().ToJsonString());
    }

    // ---- PipeOperation wire shapes -------------------------------------------

    [Fact]
    public void ForwardOperationIsJustTheTag()
    {
        Assert.Equal(
            """{"operation":"forward"}""",
            PipeOperation.Forward().ToWire().ToJsonString());
    }

    [Fact]
    public void EnrichOperationDefaultsLatestAndForward()
    {
        Assert.Equal(
            """{"operation":"enrich","key":{"kind":"key"},"freshness":"latest","on_miss":"forward"}""",
            PipeOperation.Enrich(Extractor.RecordKey()).ToWire().ToJsonString());
    }

    [Fact]
    public void EnrichOperationSerializesKeyFreshnessNamedMiss()
    {
        var wire = PipeOperation.Enrich(
            Extractor.Metadata("customer_id"),
            EnrichFreshness.Strict,
            "by-id",
            EnrichMiss.Quarantine).ToWire();
        Assert.Equal(
            """{"operation":"enrich","key":{"kind":"metadata","name":"customer_id"},"freshness":"strict","named":"by-id","on_miss":"quarantine"}""",
            wire.ToJsonString());
    }

    [Fact]
    public void WindowOperationDefaultsEventTime()
    {
        var wire = PipeOperation.Window(
            60_000_000_000L,
            Extractor.RecordKey(),
            WindowAggregation.Count()).ToWire();
        Assert.Equal(
            """{"operation":"window","window_ns":60000000000,"key":{"kind":"key"},"aggregation":{"aggregation":"count"},"time":"event"}""",
            wire.ToJsonString());
    }

    [Fact]
    public void WindowOperationSerializesEveryDeclaredBound()
    {
        var wire = PipeOperation.Window(
            3_600_000_000_000L,
            Extractor.RecordKey(),
            WindowAggregation.Sum(Extractor.Json("total")),
            WindowTime.Ingest,
            maxLateNs: 60_000_000_000L,
            expireNs: 86_400_000_000_000L,
            keyLimit: 10_000L,
            byteLimit: 4_000_000L).ToWire();
        Assert.Equal(
            """{"operation":"window","window_ns":3600000000000,"key":{"kind":"key"},"aggregation":{"aggregation":"sum","value":{"kind":"projection","extractor":"json","version":1,"field":"total"}},"time":"ingest","max_late_ns":60000000000,"expire_ns":86400000000000,"key_limit":10000,"byte_limit":4000000}""",
            wire.ToJsonString());
    }

    [Fact]
    public void WindowOperationSerializesWatermarkAndGrace()
    {
        var wire = PipeOperation.Window(
            300_000_000_000L,
            Extractor.Json("user"),
            WindowAggregation.Average(Extractor.Json("total")),
            watermarkNs: 120_000_000_000L,
            graceNs: 30_000_000_000L).ToWire();
        Assert.Equal("window", wire["operation"]?.GetValue<string>());
        Assert.Equal(120_000_000_000L, wire["watermark_ns"]?.GetValue<long>());
        Assert.Equal(30_000_000_000L, wire["grace_ns"]?.GetValue<long>());
        Assert.Null(wire["max_late_ns"]);
        Assert.Null(wire["expire_ns"]);
    }

    // ---- Factory validation ---------------------------------------------------

    [Fact]
    public void WindowRejectsNonPositiveWindowNs()
    {
        Assert.Throws<ArgumentException>(() => PipeOperation.Window(
            0, Extractor.RecordKey(), WindowAggregation.Count()));
        Assert.Throws<ArgumentException>(() => PipeOperation.Window(
            -5, Extractor.RecordKey(), WindowAggregation.Count()));
    }

    [Fact]
    public void RequiredArgumentsAreEnforced()
    {
        Assert.Throws<ArgumentNullException>(
            () => PipeOperation.Enrich(null!));
        Assert.Throws<ArgumentNullException>(() => PipeOperation.Window(
            1, null!, WindowAggregation.Count()));
        Assert.Throws<ArgumentNullException>(() => PipeOperation.Window(
            1, Extractor.RecordKey(), null!));
        Assert.Throws<ArgumentNullException>(
            () => WindowAggregation.Sum(null!));
        Assert.Throws<ArgumentNullException>(
            () => WindowAggregation.Average(null!));
        Assert.Throws<ArgumentException>(() => Extractor.Metadata(" "));
        Assert.Throws<ArgumentException>(() => Extractor.Json(""));
    }

    // ---- define_pipe commands ---------------------------------------------------

    [Fact]
    public async Task DefineForwardPipePostsTheCopyShape()
    {
        await _client.DefineForwardPipeAsync(
            "orders.relay", "orders.audit", "orders.archive");
        var command = PostedCommand(0);
        Assert.Equal("define_pipe", command["command"]?.GetValue<string>());
        Assert.Equal("orders.relay", command["name"]?.GetValue<string>());
        Assert.Equal("orders.audit", command["subscription"]?.GetValue<string>());
        Assert.Equal("orders.archive", command["output_feed"]?.GetValue<string>());
        Assert.Null(command["output_store"]);
        Assert.Null(command["quarantine_feed"]);
        Assert.Null(command["emit_feed"]);
        Assert.Equal(
            "forward",
            command["operation"]?["operation"]?.GetValue<string>());
        Assert.Equal("/v1/admin/commands", _calls.First().Path);
    }

    [Fact]
    public async Task DefineEnrichPipePostsStoreAndQuarantine()
    {
        await _client.DefineEnrichPipeAsync(
            "orders.enrich", "orders.audit", "orders.enriched", "orders.customers",
            PipeOperation.Enrich(
                Extractor.Metadata("customer_id"),
                EnrichFreshness.Strict,
                "by-id",
                EnrichMiss.Quarantine),
            quarantineFeed: "orders.quarantine");
        var command = PostedCommand(0);
        Assert.Equal("define_pipe", command["command"]?.GetValue<string>());
        Assert.Equal("orders.enriched", command["output_feed"]?.GetValue<string>());
        Assert.Equal("orders.customers", command["output_store"]?.GetValue<string>());
        Assert.Equal("orders.quarantine", command["quarantine_feed"]?.GetValue<string>());
        var operation = command["operation"]!.AsObject();
        Assert.Equal("enrich", operation["operation"]?.GetValue<string>());
        Assert.Equal("metadata", operation["key"]?["kind"]?.GetValue<string>());
        Assert.Equal("customer_id", operation["key"]?["name"]?.GetValue<string>());
        Assert.Equal("strict", operation["freshness"]?.GetValue<string>());
        Assert.Equal("by-id", operation["named"]?.GetValue<string>());
        Assert.Equal("quarantine", operation["on_miss"]?.GetValue<string>());
    }

    [Fact]
    public async Task DefineEnrichPipeOmitsQuarantineWhenAbsent()
    {
        await _client.DefineEnrichPipeAsync(
            "orders.enrich", "orders.audit", "orders.enriched", "orders.customers",
            PipeOperation.Enrich(Extractor.RecordKey()));
        var command = PostedCommand(0);
        Assert.Null(command["quarantine_feed"]);
        Assert.Null(command["emit_feed"]);
        Assert.Null(command["operation"]?["named"]);
        Assert.Equal("latest", command["operation"]?["freshness"]?.GetValue<string>());
        Assert.Equal("forward", command["operation"]?["on_miss"]?.GetValue<string>());
    }

    [Fact]
    public async Task DefineWindowPipePostsStoreAndEmit()
    {
        await _client.DefineWindowPipeAsync(
            "orders.hourly", "orders.audit", "orders.hourlyspend",
            PipeOperation.Window(
                3_600_000_000_000L,
                Extractor.RecordKey(),
                WindowAggregation.Sum(Extractor.Json("total")),
                WindowTime.Ingest,
                maxLateNs: 60_000_000_000L,
                expireNs: 86_400_000_000_000L,
                keyLimit: 10_000L,
                byteLimit: 4_000_000L),
            emitFeed: "orders.hourlyfeed");
        var command = PostedCommand(0);
        Assert.Equal("define_pipe", command["command"]?.GetValue<string>());
        Assert.Equal("orders.hourly", command["name"]?.GetValue<string>());
        Assert.Equal("orders.audit", command["subscription"]?.GetValue<string>());
        Assert.Equal("orders.hourlyspend", command["output_store"]?.GetValue<string>());
        Assert.Equal("orders.hourlyfeed", command["emit_feed"]?.GetValue<string>());
        Assert.Null(command["output_feed"]);
        var operation = command["operation"]!.AsObject();
        Assert.Equal("window", operation["operation"]?.GetValue<string>());
        Assert.Equal(3_600_000_000_000L, operation["window_ns"]?.GetValue<long>());
        Assert.Equal("key", operation["key"]?["kind"]?.GetValue<string>());
        Assert.Equal("sum", operation["aggregation"]?["aggregation"]?.GetValue<string>());
        Assert.Equal("ingest", operation["time"]?.GetValue<string>());
        Assert.Equal(60_000_000_000L, operation["max_late_ns"]?.GetValue<long>());
        Assert.Equal(86_400_000_000_000L, operation["expire_ns"]?.GetValue<long>());
        Assert.Equal(10_000L, operation["key_limit"]?.GetValue<long>());
        Assert.Equal(4_000_000L, operation["byte_limit"]?.GetValue<long>());
    }

    [Fact]
    public async Task DefineWindowPipeOmitsEmitWhenAbsent()
    {
        await _client.DefineWindowPipeAsync(
            "orders.hourly", "orders.audit", "orders.hourlyspend",
            PipeOperation.Window(
                60_000_000_000L, Extractor.RecordKey(), WindowAggregation.Count()));
        var command = PostedCommand(0);
        Assert.Null(command["emit_feed"]);
        Assert.Equal("event", command["operation"]?["time"]?.GetValue<string>());
    }

    // ---- Client-side validation ------------------------------------------------

    [Fact]
    public async Task WrongOperationKindsAreRefused()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => _client.DefineEnrichPipeAsync(
            "orders.p", "orders.audit", "orders.out", "orders.store",
            PipeOperation.Forward()));
        await Assert.ThrowsAsync<ArgumentException>(() => _client.DefineWindowPipeAsync(
            "orders.p", "orders.audit", "orders.store",
            PipeOperation.Enrich(Extractor.RecordKey())));
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.DefineEnrichPipeAsync(
            "orders.p", "orders.audit", "orders.out", "orders.store", null!));
        Assert.Empty(_calls);
    }

    [Fact]
    public async Task InvalidNamesFailLocally()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => _client.DefineForwardPipeAsync(
            "Bad.Name", "orders.audit", "orders.archive"));
        await Assert.ThrowsAsync<ArgumentException>(() => _client.DefineForwardPipeAsync(
            "orders.relay", "bad name", "orders.archive"));
        await Assert.ThrowsAsync<ArgumentException>(() => _client.DefineForwardPipeAsync(
            "orders.relay", "orders.audit", "Orders.Archive"));
        await Assert.ThrowsAsync<ArgumentException>(() => _client.DefineWindowPipeAsync(
            "orders.p", "orders.audit", "orders.store-!",
            PipeOperation.Window(
                1, Extractor.RecordKey(), WindowAggregation.Count())));
        await Assert.ThrowsAsync<ArgumentException>(() => _client.DefineEnrichPipeAsync(
            "orders.p", "orders.audit", "orders.out", "orders.store",
            PipeOperation.Enrich(Extractor.RecordKey()),
            quarantineFeed: ".leading"));
        Assert.Empty(_calls);
    }
}
