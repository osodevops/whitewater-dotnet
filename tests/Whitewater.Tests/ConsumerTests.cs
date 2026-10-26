using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json;
using Whitewater;
using Xunit;

namespace Whitewater.Tests;

/// <summary>
/// The stub holds a pending page: once queued it keeps answering
/// fetches until an ack lands — exactly like the server redelivering an
/// unacknowledged page. Covers feed.Consumer() supervision plus the
/// Store find/findBy/getOrDefault helpers.
/// </summary>
public class ConsumerTests : IDisposable
{
    private readonly HttpListener _listener;
    private readonly WhitewaterClient _client;
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentQueue<(string Path, string Body)> _calls = new();
    private readonly ConcurrentQueue<string> _queuedPages = new();
    private string? _pending;
    private int _opens;
    private int _wclStatus = 200;
    private string _wclBody = WclOk;
    private (int Status, string Body)? _openError;
    private readonly ConcurrentDictionary<string, object> _storeBodies = new();
    private readonly SemaphoreSlim _delivered = new(0);

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

    public ConsumerTests()
    {
        _listener = new HttpListener();
        var port = FreePort();
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

    private static int FreePort()
    {
        var probe = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }

    private static string RecordJson(string cursor, string payloadBase64) => $$"""
        {
          "cursor": "{{cursor}}",
          "message_id": "00000000-0000-7000-8000-000000000002",
          "producer_id": "00000000-0000-7000-8000-000000000003",
          "sequence": 7,
          "event_time_ns": "1697040000123456789",
          "ingest_time_ns": "1697040000223456789",
          "key_base64": "{{Convert.ToBase64String(Encoding.UTF8.GetBytes("k-" + cursor))}}",
          "payload_base64": "{{payloadBase64}}",
          "metadata_base64": {}
        }
        """;

    private static string PageJson(string? delivered, params string[] payloads)
    {
        var records = string.Join(",", payloads.Select((p, i) =>
            RecordJson($"c{i + 1}", Convert.ToBase64String(Encoding.UTF8.GetBytes(p)))));
        return $$"""
            {"records": [{{records}}], "delivered_cursor": {{Json(delivered)}},
             "acknowledged_cursor": null, "recommended_capacity": 0, "retry_after_ms": 0}
            """;
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
            var (status, response) = Route(path);
            var bytes = Encoding.UTF8.GetBytes(response);
            context.Response.StatusCode = status;
            context.Response.ContentType = "application/json";
            context.Response.ContentLength64 = bytes.Length;
            await context.Response.OutputStream.WriteAsync(bytes);
            context.Response.Close();
        }
    }

    private (int Status, string Body) Route(string path)
    {
        switch (path)
        {
            case "/v1/admin/wcl":
                return (_wclStatus, _wclBody);
            case "/v1/readers/open":
                Interlocked.Increment(ref _opens);
                return _openError ?? (200, Session);
            case "/v1/readers/fetch":
                _delivered.Release();
                string response;
                if (_pending is not null)
                {
                    response = _pending;         // unacked: redeliver
                }
                else if (_queuedPages.TryDequeue(out var page))
                {
                    _pending = page;
                    response = page;
                }
                else
                {
                    response = PageJson(null);
                }
                return (200, response);
            case "/v1/readers/ack":
                _pending = null;
                return (200, Session);
            case "/v1/readers/close":
                return (200, Session);
            default:
                return (200, _storeBodies.TryGetValue(path, out var storeBody)
                    ? System.Text.Json.JsonSerializer.Serialize(storeBody)
                    : "{}");
        }
    }

    private int Calls(string path) => _calls.Count(c => c.Path == path);

    private async Task WaitForAsync(Func<bool> cond, string what)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!cond() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }
        Assert.True(cond(), $"timed out waiting for {what}");
    }

    [Fact]
    public async Task MessagesDeliversEachRecordAndAcksThePage()
    {
        _queuedPages.Enqueue(PageJson("c2", "alpha", "beta"));
        var feed = _client.Feed("users.update.request");
        await using var consumer = feed.Consumer("enricher");
        var seen = new List<FeedMessage<string, string>>();

        await foreach (var msg in consumer.Messages())
        {
            seen.Add(msg);
            if (seen.Count == 2)
            {
                break;
            }
        }
        await WaitForAsync(() => Calls("/v1/readers/ack") == 1, "ack");

        Assert.Equal(
            new[] { "alpha", "beta" },
            seen.Select(m => m.Value!));
        Assert.Equal(1, Calls("/v1/readers/ack"));
        Assert.True(Calls("/v1/readers/close") > 0);
        Assert.Null(consumer.Failure);
    }

    [Fact]
    public async Task RunAsyncHandlerFailureSkipsAckAndRedelivers()
    {
        _queuedPages.Enqueue(PageJson("c1", "alpha"));
        var feed = _client.Feed("users.update.request");
        await using var consumer = feed.Consumer("enricher")
            .WithBackoff(TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(50));
        var attempts = 0;
        var errors = new List<Exception>();
        consumer.OnError(errors.Add);

        await consumer.RunAsync(async m =>
        {
            attempts++;
            if (attempts == 1)
            {
                throw new InvalidOperationException("transient handler bug");
            }
            await consumer.DisposeAsync();   // safe inside the handler
            await Task.CompletedTask;
        });

        Assert.True(attempts >= 2, $"attempts = {attempts}, want redelivery");
        Assert.Equal(1, Calls("/v1/readers/ack"));   // only the replay acks
        Assert.True(_opens >= 2, $"opens = {_opens}, want restart");
        Assert.Contains(errors, e => e is InvalidOperationException);
    }

    [Fact]
    public async Task StaleSessionReopensAndResumes()
    {
        _openError = (409, """{"error": "fenced", "code": "stale_epoch"}""");
        var feed = _client.Feed("users.update.request");
        await using var consumer = feed.Consumer("enricher")
            .WithBackoff(TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(50));
        var seen = new List<string>();

        _ = Task.Run(async () =>
        {
            while (Volatile.Read(ref _opens) == 0)
            {
                await Task.Delay(5);
            }
            _openError = null;
            _queuedPages.Enqueue(PageJson("c1", "alpha"));
        });

        await consumer.RunAsync(m =>
        {
            seen.Add(m.Value!);
            return consumer.DisposeAsync();
        });

        Assert.Equal(new[] { "alpha" }, seen);
        Assert.True(_opens >= 2, $"opens = {_opens}, want reopen");
    }

    [Fact]
    public async Task StartupRefusalFailsFast()
    {
        _wclStatus = 400;
        _wclBody = """{"error": "refused: denied", "code": "refused"}""";
        var feed = _client.Feed("users.update.request");
        await using var consumer = feed.Consumer("enricher");

        await Assert.ThrowsAsync<WhitewaterException.Refused>(
            () => consumer.RunAsync(_ => ValueTask.CompletedTask));
        Assert.Equal(0, _opens);
    }

    [Fact]
    public async Task IterationPropagatesStartupRefusal()
    {
        _wclStatus = 400;
        _wclBody = """{"error": "refused: denied", "code": "refused"}""";
        var feed = _client.Feed("users.update.request");
        await using var consumer = feed.Consumer("enricher");

        await Assert.ThrowsAsync<WhitewaterException.Refused>(async () =>
        {
            await foreach (var _ in consumer.Messages())
            {
            }
        });
    }

    [Fact]
    public async Task InvalidReaderNameFailsFastNotLoops()
    {
        var feed = _client.Feed("users.update.request");
        await using var consumer = feed.Consumer("Bad Name!");

        await Assert.ThrowsAsync<ArgumentException>(
            () => consumer.RunAsync(_ => ValueTask.CompletedTask));
        Assert.Equal(0, Calls("/v1/readers/open"));
        Assert.Empty(_calls);                    // died on local validation
    }

    [Fact]
    public async Task StoreFindFindByAndGetOrDefault()
    {
        var b64 = (string s) => Convert.ToBase64String(Encoding.UTF8.GetBytes(s));
        _storeBodies["/v1/state-stores/lookup"] = new
        {
            found = true,
            payload_base64 = b64("\"alice\""),
            applied_cursor = "00000000000000000003",
            applied_seq = 7,
            committed_seq = 7,
            behind = false,
            served_by = "storage-1",
        };
        _storeBodies["/v1/state-stores/get-by"] = new
        {
            rows = new[]
            {
                new
                {
                    key_base64 = b64("a@x.io"),
                    payload_base64 = b64("\"alice\""),
                    applied_cursor = "00000000000000000003",
                },
            },
            applied_seq = 7,
            committed_seq = 7,
            behind = false,
            served_by = "storage-1",
        };
        var profiles = _client.Store<string, string>(
            "users.profiles", Codecs.Utf8, Codecs.Json<string>());

        Assert.Equal("alice", await profiles.FindAsync("a@x.io"));
        Assert.Equal("alice", await profiles.FindByAsync("user_id", "u-1"));
        var getBy = _calls
            .Where(c => c.Path == "/v1/state-stores/get-by")
            .Select(c => JsonDocument.Parse(c.Body).RootElement)
            .First();
        Assert.Equal(1, getBy.GetProperty("limit").GetInt32());

        _storeBodies["/v1/state-stores/lookup"] = new
        {
            found = false,
            applied_seq = 7,
            committed_seq = 7,
        };
        _storeBodies["/v1/state-stores/get-by"] = new
        {
            rows = Array.Empty<object>(),
            applied_seq = 7,
            committed_seq = 7,
        };
        Assert.Null(await profiles.FindAsync("ghost"));
        Assert.Null(await profiles.FindByAsync("user_id", "ghost"));
        Assert.Equal("fallback", await profiles.GetOrDefaultAsync("ghost", "fallback"));
    }
}
