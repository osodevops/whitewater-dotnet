using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using Whitewater;
using Xunit;

namespace Whitewater.Tests;

/// <summary>Scripted stub covering the StateStore facade endpoints.</summary>
public class StateStoreTests : IDisposable
{
    private readonly HttpListener _listener;
    private readonly WhitewaterClient _client;
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentQueue<(string Path, string Body)> _calls = new();
    private int _iterateCalls;

    private const string CommandsOk = """
        {
          "request_id": "00000000-0000-7000-8000-000000000006",
          "revision": 9,
          "authority": "control_plane",
          "warning": null,
          "results": [{"statement": "s", "message": "ok", "data": {}}]
        }
        """;

    private const string MutateOk = """
        {
          "store_id": "00000000-0000-7000-8000-000000000007",
          "mutation_id": "00000000-0000-7000-8000-000000000008",
          "sequence": 4,
          "deduplicated": false
        }
        """;

    private static string RowJson(string key, string payload) => $$"""
        {"key_base64": "{{Convert.ToBase64String(Encoding.UTF8.GetBytes(key))}}",
         "payload_base64": "{{Convert.ToBase64String(Encoding.UTF8.GetBytes(payload))}}"}
        """;

    public StateStoreTests()
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
        "/v1/admin/commands" => CommandsOk,
        "/v1/state-stores/mutate" or "/v1/state-stores/clear" => MutateOk,
        "/v1/state-stores/lookup" => """
            {"found": true, "payload_base64": "cGF5bG9hZC0x",
             "applied_cursor": "41", "applied_seq": 4, "committed_seq": 5,
             "behind": true, "served_by": "n1"}
            """,
        "/v1/state-stores/get-by" => $$"""
            {"rows": [{{RowJson("user-1", "p1")}}, {{RowJson("user-2", "p2")}}],
             "applied_seq": 4, "committed_seq": 4, "behind": false, "served_by": "n1"}
            """,
        "/v1/state-stores/iterate" => Interlocked.Increment(ref _iterateCalls) == 1
            ? $$"""
                {"rows": [{{RowJson("a", "pa")}}],
                 "next_key_base64": "{{Convert.ToBase64String("a"u8.ToArray())}}",
                 "pinned_seq": 9, "served_by": "n1"}
                """
            : $$"""
                {"rows": [{{RowJson("b", "pb")}}], "pinned_seq": 9, "served_by": "n1"}
                """,
        _ => "{}",
    };

    private JsonNode LastBody(string path)
    {
        var call = _calls.LastOrDefault(c => c.Path == path);
        Assert.NotEqual(default, call);
        return JsonNode.Parse(call.Body)!;
    }

    public void Dispose()
    {
        _stop.Cancel();
        _listener.Stop();
        _listener.Close();
        _stop.Dispose();
    }

    [Fact]
    public async Task DefineSendsTheTypedCommandWithDeclaredNamedKeys()
    {
        var spec = StateStore.Named("accounts.employees")
            .AddKey("passport", unique: true)
            .AddKey("department");
        var store = await _client.StateStores.DefineAsync(spec);

        Assert.Equal("accounts.employees", store.Name);
        var sent = LastBody("/v1/admin/commands")["commands"]![0]!;
        Assert.Equal("define_state_store", sent["command"]!.GetValue<string>());
        Assert.Equal("accounts.employees", sent["name"]!.GetValue<string>());
        Assert.Equal("manual", sent["source"]!["kind"]!.GetValue<string>());
        Assert.Equal("passport", sent["named_keys"]![0]!["name"]!.GetValue<string>());
        Assert.True(sent["named_keys"]![0]!["unique"]!.GetValue<bool>());
        Assert.False(sent["named_keys"]![1]!["unique"]!.GetValue<bool>());
    }

    [Fact]
    public void BoundsNamedKeysAndRejectsDuplicates()
    {
        var spec = StateStore.Named("accounts.ten");
        for (var i = 0; i < 10; i++)
        {
            spec.AddKey($"k{i}");
        }
        Assert.Throws<InvalidOperationException>(() => spec.AddKey("k10"));
        Assert.Throws<ArgumentException>(
            () => StateStore.Named("accounts.d").AddKey("a").AddKey("a"));
        Assert.Throws<ArgumentException>(() => StateStore.Named("accounts.b").AddKey(" "));
    }

    [Fact]
    public async Task DefineRefusesNamedKeysOnAFeedSourcedStore()
    {
        var spec = StateStore.Named("accounts.src").AddKey("k");
        await Assert.ThrowsAsync<ArgumentException>(
            () => _client.StateStores.DefineAsync(spec, "accounts.events"));
    }

    [Fact]
    public async Task UnboundSpecRefusesRowOperations()
    {
        var spec = StateStore.Named("accounts.unbound");
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => spec.GetAsync("k"u8.ToArray()));
    }

    [Fact]
    public async Task PutEncodesTypedNamedValuesAndDeletePostsTheOp()
    {
        var store = _client.StateStore("accounts.employees");
        var put = await store.PutAsync("user-1"u8.ToArray(), "profile-1"u8.ToArray(),
            new Dictionary<string, object?>
            {
                ["passport"] = "P111",
                ["salary"] = 92000L,
                ["active"] = true,
                ["badge"] = new byte[] { 1, 2 },
                ["note"] = null,
            });
        Assert.Equal(4, put.Sequence);
        Assert.False(put.Deduplicated);

        var sent = LastBody("/v1/state-stores/mutate");
        Assert.Equal("accounts.employees", sent["store"]!.GetValue<string>());
        Assert.False(string.IsNullOrEmpty(sent["mutation_id"]!.GetValue<string>()));
        var op = sent["op"]!;
        Assert.Equal("put_row", op["kind"]!.GetValue<string>());
        Assert.Equal("user-1", Encoding.UTF8.GetString(
            Convert.FromBase64String(op["key_base64"]!.GetValue<string>())));
        var named = new Dictionary<string, JsonNode>();
        foreach (var entry in op["named"]!.AsArray())
        {
            named[entry!["name"]!.GetValue<string>()] = entry["values"]![0]!;
        }
        Assert.Equal("text", named["passport"]!["kind"]!.GetValue<string>());
        Assert.Equal("P111", named["passport"]!["value"]!.GetValue<string>());
        Assert.Equal("i64", named["salary"]!["kind"]!.GetValue<string>());
        Assert.Equal(92000L, named["salary"]!["value"]!.GetValue<long>());
        Assert.Equal("bool", named["active"]!["kind"]!.GetValue<string>());
        Assert.True(named["active"]!["value"]!.GetValue<bool>());
        Assert.Equal("bytes", named["badge"]!["kind"]!.GetValue<string>());
        Assert.Equal("AQI=", named["badge"]!["value_base64"]!.GetValue<string>());
        Assert.Equal("null", named["note"]!["kind"]!.GetValue<string>());

        await store.DeleteAsync("user-1"u8.ToArray());
        var deleteOp = LastBody("/v1/state-stores/mutate")["op"]!;
        Assert.Equal("delete_row", deleteOp["kind"]!.GetValue<string>());
    }

    [Fact]
    public async Task FloatsAreRefusedAsNamedValues()
    {
        var store = _client.StateStore("accounts.employees");
        await Assert.ThrowsAsync<ArgumentException>(() => store.PutAsync(
            "k"u8.ToArray(), "v"u8.ToArray(),
            new Dictionary<string, object?> { ["score"] = 1.5 }));
    }

    [Fact]
    public async Task GetReturnsTheRowWithFreshnessEvidence()
    {
        var store = _client.StateStore("accounts.employees");
        var row = await store.GetAsync("user-1"u8.ToArray(), strict: true, waitMs: 1000);
        Assert.NotNull(row);
        Assert.Equal("payload-1", Encoding.UTF8.GetString(row.Payload));
        Assert.Equal("41", row.AppliedCursor);
        Assert.Equal(5, row.Freshness!.CommittedSeq);
        Assert.True(row.Freshness.Behind);
        var sent = LastBody("/v1/state-stores/lookup");
        Assert.True(sent["strict"]!.GetValue<bool>());
        Assert.Equal(1000, sent["wait_ms"]!.GetValue<int>());
    }

    [Fact]
    public async Task GetByPostsTheNamedKeyLookupAndDecodesRows()
    {
        var store = _client.StateStore("accounts.employees");
        var result = await store.GetByAsync("department", "eng", limit: 50);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("user-1", Encoding.UTF8.GetString(result.Rows[0].Key));
        var sent = LastBody("/v1/state-stores/get-by");
        Assert.Equal("department", sent["named_key"]!.GetValue<string>());
        Assert.Equal("text", sent["values"]![0]!["kind"]!.GetValue<string>());
        Assert.Equal("eng", sent["values"]![0]!["value"]!.GetValue<string>());
        Assert.Equal(50, sent["limit"]!.GetValue<int>());
    }

    [Fact]
    public async Task IteratePagesUntilNoNextKey()
    {
        var store = _client.StateStore("accounts.employees");
        var keys = new List<string>();
        await foreach (var row in store.IterateAsync(limit: 1))
        {
            keys.Add(Encoding.UTF8.GetString(row.Key));
        }
        Assert.Equal(new[] { "a", "b" }, keys);
        var second = _calls.Where(c => c.Path == "/v1/state-stores/iterate").ElementAt(1);
        Assert.Equal("a", Encoding.UTF8.GetString(Convert.FromBase64String(
            JsonNode.Parse(second.Body)!["after_key_base64"]!.GetValue<string>())));
    }

    [Fact]
    public async Task ClearPostsAJournaledMutationAndDropIssuesTheCommand()
    {
        var store = _client.StateStore("accounts.employees");
        var cleared = await store.ClearAsync();
        Assert.Equal(4, cleared.Sequence);
        var sent = LastBody("/v1/state-stores/clear");
        Assert.Equal("accounts.employees", sent["store"]!.GetValue<string>());
        Assert.False(string.IsNullOrEmpty(sent["mutation_id"]!.GetValue<string>()));

        await store.DropAsync();
        var command = LastBody("/v1/admin/commands")["commands"]![0]!;
        Assert.Equal("drop", command["command"]!.GetValue<string>());
        Assert.Equal("state_store", command["kind"]!.GetValue<string>());
        Assert.Equal("accounts.employees", command["name"]!.GetValue<string>());
    }
}
