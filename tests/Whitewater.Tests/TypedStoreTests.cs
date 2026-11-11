using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Whitewater;
using Xunit;

namespace Whitewater.Tests;

/// <summary>Scripted stub covering the codec-bound Store facade endpoints.</summary>
public class TypedStoreTests : IDisposable
{
    private sealed record Profile(string Id, string Name);

    private readonly HttpListener _listener;
    private readonly WhitewaterClient _client;
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentQueue<(string Path, string Body)> _calls = new();

    private const string MutateOk = """
        {
          "store_id": "00000000-0000-7000-8000-000000000007",
          "mutation_id": "00000000-0000-7000-8000-000000000008",
          "sequence": 4,
          "deduplicated": false
        }
        """;

    private static string B64(string value)
        => Convert.ToBase64String(Encoding.UTF8.GetBytes(value));

    private static string RowJson(string key, object payload, string extra = "")
        => $$"""
            {"key_base64": "{{B64(key)}}",
             "payload_base64": "{{B64(JsonSerializer.Serialize(payload))}}"{{extra}}}
            """;

    public TypedStoreTests()
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
        "/v1/admin/commands" => """
            {"request_id":"00000000-0000-7000-8000-000000000006","revision":9,"authority":"control_plane","warning":null,"results":[{"statement":"s","message":"ok","data":{}}]}
            """,
        "/v1/state-stores/mutate" or "/v1/state-stores/clear" => MutateOk,
        "/v1/state-stores/lookup" => $$"""
            {"found": true,
             "payload_base64": "{{B64("""{"Id":"user-1","Name":"Ada"}""")}}",
             "applied_cursor": "41", "applied_seq": 4, "committed_seq": 5,
             "behind": true, "served_by": "n1"}
            """,
        "/v1/state-stores/get-by" => $$"""
            {"rows": [{{RowJson("user-1", new Profile("user-1", "Ada"),
                ", \"applied_cursor\": \"9\"")}}],
             "applied_seq": 4, "committed_seq": 4, "behind": false, "served_by": "n1"}
            """,
        "/v1/state-stores/iterate" => $$"""
            {"rows": [{{RowJson("user-1", new Profile("user-1", "Ada"))}}],
             "next_key_base64": null, "pinned_seq": 4, "served_by": "n1"}
            """,
        _ => "{}",
    };

    private JsonNode[] Bodies(string path)
        => _calls.Where(c => c.Path == path)
            .Select(c => JsonNode.Parse(c.Body)!).ToArray();

    private static string Text(JsonNode? node, string name)
        => Encoding.UTF8.GetString(Convert.FromBase64String(node![name]!.GetValue<string>()));

    [Fact]
    public async Task PutEncodesKeyAndPayloadThroughTheBoundCodecs()
    {
        var profiles = _client.Store("accounts.employees", Codecs.Json<Profile>());
        await profiles.PutAsync("user-1", new Profile("user-1", "Ada"),
            new Dictionary<string, object?> { ["passport"] = "P111" });
        var op = Assert.Single(Bodies("/v1/state-stores/mutate"))["op"]!;
        Assert.Equal("put_row", op["kind"]!.GetValue<string>());
        Assert.Equal("user-1", Text(op, "key_base64"));
        Assert.Equal("""{"id":"user-1","name":"Ada"}""", Text(op, "payload_base64"));
        var named = op["named"]!.AsArray();
        Assert.Equal("passport", named[0]!["name"]!.GetValue<string>());
    }

    [Fact]
    public async Task GetDecodesTheRowAndPreservesFreshness()
    {
        var profiles = _client.Store("accounts.employees", Codecs.Json<Profile>());
        var row = await profiles.GetAsync("user-1");
        Assert.NotNull(row);
        Assert.Equal("user-1", row.Key);
        Assert.Equal(new Profile("user-1", "Ada"), row.Value);
        Assert.Equal("41", row.AppliedCursor);
        Assert.Equal("user-1",
            Text(Assert.Single(Bodies("/v1/state-stores/lookup")), "key_base64"));
    }

    [Fact]
    public async Task GetByDecodesEveryMatchedPrimaryRow()
    {
        var profiles = _client.Store("accounts.employees", Codecs.Json<Profile>());
        var result = await profiles.GetByAsync("passport", "P111");
        var row = Assert.Single(result.Rows);
        Assert.Equal("Ada", row.Value.Name);
        Assert.Equal("9", row.AppliedCursor);
    }

    [Fact]
    public async Task IterateYieldsDecodedRowsInKeyOrder()
    {
        var profiles = _client.Store("accounts.employees", Codecs.Json<Profile>());
        var visited = new List<string>();
        await foreach (var row in profiles.IterateAsync())
        {
            visited.Add($"{row.Key}:{row.Value.Name}");
        }
        Assert.Equal(new[] { "user-1:Ada" }, visited);
    }

    [Fact]
    public async Task DeleteEncodesTheKeyAndPostsDeleteRow()
    {
        var profiles = _client.Store("accounts.employees", Codecs.Json<Profile>());
        await profiles.DeleteAsync("user-1");
        var op = Bodies("/v1/state-stores/mutate").Last()["op"]!;
        Assert.Equal("delete_row", op["kind"]!.GetValue<string>());
        Assert.Equal("user-1", Text(op, "key_base64"));
    }

    [Fact]
    public async Task ViewRebindsCodecsAndStoreDefaultsToUtf8KeysRawBytes()
    {
        var profiles = _client.Store("accounts.employees", Codecs.Json<Profile>());
        var raw = profiles.View(Codecs.Utf8, Codecs.Bytes);
        var found = await raw.GetAsync("user-1");
        Assert.Equal("""{"Id":"user-1","Name":"Ada"}""",
            Encoding.UTF8.GetString(found!.Value));

        var defaults = _client.Store("accounts.employees");
        var d = await defaults.GetAsync("user-1");
        Assert.Equal("user-1", d!.Key);
        Assert.Equal("""{"Id":"user-1","Name":"Ada"}""", d.Value);
    }

    [Fact]
    public async Task BindTypesAnAlreadyBoundByteLevelFacade()
    {
        var bound = _client.StateStore("accounts.employees");
        var profiles = Store<string, Profile>.Bind(
            bound, Codecs.Utf8, Codecs.Json<Profile>());
        var row = await profiles.GetAsync("user-1");
        Assert.Equal("Ada", row!.Value.Name);
        Assert.Same(bound, profiles.Raw);
        Assert.Equal("accounts.employees", profiles.Name);
    }

    private sealed class PassportKeySet : AbstractStoreKeySet
    {
        public string? Passport
        {
            get => Get<string>("passport");
            set => Put("passport", value);
        }

        public string? Department
        {
            get => Get<string>("department");
            set => Put("department", value);
        }

        public PassportKeySet WithPassport(string v) => Put<PassportKeySet>("passport", v);
        public PassportKeySet WithDepartment(string v) => Put<PassportKeySet>("department", v);

        public static PassportKeySet With(string passport, string department)
            => new PassportKeySet().WithPassport(passport).WithDepartment(department);

        public static PassportKeySet Of(Action<PassportKeySet> builder)
        {
            var ks = new PassportKeySet();
            builder(ks);
            return ks;
        }
    }

    private static Dictionary<string, string?> NamedMap(JsonNode op)
    {
        var map = new Dictionary<string, string?>();
        foreach (var entry in op["named"]!.AsArray())
        {
            map[entry!["name"]!.GetValue<string>()] = entry["values"]![0]!["value"]?.GetValue<string>();
        }
        return map;
    }

    [Fact]
    public async Task StoreKeySetSupportsAllInlineStylesAndSingleStringFallback()
    {
        var accounts = _client.Domain("accounts");
        var spec = StateStore.Named("employees").AddUniqueKey("passport").AddNonUniqueKey("department");
        await accounts.DefineStateStoreAsync(spec);
        var profiles = accounts.Store("employees", Codecs.Json<Profile>());

        // 1A: Static factory PassportKeySet.With("P123", "eng")
        await profiles.PutAsync("emp-1", new Profile("emp-1", "Ada"), PassportKeySet.With("P123", "eng"));
        var n1 = NamedMap(Bodies("/v1/state-stores/mutate").Last()["op"]!);
        Assert.Equal("P123", n1["passport"]);
        Assert.Equal("eng", n1["department"]);

        // 1B: C# object initializer
        await profiles.PutAsync("emp-2", new Profile("emp-2", "Bob"), new PassportKeySet { Passport = "P222", Department = "ops" });
        var n2 = NamedMap(Bodies("/v1/state-stores/mutate").Last()["op"]!);
        Assert.Equal("P222", n2["passport"]);
        Assert.Equal("ops", n2["department"]);

        // 2A: Lambda on KeySet class
        await profiles.PutAsync("emp-3", new Profile("emp-3", "Cara"), PassportKeySet.Of(k => k.WithPassport("P333").WithDepartment("eng")));
        var n3 = NamedMap(Bodies("/v1/state-stores/mutate").Last()["op"]!);
        Assert.Equal("P333", n3["passport"]);
        Assert.Equal("eng", n3["department"]);

        // 2B: Store typed with KeySet class via WithKeySet
        var keyed = profiles.WithKeySet<PassportKeySet>();
        await keyed.PutAsync("emp-4", new Profile("emp-4", "Dan"), k => k.WithPassport("P444").WithDepartment("sales"));
        var n4 = NamedMap(Bodies("/v1/state-stores/mutate").Last()["op"]!);
        Assert.Equal("P444", n4["passport"]);
        Assert.Equal("sales", n4["department"]);

        // 3: Single default string fallback -> maps to first declared key ("passport")
        await profiles.PutAsync("emp-5", new Profile("emp-5", "Eve"), "P555");
        var n5 = NamedMap(Bodies("/v1/state-stores/mutate").Last()["op"]!);
        Assert.Single(n5);
        Assert.Equal("P555", n5["passport"]);
    }
}
