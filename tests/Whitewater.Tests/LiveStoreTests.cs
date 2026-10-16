using System.Text;
using Xunit;

namespace Whitewater.Tests;

/// <summary>
/// Live store smoke against a running Riverbed — declares a named-key
/// StateStore on the Nodes, then exercises put/get/getBy/iterate/delete/
/// clear end to end.
///
/// Runs only when <c>WHITEWATER_ENDPOINTS</c> is set (comma-separated),
/// e.g. <c>http://localhost:7371,http://localhost:7372,http://localhost:7373</c>;
/// <c>WHITEWATER_API_KEY</c> defaults to the documented development key.
/// </summary>
public class LiveStoreTests
{
    private static readonly string? Endpoints =
        Environment.GetEnvironmentVariable("WHITEWATER_ENDPOINTS");

    private static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);

    [Fact]
    public async Task DeclaresAndExercisesAServerSideStateStore()
    {
        if (string.IsNullOrWhiteSpace(Endpoints))
        {
            return; // env-gated: no Riverbed configured
        }

        var endpoints = Endpoints
            .Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(raw => raw.Trim())
            .Select(raw => new Uri(raw.StartsWith("http") ? raw : "http://" + raw))
            .ToArray();
        var client = new WhitewaterClient(
            Environment.GetEnvironmentVariable("WHITEWATER_API_KEY")
                ?? "whitewater-local-development-admin-key",
            endpoints);

        var domain = $"smoke.net{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds():x}";
        Assert.True(await client.HealthyAsync());
        await client.CreateDomainAsync(domain);

        var spec = StateStore.Named($"{domain}.employees")
            .AddKey("passport", unique: true)
            .AddKey("department");
        var store = await client.StateStores.DefineAsync(spec);
        Assert.Equal($"{domain}.employees", store.Name);

        await store.PutAsync(Utf8("emp-1"), Utf8("alice"),
            new Dictionary<string, object?> { ["passport"] = "P111", ["department"] = "eng" });
        await store.PutAsync(Utf8("emp-2"), Utf8("bob"),
            new Dictionary<string, object?> { ["passport"] = "P222", ["department"] = "eng" });
        await store.PutAsync(Utf8("emp-3"), Utf8("cara"),
            new Dictionary<string, object?> { ["passport"] = "P333", ["department"] = "ops" });

        var row = await store.GetAsync(Utf8("emp-1"), strict: true, waitMs: 5000);
        Assert.NotNull(row);
        Assert.Equal("alice", Encoding.UTF8.GetString(row.Payload));

        var eng = await store.GetByAsync("department", "eng", strict: true, waitMs: 5000);
        Assert.Equal(2, eng.Rows.Count);
        Assert.True(eng.Freshness.AppliedSeq > 0);

        var keys = new SortedSet<string>();
        await foreach (var r in store.IterateAsync(limit: 1))
        {
            keys.Add(Encoding.UTF8.GetString(r.Key));
        }
        Assert.Equal(new SortedSet<string>(new[] { "emp-1", "emp-2", "emp-3" }), keys);

        await store.DeleteAsync(Utf8("emp-1"));
        Assert.Null(await store.GetAsync(Utf8("emp-1"), strict: true, waitMs: 5000));
        Assert.Single((await store.GetByAsync(
            "department", "eng", strict: true, waitMs: 5000)).Rows);

        await store.ClearAsync();
        Assert.Empty((await store.PageAsync()).Rows);
    }
}
