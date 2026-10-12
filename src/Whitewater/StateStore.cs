using System.Text.Json.Nodes;
using Whitewater.Internal;

namespace Whitewater;

/// <summary>
/// A thin facade over one node-side StateStore — the durable RF3 + Fjall
/// store on the Nodes is the only store; this object holds no rows.
/// Declare it with named keys, then put/get/delete/iterate:
///
/// <code>
/// var spec = StateStore.Named("accounts.employees")
///     .AddKey("passport", unique: true).AddKey("department");
/// var store = await client.StateStores.DefineAsync(spec);
///
/// await store.PutAsync("user-1"u8.ToArray(), "profile-1"u8.ToArray(),
///     new Dictionary&lt;string, object?&gt; { ["passport"] = "P111", ["department"] = "eng" });
/// var row = await store.GetAsync("user-1"u8.ToArray());          // StoreRow | null
/// var rows = await store.GetByAsync("department", "eng");        // every match
/// await foreach (var r in store.IterateAsync()) { }              // key-ordered pages
/// await store.ClearAsync();
/// </code>
///
/// <see cref="AddKey"/> records declaration intent used by
/// <see cref="StateStoresClient.DefineAsync"/>; it does not mutate an
/// already-declared store (named keys are declared at creation). Named
/// values are typed — string, integral types/<see cref="System.Numerics.BigInteger"/>,
/// bool, byte[], or null; an enumerable value carries one typed value
/// per posting component.
/// </summary>
public sealed class StateStore
{
    /// <summary>Named secondary keys per store; matches the server declaration budget.</summary>
    public const int MaxNamedKeys = StateStoreLimits.MaxNamedKeys;

    private readonly List<NamedKey> _declaredKeys = new();
    private Transport? _transport;

    private StateStore(Transport? transport, string name)
    {
        _transport = transport;
        Name = name;
    }

    /// <summary>An unbound declaration spec for <see cref="StateStoresClient.DefineAsync"/>.</summary>
    public static StateStore Named(string name) => new(null, WhitewaterClient.DottedName(name));

    internal static StateStore Bound(Transport transport, string name) =>
        new(transport, WhitewaterClient.DottedName(name));

    /// <summary>The store's dotted name.</summary>
    public string Name { get; }

    /// <summary>
    /// Declare a named secondary key for Define (at most
    /// <see cref="MaxNamedKeys"/>). <paramref name="extractor"/> declares
    /// how replay projects each source-Feed record to this key's values —
    /// required on feed-sourced stores, refused on manual ones.
    /// </summary>
    public StateStore AddKey(string namedKey, bool unique = false, Extractor? extractor = null)
    {
        if (string.IsNullOrWhiteSpace(namedKey))
        {
            throw new ArgumentException("named key is required", nameof(namedKey));
        }
        if (_declaredKeys.Any(k => k.Name == namedKey))
        {
            throw new ArgumentException($"duplicate named key: {namedKey}", nameof(namedKey));
        }
        if (_declaredKeys.Count >= MaxNamedKeys)
        {
            throw new InvalidOperationException(
                $"a StateStore declares at most {MaxNamedKeys} named keys");
        }
        _declaredKeys.Add(new NamedKey(namedKey, unique, extractor));
        return this;
    }

    /// <summary>The declared named keys, in declaration order.</summary>
    public IReadOnlyList<NamedKey> NamedKeys => _declaredKeys.ToArray();

    private Transport BoundTransport => _transport
        ?? throw new InvalidOperationException(
            "unbound StateStore spec — pass it to client.StateStores.DefineAsync() "
            + "or get a bound facade via client.StateStore(name)");

    internal StateStore Bind(Transport transport)
    {
        _transport = transport;
        return this;
    }

    /// <summary>
    /// Insert or replace one row. <paramref name="named"/> maps a declared
    /// named key to a typed value (or an enumerable for multi-component
    /// postings). <paramref name="mutationId"/> is the caller-owned
    /// idempotency identity; null generates one.
    /// </summary>
    public async Task<MutationResult> PutAsync(
        byte[] key,
        byte[] payload,
        IReadOnlyDictionary<string, object?>? named = null,
        Guid? mutationId = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(payload);
        var op = new JsonObject
        {
            ["kind"] = "put_row",
            ["key_base64"] = Convert.ToBase64String(key),
            ["payload_base64"] = Convert.ToBase64String(payload),
        };
        if (named is { Count: > 0 })
        {
            op["named"] = Wire.NamedValues(named);
        }
        var body = new JsonObject
        {
            ["store"] = Name,
            ["mutation_id"] = (mutationId ?? Guid.NewGuid()).ToString(),
            ["op"] = op,
        };
        return Wire.MutationResultOf(await BoundTransport
            .PostAsync("/v1/state-stores/mutate", body, ct).ConfigureAwait(false));
    }

    /// <summary>
    /// Primary-key read with freshness evidence on the returned row; null
    /// when absent. <paramref name="strict"/> bounded-waits
    /// (<paramref name="waitMs"/>) for the serving replica to apply its
    /// committed frontier rather than answering stale.
    /// </summary>
    public async Task<StoreRow?> GetAsync(
        byte[] key,
        bool strict = false,
        int? waitMs = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        var body = new JsonObject
        {
            ["store"] = Name,
            ["key_base64"] = Convert.ToBase64String(key),
            ["strict"] = strict,
        };
        if (waitMs.HasValue)
        {
            body["wait_ms"] = waitMs.Value;
        }
        var response = await BoundTransport
            .PostAsync("/v1/state-stores/lookup", body, ct).ConfigureAwait(false);
        if (response?["found"]?.GetValue<bool>() != true)
        {
            return null;
        }
        var cursor = response["applied_cursor"]?.GetValue<string>();
        return new StoreRow(
            key,
            Convert.FromBase64String(response["payload_base64"]!.GetValue<string>()),
            string.IsNullOrEmpty(cursor) ? null : cursor,
            Wire.StoreFreshnessOf(response));
    }

    /// <summary>
    /// Named-key lookup: every primary row the value points at, plus the
    /// serving replica's freshness evidence. <paramref name="value"/> is a
    /// typed scalar or an enumerable for multi-component postings.
    /// </summary>
    public async Task<StoreRows> GetByAsync(
        string namedKey,
        object? value,
        int? limit = null,
        bool strict = false,
        int? waitMs = null,
        CancellationToken ct = default)
    {
        var values = new JsonArray();
        if (value is System.Collections.IEnumerable list and not string and not byte[])
        {
            foreach (var component in list)
            {
                values.Add(Wire.IndexValue(component));
            }
        }
        else
        {
            values.Add(Wire.IndexValue(value));
        }
        var body = new JsonObject
        {
            ["store"] = Name,
            ["named_key"] = namedKey,
            ["values"] = values,
            ["strict"] = strict,
        };
        if (limit.HasValue)
        {
            body["limit"] = limit.Value;
        }
        if (waitMs.HasValue)
        {
            body["wait_ms"] = waitMs.Value;
        }
        return Wire.StoreRowsOf(await BoundTransport
            .PostAsync("/v1/state-stores/get-by", body, ct).ConfigureAwait(false));
    }

    /// <summary>Remove the row and its named postings.</summary>
    public async Task<MutationResult> DeleteAsync(
        byte[] key, Guid? mutationId = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        var body = new JsonObject
        {
            ["store"] = Name,
            ["mutation_id"] = (mutationId ?? Guid.NewGuid()).ToString(),
            ["op"] = new JsonObject
            {
                ["kind"] = "delete_row",
                ["key_base64"] = Convert.ToBase64String(key),
            },
        };
        return Wire.MutationResultOf(await BoundTransport
            .PostAsync("/v1/state-stores/mutate", body, ct).ConfigureAwait(false));
    }

    /// <summary>
    /// One bounded iteration page in primary-key order — the same row
    /// set replica transfer serves, pinned to <see cref="StorePage.PinnedSeq"/>.
    /// </summary>
    public async Task<StorePage> PageAsync(
        byte[]? afterKey = null, int? limit = null, CancellationToken ct = default)
    {
        var body = new JsonObject { ["store"] = Name };
        if (afterKey != null)
        {
            body["after_key_base64"] = Convert.ToBase64String(afterKey);
        }
        if (limit.HasValue)
        {
            body["limit"] = limit.Value;
        }
        return Wire.StorePageOf(await BoundTransport
            .PostAsync("/v1/state-stores/iterate", body, ct).ConfigureAwait(false));
    }

    /// <summary>Async-enumerate every row in primary-key order across bounded pages.</summary>
    public async IAsyncEnumerable<StoreRow> IterateAsync(
        int? limit = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        byte[]? after = null;
        while (true)
        {
            var page = await PageAsync(after, limit, ct).ConfigureAwait(false);
            foreach (var row in page.Rows)
            {
                yield return row;
            }
            if (page.NextKey == null)
            {
                yield break;
            }
            after = page.NextKey;
        }
    }

    /// <summary>Empty the store as one journaled mutation — durable and
    /// idempotent under <paramref name="mutationId"/> retries (null generates one).</summary>
    public async Task<MutationResult> ClearAsync(Guid? mutationId = null, CancellationToken ct = default)
    {
        var body = new JsonObject
        {
            ["store"] = Name,
            ["mutation_id"] = (mutationId ?? Guid.NewGuid()).ToString(),
        };
        return Wire.MutationResultOf(await BoundTransport
            .PostAsync("/v1/state-stores/clear", body, ct).ConfigureAwait(false));
    }

    /// <summary>Tombstone the store: the name cannot be recreated and
    /// every data path refuses. Applied rows remain on replicas until reclaim.</summary>
    public async Task<ControlExecution> DropAsync(CancellationToken ct = default)
    {
        var body = new JsonObject
        {
            ["request_id"] = Guid.NewGuid().ToString(),
            ["commands"] = new JsonArray(new JsonObject
            {
                ["command"] = "drop",
                ["kind"] = "state_store",
                ["name"] = Name,
            }),
        };
        return Wire.ControlExecution(await BoundTransport
            .PostAsync("/v1/admin/commands", body, ct).ConfigureAwait(false));
    }
}
