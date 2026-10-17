namespace Whitewater;

/// <summary>One StateStore row with key and payload decoded through
/// the bound codecs — the <see cref="StoreRow"/> fields carry through;
/// only the values are typed.</summary>
public sealed record Row<TKey, TValue>(
    TKey Key,
    TValue Value,
    string? AppliedCursor = null,
    StoreFreshness? Freshness = null);

/// <summary>A named-key lookup result: typed rows plus the serving
/// replica's freshness evidence.</summary>
public sealed record RowsResult<TKey, TValue>(
    IReadOnlyList<Row<TKey, TValue>> Rows,
    StoreFreshness Freshness);

/// <summary>One bounded typed iteration page in primary-key order.
/// <paramref name="NextKey"/> continues the scan;
/// <paramref name="PinnedSeq"/> is the replica's applied journal
/// position.</summary>
public sealed record RowPage<TKey, TValue>(
    IReadOnlyList<Row<TKey, TValue>> Rows,
    TKey? NextKey,
    long PinnedSeq,
    string ServedBy);

/// <summary>
/// A codec-bound facade over one node-side StateStore — the same shape
/// the Java SDK calls <c>StateStore&lt;K,V&gt;</c>. Every
/// <see cref="GetAsync"/>/<see cref="PutAsync"/>/<see cref="IterateAsync"/>
/// serializes through the bound codecs, so application code stays in
/// its own types:
///
/// <code>
/// var profiles = client.Store("accounts.employees", Codecs.Json&lt;Profile&gt;());
/// await profiles.PutAsync("user-1", profile, new() { ["passport"] = "P111" });
/// var row = await profiles.GetAsync("user-1");   // row.Value is Profile
/// await foreach (var r in profiles.IterateAsync()) { }
/// </code>
///
/// Declare named keys on the raw spec first — Store binds codecs for
/// data operations, not declaration:
/// <code>
/// var spec = StateStore.Named("accounts.employees").AddKey("passport", unique: true);
/// var bound = await client.StateStores.DefineAsync(spec);
/// var profiles = Store.Bind(bound, Codecs.Utf8, Codecs.Json&lt;Profile&gt;());
/// </code>
/// </summary>
public sealed class Store<TKey, TValue>
{
    private readonly StateStore _store;
    private readonly ICodec<TKey> _keyCodec;
    private readonly ICodec<TValue> _valueCodec;

    private Store(StateStore store, ICodec<TKey> keyCodec, ICodec<TValue> valueCodec)
    {
        _store = store;
        _keyCodec = keyCodec;
        _valueCodec = valueCodec;
    }

    /// <summary>Rebind an already-bound byte-level StateStore under
    /// typed codecs — no wire call.</summary>
    public static Store<TKey, TValue> Bind(
        StateStore store, ICodec<TKey> keyCodec, ICodec<TValue> valueCodec)
        => new(store, keyCodec, valueCodec);

    /// <summary>The store's name.</summary>
    public string Name => _store.Name;

    /// <summary>The underlying byte-level StateStore.</summary>
    public StateStore Raw => _store;

    /// <summary>Rebind the same store under different codecs — no wire
    /// call: <c>store.View(Codecs.Utf8, Codecs.Bytes)</c> reads raw.</summary>
    public Store<TKey2, TValue2> View<TKey2, TValue2>(
        ICodec<TKey2> keyCodec, ICodec<TValue2> valueCodec)
        => new(_store, keyCodec, valueCodec);

    /// <summary>Insert or replace one row, encoding key and payload
    /// through the bound codecs. <paramref name="named"/> maps declared
    /// named keys to typed scalar values; a stable
    /// <paramref name="mutationId"/> makes retries idempotent.</summary>
    public Task<MutationResult> PutAsync(
        TKey key,
        TValue value,
        IReadOnlyDictionary<string, object?>? named = null,
        Guid? mutationId = null,
        CancellationToken ct = default)
        => _store.PutAsync(
            _keyCodec.Encode(key), _valueCodec.Encode(value),
            named, mutationId, ct);

    /// <summary>Primary-key read, decoded through the bound codecs;
    /// null when absent. <paramref name="strict"/> bounded-waits for the
    /// replica's committed frontier.</summary>
    public async Task<Row<TKey, TValue>?> GetAsync(
        TKey key,
        bool strict = false,
        int? waitMs = null,
        CancellationToken ct = default)
    {
        var row = await _store.GetAsync(_keyCodec.Encode(key), strict, waitMs, ct)
            .ConfigureAwait(false);
        return row is null ? null : DecodeRow(row);
    }

    /// <summary>Named-key lookup: every typed primary row the value
    /// points at plus the serving replica's freshness evidence.</summary>
    public async Task<RowsResult<TKey, TValue>> GetByAsync(
        string namedKey,
        object? value,
        int? limit = null,
        bool strict = false,
        int? waitMs = null,
        CancellationToken ct = default)
    {
        var result = await _store.GetByAsync(namedKey, value, limit, strict, waitMs, ct)
            .ConfigureAwait(false);
        return new RowsResult<TKey, TValue>(
            DecodeRows(result.Rows), result.Freshness);
    }

    /// <summary>Value-only primary lookup: the decoded value or null
    /// when the key is absent. <see cref="GetAsync"/> remains for the
    /// row's freshness/applied-cursor evidence.</summary>
    public async Task<TValue?> FindAsync(
        TKey key,
        bool strict = false,
        int? waitMs = null,
        CancellationToken ct = default)
    {
        var row = await GetAsync(key, strict, waitMs, ct).ConfigureAwait(false);
        return row is null ? default : row.Value;
    }

    /// <summary>Value-only named-key lookup: the first matching row's
    /// decoded value or null on no match. <see cref="GetByAsync"/>
    /// remains for every match plus freshness evidence.</summary>
    public async Task<TValue?> FindByAsync(
        string namedKey,
        object? value,
        bool strict = false,
        int? waitMs = null,
        CancellationToken ct = default)
    {
        var rows = await GetByAsync(namedKey, value, 1, strict, waitMs, ct)
            .ConfigureAwait(false);
        return rows.Rows.Count == 0 ? default : rows.Rows[0].Value;
    }

    /// <summary>Primary lookup returning <paramref name="fallback"/>
    /// when the key is absent — counters and flags become one
    /// line.</summary>
    public async Task<TValue> GetOrDefaultAsync(
        TKey key,
        TValue fallback,
        CancellationToken ct = default)
    {
        var value = await FindAsync(key, ct: ct).ConfigureAwait(false);
        return value is null ? fallback : value;
    }

    /// <summary>Remove the row and its named postings.</summary>
    public Task<MutationResult> DeleteAsync(
        TKey key, Guid? mutationId = null, CancellationToken ct = default)
        => _store.DeleteAsync(_keyCodec.Encode(key), mutationId, ct);

    /// <summary>One bounded typed iteration page in primary-key order;
    /// pass the previous page's <c>NextKey</c> (or null to start) to
    /// continue.</summary>
    public async Task<RowPage<TKey, TValue>> PageAsync(
        TKey? afterKey = default, int? limit = null, CancellationToken ct = default)
    {
        var page = await _store.PageAsync(
            afterKey is null ? null : _keyCodec.Encode(afterKey), limit, ct)
            .ConfigureAwait(false);
        return new RowPage<TKey, TValue>(
            DecodeRows(page.Rows),
            page.NextKey is null ? default : _keyCodec.Decode(page.NextKey),
            page.PinnedSeq, page.ServedBy);
    }

    /// <summary>Async-enumerate every row in primary-key order across
    /// bounded pages.</summary>
    public async IAsyncEnumerable<Row<TKey, TValue>> IterateAsync(
        int? limit = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        TKey? after = default;
        while (true)
        {
            var page = await PageAsync(after, limit, ct).ConfigureAwait(false);
            foreach (var row in page.Rows)
            {
                yield return row;
            }
            if (page.NextKey is null)
            {
                yield break;
            }
            after = page.NextKey;
        }
    }

    /// <summary>Empty the store as one journaled mutation.</summary>
    public Task<MutationResult> ClearAsync(
        Guid? mutationId = null, CancellationToken ct = default)
        => _store.ClearAsync(mutationId, ct);

    /// <summary>Tombstone the store.</summary>
    public Task<ControlExecution> DropAsync(CancellationToken ct = default)
        => _store.DropAsync(ct);

    private Row<TKey, TValue> DecodeRow(StoreRow row)
        => new(_keyCodec.Decode(row.Key), _valueCodec.Decode(row.Payload),
            row.AppliedCursor, row.Freshness);

    private IReadOnlyList<Row<TKey, TValue>> DecodeRows(IReadOnlyList<StoreRow> rows)
    {
        var decoded = new List<Row<TKey, TValue>>(rows.Count);
        foreach (var row in rows)
        {
            decoded.Add(DecodeRow(row));
        }
        return decoded;
    }
}
