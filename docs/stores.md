# StateStore — coding guide

There is exactly one store: the **node-side StateStore** — durable,
RF3-replicated, Fjall-backed, and queryable from any Node. The SDK's
`StateStore` is a thin facade over it: it holds no rows, keeps no local
projection, and every operation is an HTTP call with freshness evidence.

```csharp
var spec = StateStore.Named("accounts.employees")
    .AddKey("passport", unique: true)        // unique: one row per value
    .AddKey("department");                   // non-unique: value -> many rows

var store = await client.StateStores.DefineAsync(spec);   // manual store
// or a bound facade for an already-declared store:
var same = client.StateStore("accounts.employees");
```

`AddKey` records declaration intent used by `DefineAsync` — named keys
are declared at creation; at most `StateStore.MaxNamedKeys` (10). Adding
a key to an existing store has no effect server-side.

## Writing

```csharp
await store.PutAsync(Utf8("user-1"), Utf8("profile-1"),
    new Dictionary<string, object?>
    {
        ["passport"] = "P111",
        ["department"] = "eng",
        ["salary"] = 92000L,
        ["active"] = true,
        ["badge"] = new byte[] { 1, 2 },
        ["note"] = null,
    });
await store.DeleteAsync(Utf8("user-1"));                  // mutationId generated
await store.DeleteAsync(Utf8("user-1"), stableMutationUuid);
```

- `PutAsync` commits one journaled `put_row` through the store's RF3
  quorum and returns a `MutationResult(StoreId, MutationId, Sequence,
  Deduplicated)`.
- Named values are **typed**: `string` -> text, integral types
  (`byte`…`long`, `ulong`/`BigInteger` within i64) -> i64, `bool` ->
  bool, `byte[]` -> bytes, `null` -> null. An enumerable value carries
  one typed value per posting component. `float`/`double`/`decimal`
  are refused — the wire carries exact i64/text/bytes.
- Named values must match the store's declared keys — the server
  refuses undeclared names. `unique` claims arbitrate
  first-committed-wins by journal order.
- `mutationId` is the idempotency identity: retry a timed-out `PutAsync`
  with the same ID and it dedupes; reuse it with different contents and
  it conflicts.

## Reading

```csharp
var row = await store.GetAsync(Utf8("user-1"));            // StoreRow | null
row.Payload;                                               // byte[]
row.Freshness;              // AppliedSeq / CommittedSeq / Behind / ServedBy

var result = await store.GetByAsync("department", "eng");
result.Rows;                              // IReadOnlyList<StoreRow> — every match
result.Freshness.ServedBy;                // which Node answered
```

`GetByAsync` is the group/list lookup: one non-unique named value
resolves to **every** primary row carrying it. `strict` + `waitMs`
bounded-waits for the serving replica to apply its committed frontier
rather than answering stale; non-strict reads still report
`Freshness.Behind` honestly.

## Iterating

```csharp
await foreach (var row in store.IterateAsync(limit: 256)) Export(row);

var page = await store.PageAsync(afterKey, 256);   // one bounded page
page.NextKey;                                    // continuation, or null
page.PinnedSeq;                                  // journal position pinned to
```

`IterateAsync`/`PageAsync` ride `/v1/state-stores/iterate` — the same
bounded, key-ordered row set replica catch-up serves — so `PinnedSeq`
is the iteration's freshness evidence.

## Lifecycle

```csharp
await store.ClearAsync();                       // one journaled ClearAll
await store.DropAsync();                        // or client.StateStores.DropAsync(name)
```

- `ClearAsync` empties the store as one durable, idempotent mutation;
  replicas sweep rows and postings in bounded batches.
- `DropAsync` tombstones the definition — the name cannot be recreated
  and every data path refuses. Rows already applied on replicas remain
  until a bounded reclaim pass.
- Feed-sourced stores: `client.StateStores.DefineAsync(spec, "accounts.events")`
  materializes committed Feed history; named keys require a manual
  store until declared projections exist.

## What a StateStore facade is **not**

- **Not a local store** — there is no in-process copy to drift from
  the Nodes. Every read goes to a replica; freshness evidence tells
  you how current it was.
- **Not unique on non-unique keys** — `GetByAsync` always returns a list.

## Errors

| Where | Type | Meaning / remedy |
|---|---|---|
| `AddKey` #11 / duplicate / blank | `InvalidOperationException` / `ArgumentException` | Declaration contract (10-key bound, distinct names). |
| `DefineAsync` spec + `source` + named keys | `ArgumentException` | Named keys need a manual store — checked before the call. |
| Data op on unbound spec | `InvalidOperationException` | `StateStore.Named` builds a spec; bind via `DefineAsync` or `client.StateStore(name)`. |
| `float`/`double`/`decimal` named value | `ArgumentException` | The wire carries exact i64 — pass an integral type. |
| Undeclared named key / missing store | `WhitewaterException` 400 | Server-side contract — fix the declaration. |
| `mutationId` reused differently | `WhitewaterException` 409 | Retry with the original contents or a fresh ID. |
| No quorum / replica down | `WhitewaterException` 503 | Retry — the journal needs a quorum to commit. |
| `strict` read can't catch up | `WhitewaterException` 409 `index_behind` | Bounded wait expired; retry or drop `strict`. |
