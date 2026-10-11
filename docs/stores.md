# RecordStore and LoadStore — coding guide

A `RecordStore` is a named, thread-safe, in-memory projection of Feed
records. Fill it once with `client.LoadStore(store).FromFeedAsync`
(bulk history) and keep it warm with `client.Subscribe` (live tail) —
the Whitewater equivalent of a local materialized view.

## Store anatomy

Two structures per store:

- **Primary map** — `primary key -> FeedRecord`. One row per key; a new
  row for an existing key replaces it.
- **Named-key posting maps** — one per declared named key, conceptually
  `employee-store.passport`: `named value -> set of primary keys`.
  Named keys are **not unique** — many rows may share one named value,
  so lookups return a list.

```csharp
var store = RecordStore.Named("employee-store")
    .AddKey("passport")           // declare before use
    .AddKey("department");        // at most RecordStore.MaxNamedKeys (10)
```

`AddKey` returns the store for chaining. An eleventh key throws
`InvalidOperationException`; blank names throw `ArgumentException`.

## Loading history: `client.LoadStore(store).FromFeedAsync(...)`

```csharp
var loaded = await client.LoadStore(store).FromFeedAsync(
    "hr.employees",
    ReaderStart.Beginning(),            // or .Now(), .Cursor(c), .TimestampNs(ns)
    (s, record) => s.Upsert(record)
        .WithKey(record.Key!)                          // primary key
        .WithNamedKey("passport", record.Metadata["pp"])
        .WithNamedKey("department", "eng"));           // string overloads are UTF-8
```

Semantics, in order:

1. Pages `TemporaryFetchAsync` from `start` until the frontier is
   drained — a **one-shot bounded read**, not a tail. The optional
   `CancellationToken` cancels mid-read.
2. For every record the projection runs `s.Upsert(record)`.
3. `WithKey(byte[])` chooses the primary key. Defaults to `record.Key`;
   pass anything — a composite `"dept:emp-1"` — and the row re-keys
   (old postings are stripped first). `WithKey(string)` is UTF-8; null
   falls back to the record's own key/cursor.
4. `WithNamedKey(name, byte[])` attaches one named-key value; call it
   once per declared key you want indexed. An **undeclared** named key
   throws `ArgumentException` — a projection bug fails the load, never
   silently writes an unindexed row.
5. Returns the count of records read (not rows written — re-keys
   replace, they don't duplicate).

Skip `WithNamedKey` and the row lands in the primary map only — fully
readable, just not indexable by that named key.

`client.Feeds.LoadAsync(feed, start, sink)` remains the lighter "dump
history" path; `sink` may be the store itself (rows key on
`record.Key`, cursor fallback for unkeyed records) or a
`Func<IReadOnlyList<FeedRecord>, Task>`.

## Keeping the store warm: `Subscribe`

```csharp
await using var sub = client.Subscribe("hr-view", "hr.employees",
    ReaderStart.Now(),                              // only new records
    batch => {                                      // per fetched page
        foreach (var r in batch)
        {
            store.Upsert(r)
                .WithKey(r.Key!)
                .WithNamedKey("department", r.Metadata["department"]);
        }
        return Task.CompletedTask;
    },
    error => Console.Error.WriteLine($"subscription ended: {error}"));
// ...the loop runs on a background task until DisposeAsync()
```

Delivery is **at-least-once**: records land in `onRecords`, then the
loop acks `deliveredCursor`. A crash between callback and ack replays —
make the projection idempotent (upsert semantics already are).

## Reading the store

```csharp
store.Get("emp-1");                    // FeedRecord? by primary key (string = UTF-8)
store.Get(keyBytes);                   // byte[] overload
store.Get("passport", "P123");         // IReadOnlyList<FeedRecord> — every match
store.Get("undeclared", "x");          // throws ArgumentException
store.Count;                           // row count
store.NamedKeys;                       // declared keys, in order
store.Name;                            // "employee-store"
foreach (var e in store) { /* e.PrimaryKey (byte[]), e.Record */ }
store.ForEach(e => Export(e.Record));  // stable snapshot iteration
```

## Mutating outside a load

```csharp
store.Put(record);                     // primary-key upsert, no named keys
store.PutAll(batch);                   // subscribe-ready sink shape
store.Upsert(record).WithKey("custom");// same builder as projections
store.Delete("emp-1");                 // removes row + all its postings
store.Clear();                         // empties everything
```

`Delete` and `Clear` strip named-key postings too — a deleted row never
lingers in a `Get(namedKey, value)` result.

## Copying and parallel work

```csharp
var replica = RecordStore.Named("employee-backup");
store.Copy(replica);                   // rows + named postings; declares
                                       // missing keys on the target
store.ForEachParallel(3, entry => Rebuild(entry));
```

`ForEachParallel` is a `Parallel.ForEach` bound to
`MaxDegreeOfParallelism = threads` — real thread-pool parallelism over a
stable snapshot. `threads < 1` throws `ArgumentOutOfRangeException`.
The callback runs concurrently on multiple threads — keep it free of
un-synchronized shared state (the store itself is thread-safe).

## Error handling reference

| Where | Type | Meaning / remedy |
|---|---|---|
| `AddKey` #11 | `InvalidOperationException` | Store key budget (10) exceeded — declare fewer keys. |
| `AddKey`/`Get`/`WithNamedKey` undeclared or blank | `ArgumentException` | Programming error — declare the key or fix the name. |
| `FromFeedAsync` mid-read | `WhitewaterException` (usually `Unavailable`) | Transport refused a page; retry the call — re-upserts are idempotent. |
| `Subscribe` transient | retried internally | `Unavailable`/`AmbiguousResult` backoff inside the loop. |
| `Subscribe` terminal | `onError` + `sub.Failure` | `WhitewaterException.StaleSession` (another client opened your Reader — pick a unique name or take over deliberately), `.Refused`, `.Conflict`. The loop is dead; inspect, fix, re-subscribe. |

Catch specific subtypes — `catch (WhitewaterException.StaleSession)` —
and inspect `Status`/`Code` for wire detail.

## What a RecordStore is **not**

- **Not durable** — it dies with the process. Rebuild is cheap:
  `FromFeedAsync` from `ReaderStart.Beginning()` (or a snapshot Cursor).
- **Not a server StateStore** — the server-side replicated store model
  is a separate, later seam. This is the client-side projection.
- **Not unique on named keys** — `Get(namedKey, value)` always returns a
  list, even when it happens to hold one row.
