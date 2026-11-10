# How the .NET SDK works

Everything a caller can do enters through `WhitewaterClient` — and
every HTTP call funnels through one internal `Transport`. There are no
other network paths. JSON is `System.Text.Json` (`JsonObject`/
`JsonNode`) — no external dependencies beyond the BCL.

## Layers

```
WhitewaterClient ─┬─ WclAsync / CommandsAsync / HealthyAsync → POST /v1/admin/*
                  ├─ Domain catalog: Create/Get/Describe/Rename/Drop/List
                  │    Domain, Role, Grant                        → typed WCL builders
                  ├─ Riverbed observability: Demand*/HotKeys      → /v1/admin/commands
                  └─ Domain(name) — the ONLY front door to domain-scoped
                     resources. Every name resolves inside the Domain:
                       ├─ Feed / CreateFeedAsync        → /v1/feeds/*, /v1/readers/temporary/fetch
                       ├─ Writer(name, epoch)           → /v1/writers/append[-batch]
                       ├─ Reader(name)                  → /v1/readers/open|fetch|ack|close
                       ├─ Member(subscription)          → /v1/subscriptions/members/*
                       ├─ Store / StateStore            → /v1/statestores/*
                       ├─ Pipe / Define*PipeAsync       → /v1/admin/commands
                       └─ QueryIndexAsync               → /v1/indexes/query
       │                      │
    Transport            Wire (codec)
  (endpoints,         (request building,
   auth, errors)       response mapping)
```

All calls are `async` and accept a `CancellationToken`.

### `Transport`

Holds the endpoint list and API key. `PostAsync`/`GetAsync`
round-robin the endpoints, attach `authorization: Bearer <key>`, and
map every non-2xx to a `WhitewaterException` subtype by the wire
`code` field: `WhitewaterException.StaleSession`, `.Conflict`,
`.LeaseConflict`, `.Refused`, `.Unavailable` (unreachable/timeout),
and `.AmbiguousResult` (5xx without a code — the server may or may
not have applied the write; **retry with the same request id**,
never a new one).

### `Wire`

The only place request JSON is built and response JSON is decoded.
Keys, payloads, and metadata values are base64 `byte[]`;
`event_time_ns`/`ingest_time_ns` are `long` nanoseconds on decimal
strings. Cursors stay opaque strings.

## Resource management — the Domain is the front door

`WclAsync(script)` sends raw WCL to `POST /v1/admin/wcl`. The typed
methods are thin builders over it —
`domain.CreateFeedAsync("orders")` serializes
`CREATE FEED shop.orders` and calls `WclAsync` internally.

The public surface has exactly two scopes:

- **Riverbed scope (client):** the Domain catalog itself — create,
  get, describe, rename, drop, list Domains — plus Roles, Grants,
  demand observability, `WclAsync`, and `CommandsAsync`.
- **Domain scope:** every Feed, Writer, Reader, Subscription, StateStore,
  Index, and Pipe lives inside a Domain and is only reachable through
  it. A relative name resolves to `<domain>.<name>`; a name already
  prefixed with the Domain passes through. There is no client-level
  `createFeed`/`feed`/`reader`/`writer` — a "randomly named" resource
  cannot exist outside a Domain.

`CreateDomainAsync` returns the `Domain` handle itself, so the natural
flow is `var shop = await client.CreateDomainAsync("shop")` →
`await shop.CreateFeedAsync("orders")`.

Two validators run before interpolation, throwing
`ArgumentException` locally:

- `Dotted` — domains/feeds: `^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$`,
  ≤512 bytes.
- `Resource` — writers/readers/subscriptions/roles:
  `^[A-Za-z][A-Za-z0-9_-]{0,127}$`.

`ReaderStart` supplies `START AT` positions: `Beginning()`, `Now()`,
`Cursor(c)` (quotes escaped), `TimestampNs(ns)`.

Every management call returns `ControlExecution` — `RequestId`,
`Revision`, `Authority`, `Warning`, and one `StatementResult` per WCL
statement (`Data` is a `JsonNode`).

### WCL by example

Raw script — several statements commit as one request (pass a shared
`requestId` to dedupe a retried batch):

```csharp
await client.WclAsync("""
    CREATE DOMAIN shop;
    CREATE FEED shop.orders;
    CREATE WRITER orders.writer TO shop.orders;
    OPEN WRITER SESSION orders.writer;
    CREATE READER audit FROM shop.orders START AT BEGINNING;
    SHOW FEEDS;
    """);
```

The same calls through the typed builders — each serializes one WCL
statement and calls `WclAsync` internally:

```csharp
var shop = await client.CreateDomainAsync("shop");   // CREATE DOMAIN shop;
await shop.CreateFeedAsync("orders");                // CREATE FEED shop.orders;
await shop.CreateWriterAsync("orders.writer", "orders");
await shop.OpenWriterSessionAsync("orders.writer");  // epoch in .Data
await shop.CreateReaderAsync("audit", "orders", ReaderStart.Beginning());
await shop.FeedNamesAsync();                         // ['shop.orders', ...]
await shop.SeekReaderAsync("audit", ReaderStart.Now()); // SEEK ... TO NOW;
await shop.DropFeedAsync("orders");                  // DROP FEED shop.orders;
```

Every name is resolved inside the Domain — `shop.DropFeedAsync("orders")`
drops `shop.orders`. Passing a name already prefixed with the Domain
(`"shop.orders"`) is accepted as the same resource.
```

`client.CommandsAsync(...)` is the second door — the typed JSON
command path for what WCL does not spell. Declaring a StateStore with
named keys is the current example (there is no `DEFINE STATESTORE`
WCL statement yet):

```csharp
await client.CommandsAsync(new[]
{
    new JsonObject
    {
        ["command"] = "define_state_store",
        ["name"] = "accounts.employees",
        ["source"] = new JsonObject { ["kind"] = "manual" },
        ["named_keys"] = new JsonArray
        {
            new JsonObject { ["name"] = "passport", ["unique"] = true },
            new JsonObject { ["name"] = "department", ["unique"] = false },
        },
    },
});
```

(`domain.DefineStateStoreAsync(spec)` posts exactly this body, with
`spec.Name` resolved inside the Domain.) `DROP` is spelled in WCL for
every resource including stores:
`await client.WclAsync("DROP STATESTORE accounts.employees")` ≡
`store.DropAsync()`.

## Reading

Three shapes, in increasing durability:

1. **`feed.RecordsAsync(limit, after)`** — one-shot committed
   history.
2. **`feed.FetchAsync(options)`** — anonymous ephemeral reader;
   `tail` + `wait_ms` is a server-side long-poll returning early
   when records arrive. Continue with `NextCursor`, never the last
   record's cursor.
3. **`domain.Reader(name)`** — durable independent cursor:
   `OpenAsync` → `FetchAsync` → `AckAsync(deliveredCursor)` →
   `CloseAsync`. Progress survives restarts.

Two managed wrappers sit on top of those primitives:

- **`feed.LoadAsync(start, sink)`** — one-shot bulk read: pages the
  temporary fetch from `ReaderStart` until the frontier is drained.
  `sink` is any `Func<IReadOnlyList<FeedRecord>, Task>`.
- **`domain.Subscribe(reader, feed, start, onRecords[, onError])`** —
  a durable consume loop on a background task: provisions the Reader
  when missing, then `OpenAsync` → [`FetchAsync` → `onRecords` →
  `AckAsync`] → `CloseAsync`. Transient errors retry; `StaleSession`
  and hard refusals terminate via `onError`/`sub.Failure`.
  `await sub.DisposeAsync()` stops it.

`domain.Member(sub)` is the lease vocabulary for cooperating
consumers: `JoinAsync` → `ClaimAsync`/`RenewAsync`/`ReleaseAsync` →
`FetchAsync` → `AckAsync`.

## Writing

- **Named session**: `domain.OpenWriterSessionAsync(name)` returns a
  `session_epoch` in `Results[0].Data`; bind `domain.Writer(name, epoch)`
  and `AppendAsync` with a stable `Guid` request id per logical
  write.
- **Ad-hoc**: `feed.AppendAsync(record)` — ephemeral writer
  identity; same request-id dedup rule.

## Testing

Unit tests stub the wire with `HttpListener`, asserting serialized
statements and response mapping — no live Riverbed required.
`ConformanceRunner` replays the shared `sdk/spec/conformance`
fixtures when `WHITEWATER_CONFORMANCE_URLS` + `WHITEWATER_FIXTURE_DIR`
are set; skipped otherwise.
