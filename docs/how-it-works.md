# How the .NET SDK works

Everything a caller can do enters through `WhitewaterClient` — and
every HTTP call funnels through one internal `Transport`. There are no
other network paths. JSON is `System.Text.Json` (`JsonObject`/
`JsonNode`) — no external dependencies beyond the BCL.

## Layers

```
WhitewaterClient ─┬─ WclAsync / CommandsAsync / HealthyAsync → POST /v1/admin/*
                  ├─ CreateFeedAsync / DescribeFeedAsync / … → typed WCL builders
                  ├─ Feeds   → FeedClient                    → /v1/feeds/*, /v1/readers/temporary/fetch
                  ├─ Writer(name, epoch)                     → /v1/writers/append[-batch]
                  ├─ Reader(name)                            → /v1/readers/open|fetch|ack|close
                  └─ SubscriptionMember(name)                → /v1/subscriptions/members/*
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

## Resource management — two doors, one room

`WclAsync(script)` sends raw WCL to `POST /v1/admin/wcl`. The typed
methods are thin builders over it —
`CreateFeedAsync("orders.events")` serializes
`CREATE FEED orders.events` and calls `WclAsync` internally.

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

## Reading

Three shapes, in increasing durability:

1. **`Feeds.RecordsAsync(feed, limit, after)`** — one-shot committed
   history.
2. **`Feeds.TemporaryFetchAsync(feed, options)`** — anonymous
   ephemeral reader; `tail` + `wait_ms` is a server-side long-poll
   returning early when records arrive. Continue with `NextCursor`,
   never the last record's cursor.
3. **`Reader(name)`** — durable independent cursor:
   `OpenAsync` → `FetchAsync` → `AckAsync(deliveredCursor)` →
   `CloseAsync`. Progress survives restarts.

Two managed wrappers sit on top of those primitives:

- **`Feeds.LoadAsync(feed, start, sink)`** — one-shot bulk read: pages
  `TemporaryFetchAsync` from `ReaderStart` until the frontier is
  drained. `sink` is any `Func<IReadOnlyList<FeedRecord>, Task>`.
- **`Subscribe(reader, feed, start, onRecords[, onError])`** — a durable
  consume loop on a background task: provisions the Reader when missing,
  then `OpenAsync` → [`FetchAsync` → `onRecords` → `AckAsync`] →
  `CloseAsync`. Transient errors retry; `StaleSession` and hard refusals
  terminate via `onError`/`sub.Failure`. `await sub.DisposeAsync()`
  stops it.

`SubscriptionMember(sub)` is the lease vocabulary for cooperating
consumers: `JoinAsync` → `ClaimAsync`/`RenewAsync`/`ReleaseAsync` →
`FetchAsync` → `AckAsync`.

## Writing

- **Named session**: `OpenWriterSessionAsync(name)` returns a
  `session_epoch` in `Results[0].Data`; bind `Writer(name, epoch)`
  and `AppendAsync` with a stable `Guid` request id per logical
  write.
- **Ad-hoc**: `Feeds.AppendAsync(feed, record)` — ephemeral writer
  identity; same request-id dedup rule.

## Testing

Unit tests stub the wire with `HttpListener`, asserting serialized
statements and response mapping — no live Riverbed required.
`ConformanceRunner` replays the shared `sdk/spec/conformance`
fixtures when `WHITEWATER_CONFORMANCE_URLS` + `WHITEWATER_FIXTURE_DIR`
are set; skipped otherwise.
