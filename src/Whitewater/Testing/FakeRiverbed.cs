using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json.Nodes;

namespace Whitewater.Testing;

/// <summary>
/// An in-process Riverbed double for application unit tests — a real
/// loopback HTTP endpoint speaking the v1 wire contract, so code under
/// test exercises the unmodified <see cref="WhitewaterClient"/>:
///
/// <code>
/// await using var riverbed = new FakeRiverbed();
/// var client = new WhitewaterClient("test-key", riverbed.Endpoints);
/// var feed = client.Feed("orders.events", Codecs.Json&lt;Order&gt;());
/// var writer = await feed.WriterAsync("ingest");
/// await writer.AppendAsync("order-1", order);
/// </code>
///
/// Modeled semantics, not canned replies:
/// <list type="bullet">
/// <item>Declarations flow through the same WCL/commands surface the
/// client uses — <c>CREATE WRITER … TO</c>/<c>CREATE READER … FROM</c>
/// bind sessions to Feeds; duplicates refuse with an "exists" message
/// the facades already handle.</item>
/// <item>Appends commit ordered records with opaque cursors and uuid
/// message_ids; (writer, session_epoch, request_id) retries return the
/// original receipt with <c>deduplicated: true</c>; a superseded
/// session_epoch answers 409 <c>stale_epoch</c> (the SDK's
/// <see cref="WhitewaterException.StaleSession"/>).</item>
/// <item>Reader pages are at-least-once: a fetched page must be acked;
/// closing without acking replays it on the next session.</item>
/// <item>StateStores keep real rows and named-key postings with
/// mutation_id dedup; lookups report freshness evidence.</item>
/// <item>Nanoseconds travel as decimal strings; unknown routes answer
/// an SDK-decodable 404.</item>
/// </list>
///
/// Feeds materialize on first reference (the typed facade's
/// WriterAsync/ReaderAsync never CREATE FEED first); Writers, Readers,
/// and StateStores must be declared through the client — appending to
/// an undeclared writer or mutating a missing store refuses, matching
/// the live contract. Deterministic and single-Noded: no quorum waits,
/// no long-poll delays.
/// </summary>
public sealed class FakeRiverbed : IDisposable, IAsyncDisposable
{
    /// <summary>One request the fake served — the recorded wire traffic
    /// for assertions.</summary>
    public sealed record RecordedRequest(string Method, string Path, string Body);

    private const int PortAttempts = 16;
    private const string NodeId = "fake-node-1";

    private readonly HttpListener _listener;
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _serving;
    private readonly ConcurrentQueue<RecordedRequest> _requests = new();

    private readonly Dictionary<string, FeedState> _feeds = new();
    private readonly Dictionary<string, WriterState> _writers = new();
    private readonly Dictionary<string, ReaderState> _readers = new();
    private readonly Dictionary<string, StoreState> _stores = new();
    private readonly Dictionary<string, DomainState> _domains = new();
    private readonly Dictionary<string, Guid> _roles = new();
    private readonly Dictionary<string, SubscriptionState> _subscriptions = new();
    private readonly Dictionary<string, (int Status, string Body)> _adminDedup = new();
    private readonly Dictionary<string, AppendReceipt> _writerDedup = new();
    private readonly Dictionary<string, AppendReceipt> _feedDedup = new();
    private readonly Dictionary<Guid, AdhocWriter> _adhocWriters = new();
    private long _revision;
    private long _clock;

    /// <summary>Bind a loopback port and start serving immediately.</summary>
    public FakeRiverbed()
    {
        _listener = BindLoopback(out var endpoint);
        Endpoints = new[] { endpoint };
        _serving = Task.Run(ServeAsync);
    }

    /// <summary>The Node endpoints to hand to
    /// <see cref="WhitewaterClient"/>: <c>new WhitewaterClient(key,
    /// riverbed.Endpoints)</c>.</summary>
    public IReadOnlyList<Uri> Endpoints { get; }

    /// <summary>The single endpoint URI, for one-line client wiring.</summary>
    public Uri Endpoint => Endpoints[0];

    /// <summary>The wire traffic served so far, in arrival order.</summary>
    public IReadOnlyList<RecordedRequest> Requests => _requests.ToArray();

    // ---- lifecycle -------------------------------------------------------

    /// <summary>Stop the listener and let the serve loop exit.</summary>
    public void Dispose()
    {
        _stop.Cancel();
        _listener.Stop();
        _listener.Close();
        _stop.Dispose();
    }

    /// <summary>Stop the listener and await the serve loop.</summary>
    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        _listener.Stop();
        _listener.Close();
        try
        {
            await _serving.ConfigureAwait(false);
        }
        catch
        {
            // The serve loop only dies on listener teardown.
        }
        _stop.Dispose();
    }

    /// <summary>Probe-then-bind with retries: under test parallelism a
    /// probed port can be claimed between the probe and the bind.</summary>
    private static HttpListener BindLoopback(out Uri endpoint)
    {
        for (var attempt = 1; ; attempt++)
        {
            var probe = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
            probe.Start();
            var port = ((IPEndPoint)probe.LocalEndpoint).Port;
            probe.Stop();
            var listener = new HttpListener();
            listener.Prefixes.Add($"http://127.0.0.1:{port}/");
            try
            {
                listener.Start();
                endpoint = new Uri($"http://127.0.0.1:{port}");
                return listener;
            }
            catch (HttpListenerException) when (attempt < PortAttempts)
            {
                listener.Close();
            }
        }
    }

    private async Task ServeAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            HttpListenerContext context;
            try
            {
                context = await _listener.GetContextAsync().ConfigureAwait(false);
            }
            catch
            {
                return;
            }
            try
            {
                await HandleAsync(context).ConfigureAwait(false);
            }
            catch
            {
                // A dropped connection must not kill the serve loop.
            }
        }
    }

    private async Task HandleAsync(HttpListenerContext context)
    {
        var request = context.Request;
        var path = request.Url?.AbsolutePath ?? "";
        using var reader = new StreamReader(request.InputStream, Encoding.UTF8);
        var body = await reader.ReadToEndAsync().ConfigureAwait(false);
        _requests.Enqueue(new RecordedRequest(
            request.HttpMethod, request.Url?.PathAndQuery ?? path, body));

        int status;
        string response;
        try
        {
            JsonNode? parsed = body.Length > 0 ? JsonNode.Parse(body) : null;
            (status, response) = Route(
                request.HttpMethod, path, request.Url?.Query ?? "", parsed);
        }
        catch (System.Text.Json.JsonException)
        {
            status = 400;
            response = ErrorBody("request body is not valid JSON", null);
        }
        catch (Refusal refusal)
        {
            status = refusal.Status;
            response = refusal.Body();
        }
        catch (Exception error)
        {
            status = 500;
            response = ErrorBody($"fake riverbed fault: {error.Message}", null);
        }

        var bytes = Encoding.UTF8.GetBytes(response);
        context.Response.StatusCode = status;
        context.Response.ContentType = "application/json";
        context.Response.ContentLength64 = bytes.Length;
        await context.Response.OutputStream.WriteAsync(bytes).ConfigureAwait(false);
        context.Response.Close();
    }

    private static string ErrorBody(string message, string? code)
    {
        var body = new JsonObject { ["error"] = message };
        if (code != null)
        {
            body["code"] = code;
        }
        return body.ToJsonString();
    }

    /// <summary>A refusal the wire carries as a non-2xx error envelope.</summary>
    private sealed class Refusal : Exception
    {
        public Refusal(int status, string message, string? code = null)
            : base(message)
        {
            Status = status;
            Code = code;
        }

        public int Status { get; }
        public string? Code { get; }
        public string Body() => ErrorBody(Message, Code);

        public static Refusal BadRequest(string message) => new(400, message);
        public static Refusal NotFound(string message) => new(404, message, "not_found");
        public static Refusal Conflict(string message, string? code = null) => new(409, message, code);
        public static Refusal StaleEpoch(string message) => new(409, message, "stale_epoch");
    }

    // ---- routing ---------------------------------------------------------

    private (int Status, string Body) Route(
        string method, string path, string query, JsonNode? body)
    {
        if (method == "GET" && path == "/health")
        {
            return (200, Health().ToJsonString());
        }
        if (method == "GET" && path == "/v1/feeds/records")
        {
            return (200, FeedRecords(query));
        }
        if (method != "POST")
        {
            throw Refusal.NotFound($"unknown route: {method} {path}");
        }
        return path switch
        {
            "/v1/admin/wcl" => Wcl(body),
            "/v1/admin/commands" => Commands(body),
            "/v1/writers/append" => (200, WriterAppend(Require(body)).ToJsonString()),
            "/v1/writers/append-batch" => (200, WriterAppendBatch(Require(body)).ToJsonString()),
            "/v1/feeds/append" => (200, FeedAppend(Require(body)).ToJsonString()),
            "/v1/readers/open" => (200, ReaderOpen(Require(body)).ToJsonString()),
            "/v1/readers/fetch" => (200, ReaderFetch(Require(body)).ToJsonString()),
            "/v1/readers/ack" => (200, ReaderAck(Require(body)).ToJsonString()),
            "/v1/readers/close" => (200, ReaderClose(Require(body)).ToJsonString()),
            "/v1/readers/temporary/fetch" => (200, TemporaryFetch(Require(body)).ToJsonString()),
            "/v1/state-stores/mutate" => (200, StoreMutate(Require(body)).ToJsonString()),
            "/v1/state-stores/lookup" => (200, StoreLookup(Require(body)).ToJsonString()),
            "/v1/state-stores/get-by" => (200, StoreGetBy(Require(body)).ToJsonString()),
            "/v1/state-stores/iterate" => (200, StoreIterate(Require(body)).ToJsonString()),
            "/v1/state-stores/clear" => (200, StoreClear(Require(body)).ToJsonString()),
            _ => throw Refusal.NotFound($"unknown route: {method} {path}"),
        };
    }

    private static JsonObject Require(JsonNode? body)
        => body as JsonObject
            ?? throw Refusal.BadRequest("request body must be a JSON object");

    private static JsonObject Health() => new()
    {
        ["status"] = "ok",
        ["version"] = "fake-riverbed",
        ["node_id"] = NodeId,
        ["members"] = 1,
        ["protocol"] = new JsonObject { ["major"] = 1, ["minor"] = 0 },
        ["limits"] = new JsonObject
        {
            ["max_frame_bytes"] = 67_108_864,
            ["max_committed_read_bytes"] = 268_435_456,
            ["max_logical_read_frames"] = 4_096,
            ["max_logical_read_ranges"] = 64,
            ["subscription_member_fetch_max_limit"] = 256,
            ["subscription_member_max_lease_ticks"] = 600,
        },
    };

    // ---- catalog state -----------------------------------------------------

    private sealed class StoredRecord
    {
        public string Cursor = "";
        public Guid MessageId;
        public Guid ProducerId;
        public long Sequence;
        public long EventTimeNs;
        public long IngestTimeNs;
        public byte[]? Key;
        public byte[] Payload = Array.Empty<byte>();
        public Dictionary<string, byte[]> Metadata = new();
    }

    private sealed class FeedState
    {
        public Guid Id = Guid.NewGuid();
        public long CreatedAtNs;
        public List<StoredRecord> Records = new();
        /// <summary>Opaque cursor -> the index of the record that follows
        /// it (a record cursor maps to its own index + 1; boundary tokens
        /// minted for delivered/next cursors map to a position).</summary>
        public Dictionary<string, long> Cursors = new();
    }

    private sealed class WriterState
    {
        public Guid Id = Guid.NewGuid();
        public long CreatedAtNs;
        public FeedState Feed = new();
        public long Epoch;
        public Guid SessionId;
        public long Sequence;
    }

    private sealed class ReaderState
    {
        public Guid Id = Guid.NewGuid();
        public long CreatedAtNs;
        public FeedState Feed = new();
        public string StartKind = "beginning";
        public long AckIndex;
        public long DeliverIndex;
        public long Epoch;
        public int Capacity = 64;
        public bool Open;
    }

    private sealed class DomainState
    {
        public Guid Id = Guid.NewGuid();
        public long CreatedAtNs;
    }

    private sealed class SubscriptionState
    {
        public Guid Id = Guid.NewGuid();
        public long CreatedAtNs;
        public string Feed = "";
        public string StartKind = "beginning";
    }

    private sealed class AdhocWriter
    {
        public long Epoch;
        public long Sequence;
    }

    private sealed record AppendReceipt(Guid MessageId, string Cursor);

    private sealed class StoreRowState
    {
        public byte[] Payload = Array.Empty<byte>();
        /// <summary>Named key -> typed-value posting keys this row claims.</summary>
        public Dictionary<string, List<string>> Named = new();
        public long AppliedSeq;
    }

    private sealed record MutationReceipt(long Sequence, string Canonical);

    private sealed class StoreState
    {
        public Guid Id = Guid.NewGuid();
        public long CreatedAtNs;
        public bool Manual = true;
        public Dictionary<string, bool> NamedKeys = new();
        public SortedDictionary<byte[], StoreRowState> Rows =
            new(BytesComparer.Instance);
        /// <summary>named key -> typed value -> owning row keys.</summary>
        public Dictionary<string, Dictionary<string, List<byte[]>>> Postings = new();
        public Dictionary<string, MutationReceipt> Mutations = new();
        public long Seq;
    }

    private sealed class BytesComparer : IComparer<byte[]>
    {
        public static readonly BytesComparer Instance = new();

        public int Compare(byte[]? x, byte[]? y)
        {
            if (ReferenceEquals(x, y))
            {
                return 0;
            }
            if (x is null) return -1;
            if (y is null) return 1;
            var length = Math.Min(x.Length, y.Length);
            for (var i = 0; i < length; i++)
            {
                var diff = x[i].CompareTo(y[i]);
                if (diff != 0)
                {
                    return diff;
                }
            }
            return x.Length.CompareTo(y.Length);
        }
    }

    private DomainState DomainFor(string name)
    {
        if (!_domains.TryGetValue(name, out var domain))
        {
            domain = new DomainState { CreatedAtNs = NowNs() };
            _domains[name] = domain;
        }
        return domain;
    }

    /// <summary>The <c>space_id</c> a resource name falls under — the
    /// fake scopes by first dotted segment, matching its domain
    /// derivation.</summary>
    private string? SpaceIdOf(string resourceName)
    {
        var first = resourceName.Split('.')[0];
        return _domains.TryGetValue(first, out var domain) ? domain.Id.ToString() : null;
    }

    private FeedState FeedFor(string name)
    {
        if (!_feeds.TryGetValue(name, out var feed))
        {
            feed = new FeedState { CreatedAtNs = NowNs() };
            _feeds[name] = feed;
            // Declarations materialize their owning Domain on first
            // reference — the typed facade never declares it itself.
            DomainFor(name.Split('.')[0]);
        }
        return feed;
    }

    private long NowNs()
    {
        var wall = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        var next = Math.Max(wall, _clock + 1);
        _clock = next;
        return next;
    }

    /// <summary>Mint an opaque cursor at <paramref name="position"/> —
    /// the records that follow it.</summary>
    private static string CursorAt(FeedState feed, long position)
    {
        var cursor = $"cur:{feed.Id:N}:{position:x16}:{Guid.NewGuid():N}";
        feed.Cursors[cursor] = position;
        return cursor;
    }

    private static long ResolveCursor(FeedState feed, string cursor)
        => feed.Cursors.TryGetValue(cursor, out var position)
            ? position
            : throw Refusal.BadRequest($"unknown cursor: {cursor}");

    private static JsonObject RecordJson(StoredRecord record)
    {
        var metadata = new JsonObject();
        foreach (var (name, value) in record.Metadata)
        {
            metadata[name] = Convert.ToBase64String(value);
        }
        var node = new JsonObject
        {
            ["cursor"] = record.Cursor,
            ["message_id"] = record.MessageId.ToString(),
            ["producer_id"] = record.ProducerId.ToString(),
            ["sequence"] = record.Sequence,
            ["event_time_ns"] = record.EventTimeNs
                .ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["ingest_time_ns"] = record.IngestTimeNs
                .ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["payload_base64"] = Convert.ToBase64String(record.Payload),
            ["metadata_base64"] = metadata,
        };
        if (record.Key != null)
        {
            node["key_base64"] = Convert.ToBase64String(record.Key);
        }
        return node;
    }

    private static JsonObject Feedback() => new()
    {
        ["recommended_batch_count"] = 128,
        ["recommended_batch_bytes"] = 1_048_576,
        ["pressure"] = 0.0,
        ["retry_after_ms"] = 0,
        ["max_frame_bytes"] = 67_108_864,
        ["hot_key"] = false,
    };

    // ---- writers ---------------------------------------------------------

    private StoredRecord CommitRecord(
        FeedState feed,
        Guid producerId,
        long sequence,
        byte[]? key,
        byte[] payload,
        long? eventTimeNs,
        JsonNode? metadataNode)
    {
        var record = new StoredRecord
        {
            Cursor = "",
            MessageId = Guid.NewGuid(),
            ProducerId = producerId,
            Sequence = sequence,
            EventTimeNs = eventTimeNs ?? NowNs(),
            IngestTimeNs = NowNs(),
            Key = key,
            Payload = payload,
        };
        if (metadataNode is JsonObject entries)
        {
            foreach (var entry in entries)
            {
                record.Metadata[entry.Key] =
                    Convert.FromBase64String(entry.Value!.GetValue<string>());
            }
        }
        record.Cursor = CursorAt(feed, feed.Records.Count + 1);
        feed.Records.Add(record);
        return record;
    }

    private static byte[]? OptionalBytes(JsonObject body, string field)
        => body[field]?.GetValue<string>() is { } encoded
            ? Convert.FromBase64String(encoded)
            : null;

    /// <summary>Nanoseconds travel as decimal strings on the wire; a
    /// bare number is accepted too for hand-rolled requests.</summary>
    private static long? OptionalNanos(JsonObject body, string field)
    {
        var node = body[field];
        if (node is JsonValue value)
        {
            if (value.TryGetValue<long>(out var number))
            {
                return number;
            }
            if (value.TryGetValue<string>(out var text)
                && long.TryParse(text, out var nanos))
            {
                return nanos;
            }
        }
        return null;
    }

    private WriterState RequireWriter(JsonObject body)
    {
        var name = body["writer"]?.GetValue<string>() ?? "";
        return _writers.TryGetValue(name, out var writer)
            ? writer
            : throw Refusal.BadRequest($"unknown writer: {name}");
    }

    private void GuardWriterEpoch(WriterState writer, JsonObject body)
    {
        var epoch = body["session_epoch"]?.GetValue<long>() ?? -1;
        if (writer.Epoch == 0 || epoch != writer.Epoch)
        {
            throw Refusal.StaleEpoch(
                $"writer session epoch {epoch} is fenced — the session was superseded");
        }
    }

    private JsonObject AppendResultJson(StoredRecord record, bool deduplicated) => new()
    {
        ["message_id"] = record.MessageId.ToString(),
        ["cursor"] = record.Cursor,
        ["deduplicated"] = deduplicated,
        ["feedback"] = Feedback(),
    };

    private static JsonObject AppendResultJson(AppendReceipt receipt, bool deduplicated) => new()
    {
        ["message_id"] = receipt.MessageId.ToString(),
        ["cursor"] = receipt.Cursor,
        ["deduplicated"] = deduplicated,
        ["feedback"] = Feedback(),
    };

    private JsonObject WriterAppend(JsonObject body)
    {
        var writer = RequireWriter(body);
        GuardWriterEpoch(writer, body);
        var requestId = body["request_id"]?.GetValue<string>() ?? "";
        var dedupKey = $"{body["writer"]!.GetValue<string>()}|{writer.Epoch}|{requestId}";
        if (_writerDedup.TryGetValue(dedupKey, out var receipt))
        {
            return AppendResultJson(receipt, deduplicated: true);
        }
        var record = CommitRecord(
            writer.Feed,
            writer.SessionId,
            ++writer.Sequence,
            OptionalBytes(body, "key_base64"),
            Convert.FromBase64String(
                body["payload_base64"]?.GetValue<string>() ?? ""),
            OptionalNanos(body, "event_time_ns"),
            body["metadata_base64"]);
        _writerDedup[dedupKey] = new AppendReceipt(record.MessageId, record.Cursor);
        return AppendResultJson(record, deduplicated: false);
    }

    private JsonObject WriterAppendBatch(JsonObject body)
    {
        if (body["records"] is not JsonArray records)
        {
            throw Refusal.BadRequest("append-batch requires a records array");
        }
        // Fence-check the whole batch before committing any record —
        // a stale epoch refuses atomically rather than half-applying.
        var writers = new List<WriterState>(records.Count);
        foreach (var item in records)
        {
            var entry = Require(item);
            var writer = RequireWriter(entry);
            GuardWriterEpoch(writer, entry);
            writers.Add(writer);
        }
        var results = new JsonArray();
        for (var i = 0; i < records.Count; i++)
        {
            var entry = (JsonObject)records[i]!;
            var writer = writers[i];
            var requestId = entry["request_id"]?.GetValue<string>() ?? "";
            var dedupKey = $"{entry["writer"]!.GetValue<string>()}|{writer.Epoch}|{requestId}";
            if (_writerDedup.TryGetValue(dedupKey, out var receipt))
            {
                results.Add(AppendResultJson(receipt, deduplicated: true));
                continue;
            }
            var record = CommitRecord(
                writer.Feed,
                writer.SessionId,
                ++writer.Sequence,
                OptionalBytes(entry, "key_base64"),
                Convert.FromBase64String(
                    entry["payload_base64"]?.GetValue<string>() ?? ""),
                OptionalNanos(entry, "event_time_ns"),
                entry["metadata_base64"]);
            _writerDedup[dedupKey] = new AppendReceipt(record.MessageId, record.Cursor);
            results.Add(AppendResultJson(record, deduplicated: false));
        }
        return new JsonObject { ["results"] = results, ["feedback"] = Feedback() };
    }

    private JsonObject FeedAppend(JsonObject body)
    {
        var name = body["feed"]?.GetValue<string>() ?? "";
        var feed = FeedFor(name);
        var requestId = body["request_id"]?.GetValue<string>() ?? "";
        var dedupKey = $"{name}|{requestId}";
        if (_feedDedup.TryGetValue(dedupKey, out var receipt))
        {
            return AppendResultJson(receipt, deduplicated: true);
        }
        var sessionId = Guid.TryParse(
            body["writer_session_id"]?.GetValue<string>(), out var id)
                ? id
                : throw Refusal.BadRequest("writer_session_id must be a uuid");
        var epoch = body["writer_epoch"]?.GetValue<long>() ?? 0;
        var sequence = body["sequence"]?.GetValue<long>() ?? 0;
        if (!_adhocWriters.TryGetValue(sessionId, out var writer)
            || epoch > writer.Epoch)
        {
            writer = new AdhocWriter { Epoch = epoch, Sequence = 0 };
            _adhocWriters[sessionId] = writer;
        }
        if (epoch < writer.Epoch)
        {
            throw Refusal.StaleEpoch(
                $"writer epoch {epoch} is superseded by {writer.Epoch}");
        }
        if (sequence != writer.Sequence + 1)
        {
            throw Refusal.Conflict(
                $"writer sequence {sequence} is stale or gapped; expected {writer.Sequence + 1}",
                "sequence");
        }
        writer.Sequence = sequence;
        var record = CommitRecord(
            feed,
            sessionId,
            sequence,
            OptionalBytes(body, "key_base64"),
            Convert.FromBase64String(
                body["payload_base64"]?.GetValue<string>() ?? ""),
            OptionalNanos(body, "event_time_ns"),
            body["metadata_base64"]);
        _feedDedup[dedupKey] = new AppendReceipt(record.MessageId, record.Cursor);
        return AppendResultJson(record, deduplicated: false);
    }

    // ---- readers ---------------------------------------------------------

    private ReaderState RequireReader(JsonObject body)
    {
        var name = body["reader"]?.GetValue<string>() ?? "";
        return _readers.TryGetValue(name, out var reader)
            ? reader
            : throw Refusal.BadRequest($"unknown reader: {name}");
    }

    private static void GuardReaderEpoch(ReaderState reader, JsonObject body)
    {
        var epoch = body["session_epoch"]?.GetValue<long>() ?? -1;
        if (!reader.Open || epoch != reader.Epoch)
        {
            throw Refusal.StaleEpoch(
                $"reader session epoch {epoch} is fenced — the session was superseded");
        }
    }

    private JsonObject SessionJson(ReaderState reader) => new()
    {
        ["reader_id"] = reader.Id.ToString(),
        ["feed_id"] = reader.Feed.Id.ToString(),
        ["session_epoch"] = reader.Epoch,
        ["capacity"] = reader.Capacity,
        ["delivered_cursor"] = reader.DeliverIndex > 0
            ? CursorAt(reader.Feed, reader.DeliverIndex)
            : null,
        ["acknowledged_cursor"] = reader.AckIndex > 0
            ? CursorAt(reader.Feed, reader.AckIndex)
            : null,
    };

    private JsonObject ReaderOpen(JsonObject body)
    {
        var reader = RequireReader(body);
        // A fresh open supersedes any live session and resets delivery to
        // the durable acknowledged frontier — unacked work replays.
        reader.Epoch++;
        reader.Open = true;
        reader.Capacity = body["capacity"]?.GetValue<int>() ?? 64;
        reader.DeliverIndex = reader.AckIndex;
        return SessionJson(reader);
    }

    private JsonObject ReaderFetch(JsonObject body)
    {
        var reader = RequireReader(body);
        GuardReaderEpoch(reader, body);
        var limit = body["limit"]?.GetValue<int>() ?? reader.Capacity;
        var records = new JsonArray();
        var end = Math.Min(reader.DeliverIndex + limit, reader.Feed.Records.Count);
        for (var i = reader.DeliverIndex; i < end; i++)
        {
            records.Add(RecordJson(reader.Feed.Records[(int)i]));
        }
        reader.DeliverIndex = end;
        return new JsonObject
        {
            ["records"] = records,
            ["delivered_cursor"] = records.Count > 0
                ? CursorAt(reader.Feed, reader.DeliverIndex)
                : null,
            ["acknowledged_cursor"] = reader.AckIndex > 0
                ? CursorAt(reader.Feed, reader.AckIndex)
                : null,
            ["recommended_capacity"] = 0,
            ["retry_after_ms"] = 0,
        };
    }

    private JsonObject ReaderAck(JsonObject body)
    {
        var reader = RequireReader(body);
        GuardReaderEpoch(reader, body);
        var cursor = body["cursor"]?.GetValue<string>()
            ?? throw Refusal.BadRequest("ack requires a cursor");
        var position = ResolveCursor(reader.Feed, cursor);
        reader.AckIndex = Math.Max(reader.AckIndex, Math.Min(position, reader.DeliverIndex));
        return SessionJson(reader);
    }

    private JsonObject ReaderClose(JsonObject body)
    {
        var reader = RequireReader(body);
        GuardReaderEpoch(reader, body);
        reader.Open = false;
        // Delivered-but-unacked records replay on the next open.
        reader.DeliverIndex = reader.AckIndex;
        return SessionJson(reader);
    }

    private JsonObject TemporaryFetch(JsonObject body)
    {
        var name = body["feed"]?.GetValue<string>() ?? "";
        var feed = FeedFor(name);
        long start = 0;
        if (body["new_only"]?.GetValue<bool>() == true)
        {
            start = feed.Records.Count;
        }
        else if (body["after"]?.GetValue<string>() is { } after)
        {
            start = ResolveCursor(feed, after);
        }
        else if (OptionalNanos(body, "after_event_time_ns") is { } nanos)
        {
            while (start < feed.Records.Count
                && feed.Records[(int)start].EventTimeNs <= nanos)
            {
                start++;
            }
        }
        var limit = body["limit"]?.GetValue<int>() ?? 256;
        var records = new JsonArray();
        var end = Math.Min(start + limit, feed.Records.Count);
        for (var i = start; i < end; i++)
        {
            records.Add(RecordJson(feed.Records[(int)i]));
        }
        // tail/wait_ms long-poll hints return immediately — the fake
        // never parks a request on future appends.
        return new JsonObject
        {
            ["records"] = records,
            ["next_cursor"] = CursorAt(feed, end),
        };
    }

    private string FeedRecords(string query)
    {
        var parameters = new Dictionary<string, string>();
        foreach (var pair in query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = pair.Split('=', 2);
            parameters[Uri.UnescapeDataString(parts[0])] = parts.Length > 1
                ? Uri.UnescapeDataString(parts[1].Replace('+', ' '))
                : "";
        }
        var feed = FeedFor(parameters.TryGetValue("feed", out var name) ? name : "");
        long start = 0;
        if (parameters.TryGetValue("after", out var after) && after.Length > 0)
        {
            start = ResolveCursor(feed, after);
        }
        var limit = parameters.TryGetValue("limit", out var raw) && int.TryParse(raw, out var bound)
            ? bound
            : int.MaxValue;
        var records = new JsonArray();
        for (var i = start; i < feed.Records.Count && records.Count < limit; i++)
        {
            records.Add(RecordJson(feed.Records[(int)i]));
        }
        return records.ToJsonString();
    }

    // ---- state stores --------------------------------------------------------

    private StoreState RequireStore(JsonObject body)
    {
        var name = body["store"]?.GetValue<string>() ?? "";
        return _stores.TryGetValue(name, out var store)
            ? store
            : throw Refusal.BadRequest($"unknown state store: {name}");
    }

    private static void GuardManual(StoreState store, string name)
    {
        if (!store.Manual)
        {
            throw Refusal.BadRequest(
                $"state store {name} is feed-sourced — mutations arrive via its source Feed");
        }
    }

    /// <summary>One typed named value's posting key: kind + payload.</summary>
    private static string TypedKey(JsonNode? typed)
    {
        var kind = typed?["kind"]?.GetValue<string>() ?? "null";
        var payload = typed?["value"]?.ToJsonString()
            ?? typed?["value_base64"]?.GetValue<string>()
            ?? "";
        return kind + "|" + payload;
    }

    private void IndexPostings(
        StoreState store, byte[] rowKey, Dictionary<string, List<string>> named)
    {
        foreach (var (name, values) in named)
        {
            if (!store.Postings.TryGetValue(name, out var postings))
            {
                postings = new Dictionary<string, List<byte[]>>();
                store.Postings[name] = postings;
            }
            var unique = store.NamedKeys.TryGetValue(name, out var u) && u;
            foreach (var typed in values)
            {
                if (!postings.TryGetValue(typed, out var rows))
                {
                    rows = new List<byte[]>();
                    postings[typed] = rows;
                }
                // Unique keys arbitrate first-committed-wins: a later
                // row never steals an existing claim.
                if (unique && rows.Count > 0)
                {
                    continue;
                }
                rows.Add(rowKey);
            }
        }
    }

    private void RemovePostings(
        StoreState store, byte[] rowKey, Dictionary<string, List<string>> named)
    {
        foreach (var (name, values) in named)
        {
            if (!store.Postings.TryGetValue(name, out var postings))
            {
                continue;
            }
            foreach (var typed in values)
            {
                if (postings.TryGetValue(typed, out var rows))
                {
                    rows.RemoveAll(k => BytesComparer.Instance.Compare(k, rowKey) == 0);
                }
            }
        }
    }

    private JsonObject MutationResultJson(
        StoreState store, string mutationId, long sequence, bool deduplicated) => new()
    {
        ["store_id"] = store.Id.ToString(),
        ["mutation_id"] = mutationId,
        ["sequence"] = sequence,
        ["deduplicated"] = deduplicated,
    };

    private JsonObject StoreMutate(JsonObject body)
    {
        var name = body["store"]?.GetValue<string>() ?? "";
        var store = RequireStore(body);
        GuardManual(store, name);
        var mutationId = body["mutation_id"]?.GetValue<string>() ?? "";
        var op = Require(body["op"]);
        var canonical = op.ToJsonString();
        if (store.Mutations.TryGetValue(mutationId, out var receipt))
        {
            if (receipt.Canonical != canonical)
            {
                throw Refusal.Conflict(
                    $"mutation_id {mutationId} was already committed with different content");
            }
            return MutationResultJson(store, mutationId, receipt.Sequence, deduplicated: true);
        }
        var kind = op["kind"]?.GetValue<string>();
        var key = Convert.FromBase64String(
            op["key_base64"]?.GetValue<string>() ?? "");
        switch (kind)
        {
            case "put_row":
            {
                var named = new Dictionary<string, List<string>>();
                if (op["named"] is JsonArray entries)
                {
                    foreach (var entry in entries)
                    {
                        var namedName = entry!["name"]!.GetValue<string>();
                        if (!store.NamedKeys.ContainsKey(namedName))
                        {
                            throw Refusal.BadRequest(
                                $"undeclared named key on {name}: {namedName}");
                        }
                        var values = new List<string>();
                        foreach (var typed in entry["values"]!.AsArray())
                        {
                            values.Add(TypedKey(typed));
                        }
                        named[namedName] = values;
                    }
                }
                var payload = Convert.FromBase64String(
                    op["payload_base64"]?.GetValue<string>() ?? "");
                if (store.Rows.TryGetValue(key, out var existing))
                {
                    RemovePostings(store, key, existing.Named);
                }
                store.Rows[key] = new StoreRowState
                {
                    Payload = payload,
                    Named = named,
                    AppliedSeq = ++store.Seq,
                };
                IndexPostings(store, key, named);
                break;
            }
            case "delete_row":
            {
                if (store.Rows.TryGetValue(key, out var existing))
                {
                    RemovePostings(store, key, existing.Named);
                    store.Rows.Remove(key);
                }
                store.Seq++;
                break;
            }
            default:
                throw Refusal.BadRequest($"unknown mutation op kind: {kind}");
        }
        store.Mutations[mutationId] = new MutationReceipt(store.Seq, canonical);
        return MutationResultJson(store, mutationId, store.Seq, deduplicated: false);
    }

    private JsonObject FreshnessJson(StoreState store) => new()
    {
        ["applied_seq"] = store.Seq,
        ["committed_seq"] = store.Seq,
        ["behind"] = false,
        ["served_by"] = NodeId,
    };

    private JsonObject StoreLookup(JsonObject body)
    {
        var store = RequireStore(body);
        var key = Convert.FromBase64String(
            body["key_base64"]?.GetValue<string>() ?? "");
        var response = new JsonObject
        {
            ["store_id"] = store.Id.ToString(),
            ["found"] = store.Rows.TryGetValue(key, out var row),
        };
        if (row != null)
        {
            response["payload_base64"] = Convert.ToBase64String(row.Payload);
            response["applied_cursor"] = row.AppliedSeq
                .ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
        var freshness = FreshnessJson(store);
        foreach (var entry in freshness)
        {
            response[entry.Key] = entry.Value?.DeepClone();
        }
        return response;
    }

    private JsonObject StoreGetBy(JsonObject body)
    {
        var name = body["store"]?.GetValue<string>() ?? "";
        var store = RequireStore(body);
        var namedKey = body["named_key"]?.GetValue<string>() ?? "";
        if (!store.NamedKeys.ContainsKey(namedKey))
        {
            throw Refusal.BadRequest($"undeclared named key on {name}: {namedKey}");
        }
        var matches = new List<byte[]>();
        var seen = new HashSet<string>();
        if (body["values"] is JsonArray values)
        {
            foreach (var typed in values)
            {
                if (store.Postings.TryGetValue(namedKey, out var postings)
                    && postings.TryGetValue(TypedKey(typed), out var claimed))
                {
                    foreach (var rowKey in claimed)
                    {
                        if (seen.Add(Convert.ToBase64String(rowKey)))
                        {
                            matches.Add(rowKey);
                        }
                    }
                }
            }
        }
        matches.Sort(BytesComparer.Instance);
        var limit = body["limit"]?.GetValue<int>() ?? 64;
        var rows = new JsonArray();
        foreach (var rowKey in matches.Take(limit))
        {
            var row = store.Rows[rowKey];
            rows.Add(new JsonObject
            {
                ["key_base64"] = Convert.ToBase64String(rowKey),
                ["payload_base64"] = Convert.ToBase64String(row.Payload),
                ["applied_cursor"] = row.AppliedSeq
                    .ToString(System.Globalization.CultureInfo.InvariantCulture),
            });
        }
        var response = new JsonObject
        {
            ["store_id"] = store.Id.ToString(),
            ["rows"] = rows,
        };
        var freshness = FreshnessJson(store);
        foreach (var entry in freshness)
        {
            response[entry.Key] = entry.Value?.DeepClone();
        }
        return response;
    }

    private JsonObject StoreIterate(JsonObject body)
    {
        var store = RequireStore(body);
        byte[]? after = null;
        if (body["after_key_base64"]?.GetValue<string>() is { } encoded)
        {
            after = Convert.FromBase64String(encoded);
        }
        var limit = body["limit"]?.GetValue<int>() ?? 256;
        var rows = new JsonArray();
        byte[]? lastKey = null;
        var more = false;
        foreach (var (rowKey, row) in store.Rows)
        {
            if (after != null && BytesComparer.Instance.Compare(rowKey, after) <= 0)
            {
                continue;
            }
            if (rows.Count >= limit)
            {
                more = true;
                break;
            }
            rows.Add(new JsonObject
            {
                ["key_base64"] = Convert.ToBase64String(rowKey),
                ["payload_base64"] = Convert.ToBase64String(row.Payload),
                ["applied_seq"] = row.AppliedSeq,
            });
            lastKey = rowKey;
        }
        return new JsonObject
        {
            ["store_id"] = store.Id.ToString(),
            ["rows"] = rows,
            ["next_key_base64"] = more && lastKey != null
                ? Convert.ToBase64String(lastKey)
                : null,
            ["pinned_seq"] = store.Seq,
            ["served_by"] = NodeId,
        };
    }

    private JsonObject StoreClear(JsonObject body)
    {
        var name = body["store"]?.GetValue<string>() ?? "";
        var store = RequireStore(body);
        GuardManual(store, name);
        var mutationId = body["mutation_id"]?.GetValue<string>() ?? "";
        const string canonical = "clear";
        if (store.Mutations.TryGetValue(mutationId, out var receipt))
        {
            if (receipt.Canonical != canonical)
            {
                throw Refusal.Conflict(
                    $"mutation_id {mutationId} was already committed with different content");
            }
            return MutationResultJson(store, mutationId, receipt.Sequence, deduplicated: true);
        }
        store.Rows.Clear();
        store.Postings.Clear();
        store.Seq++;
        store.Mutations[mutationId] = new MutationReceipt(store.Seq, canonical);
        return MutationResultJson(store, mutationId, store.Seq, deduplicated: false);
    }

    // ---- control surface -----------------------------------------------------

    private (int Status, string Body) Wcl(JsonNode? body)
    {
        var request = Require(body);
        var requestId = request["request_id"]?.GetValue<string>()
            ?? Guid.NewGuid().ToString();
        if (_adminDedup.TryGetValue(requestId, out var cached))
        {
            return cached;
        }
        var script = request["script"]?.GetValue<string>() ?? "";
        var results = new JsonArray();
        foreach (var statement in script.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var trimmed = statement.Trim();
            if (trimmed.Length == 0)
            {
                continue;
            }
            results.Add(new JsonObject
            {
                ["statement"] = trimmed,
                ["message"] = "ok",
                ["data"] = ExecuteWcl(trimmed),
            });
        }
        var response = new JsonObject
        {
            ["request_id"] = requestId,
            ["revision"] = ++_revision,
            ["authority"] = "control_plane",
            ["warning"] = "",
            ["results"] = results,
        };
        var outcome = (200, response.ToJsonString());
        _adminDedup[requestId] = outcome;
        return outcome;
    }

    private (int Status, string Body) Commands(JsonNode? body)
    {
        var request = Require(body);
        var requestId = request["request_id"]?.GetValue<string>()
            ?? Guid.NewGuid().ToString();
        if (_adminDedup.TryGetValue(requestId, out var cached))
        {
            return cached;
        }
        var results = new JsonArray();
        if (request["commands"] is JsonArray commands)
        {
            foreach (var command in commands)
            {
                var entry = Require(command);
                results.Add(new JsonObject
                {
                    ["statement"] = entry["command"]?.GetValue<string>() ?? "",
                    ["message"] = "ok",
                    ["data"] = ExecuteCommand(entry),
                });
            }
        }
        var response = new JsonObject
        {
            ["request_id"] = requestId,
            ["revision"] = ++_revision,
            ["authority"] = "control_plane",
            ["warning"] = "",
            ["results"] = results,
        };
        var outcome = (200, response.ToJsonString());
        _adminDedup[requestId] = outcome;
        return outcome;
    }

    /// <summary>Tokenize one WCL statement; single-quoted segments stay
    /// whole (<c>CURSOR 'abc'</c>) and '' unescapes to '.</summary>
    private static List<string> WclTokens(string statement)
    {
        var tokens = new List<string>();
        var current = new StringBuilder();
        var quoted = false;
        for (var i = 0; i < statement.Length; i++)
        {
            var c = statement[i];
            if (quoted)
            {
                if (c == '\'')
                {
                    if (i + 1 < statement.Length && statement[i + 1] == '\'')
                    {
                        current.Append('\'');
                        i++;
                    }
                    else
                    {
                        quoted = false;
                    }
                }
                else
                {
                    current.Append(c);
                }
            }
            else if (c == '\'')
            {
                quoted = true;
            }
            else if (char.IsWhiteSpace(c))
            {
                if (current.Length > 0)
                {
                    tokens.Add(current.ToString());
                    current.Clear();
                }
            }
            else
            {
                current.Append(c);
            }
        }
        if (current.Length > 0)
        {
            tokens.Add(current.ToString());
        }
        return tokens;
    }

    private long StartIndex(FeedState feed, List<string> tokens, int at)
    {
        if (at >= tokens.Count)
        {
            return 0;
        }
        switch (tokens[at].ToUpperInvariant())
        {
            case "NOW":
                return feed.Records.Count;
            case "CURSOR" when at + 1 < tokens.Count:
                return ResolveCursor(feed, tokens[at + 1]);
            case "TIMESTAMP" when at + 1 < tokens.Count
                && long.TryParse(tokens[at + 1], out var nanos):
            {
                long index = 0;
                while (index < feed.Records.Count
                    && feed.Records[(int)index].EventTimeNs < nanos)
                {
                    index++;
                }
                return index;
            }
            default:
                return 0;
        }
    }

    private static Refusal Exists(string kind, string name)
        => Refusal.BadRequest($"{kind} '{name}' already exists");

    private JsonNode ExecuteWcl(string statement)
    {
        var tokens = WclTokens(statement);
        if (tokens.Count == 0)
        {
            return new JsonObject();
        }
        var verb = tokens[0].ToUpperInvariant();
        var kind = tokens.Count > 1 ? tokens[1].ToUpperInvariant() : "";
        if (tokens.Count < 3
            && verb is "CREATE" or "DROP" or "RENAME" or "DESCRIBE" or "SEEK")
        {
            throw Refusal.BadRequest($"malformed WCL: {statement}");
        }
        switch (verb)
        {
            case "CREATE" when kind is "DOMAIN" or "ROLE":
            {
                if (kind == "DOMAIN")
                {
                    if (_domains.ContainsKey(tokens[2]))
                    {
                        throw Exists(kind, tokens[2]);
                    }
                    DomainFor(tokens[2]);
                }
                else
                {
                    if (_roles.ContainsKey(tokens[2]))
                    {
                        throw Exists(kind, tokens[2]);
                    }
                    _roles[tokens[2]] = Guid.NewGuid();
                }
                return new JsonObject { ["name"] = tokens[2] };
            }
            case "CREATE" when kind == "FEED":
            {
                if (_feeds.ContainsKey(tokens[2]))
                {
                    throw Exists("FEED", tokens[2]);
                }
                FeedFor(tokens[2]);
                return new JsonObject { ["name"] = tokens[2] };
            }
            case "CREATE" when kind == "WRITER":
            {
                // CREATE WRITER <name> TO <feed>
                var name = tokens[2];
                if (_writers.ContainsKey(name))
                {
                    throw Exists("WRITER", name);
                }
                var feed = tokens.Count > 4 && tokens[3].Equals("TO", StringComparison.OrdinalIgnoreCase)
                    ? tokens[4]
                    : throw Refusal.BadRequest($"unsupported WCL: {statement}");
                _writers[name] = new WriterState
                {
                    Feed = FeedFor(feed),
                    CreatedAtNs = NowNs(),
                };
                return new JsonObject { ["name"] = name };
            }
            case "CREATE" when kind is "READER" or "SUBSCRIPTION":
            {
                // CREATE READER|SUBSCRIPTION <name> FROM <feed> [START AT ...]
                var name = tokens[2];
                var taken = kind == "READER"
                    ? _readers.ContainsKey(name)
                    : _subscriptions.ContainsKey(name);
                if (taken)
                {
                    throw Exists(kind, name);
                }
                var feed = tokens.Count > 4 && tokens[3].Equals("FROM", StringComparison.OrdinalIgnoreCase)
                    ? tokens[4]
                    : throw Refusal.BadRequest($"unsupported WCL: {statement}");
                var records = FeedFor(feed);
                var index = 0L;
                for (var i = 5; i + 2 < tokens.Count; i++)
                {
                    if (tokens[i].Equals("START", StringComparison.OrdinalIgnoreCase)
                        && tokens[i + 1].Equals("AT", StringComparison.OrdinalIgnoreCase))
                    {
                        index = StartIndex(records, tokens, i + 2);
                        break;
                    }
                }
                if (kind == "READER")
                {
                    _readers[name] = new ReaderState
                    {
                        Feed = records,
                        StartKind = StartKind(tokens),
                        AckIndex = index,
                        DeliverIndex = index,
                        CreatedAtNs = NowNs(),
                    };
                }
                else
                {
                    _subscriptions[name] = new SubscriptionState
                    {
                        Feed = feed,
                        StartKind = StartKind(tokens),
                        CreatedAtNs = NowNs(),
                    };
                }
                return new JsonObject { ["name"] = name };
            }
            case "OPEN" when kind == "WRITER"
                && tokens.Count > 3 && tokens[2].Equals("SESSION", StringComparison.OrdinalIgnoreCase):
            {
                var name = tokens[3];
                if (!_writers.TryGetValue(name, out var writer))
                {
                    throw Refusal.BadRequest($"unknown writer: {name}");
                }
                writer.Epoch++;
                writer.SessionId = Guid.NewGuid();
                return new JsonObject { ["session_epoch"] = writer.Epoch };
            }
            case "SEEK" when kind == "READER" && tokens.Count > 4:
            {
                var name = tokens[2];
                if (!_readers.TryGetValue(name, out var reader))
                {
                    throw Refusal.BadRequest($"unknown reader: {name}");
                }
                // SEEK READER <name> TO <start>
                var index = tokens[3].Equals("TO", StringComparison.OrdinalIgnoreCase)
                    ? StartIndex(reader.Feed, tokens, 4)
                    : 0;
                reader.AckIndex = index;
                reader.DeliverIndex = index;
                return new JsonObject { ["name"] = name };
            }
            case "DROP" when tokens.Count > 2:
            {
                var name = tokens[2];
                var removed = kind switch
                {
                    "DOMAIN" => _domains.Remove(name),
                    "ROLE" => _roles.Remove(name),
                    "FEED" => _feeds.Remove(name),
                    "WRITER" => _writers.Remove(name),
                    "READER" => _readers.Remove(name),
                    "SUBSCRIPTION" => _subscriptions.Remove(name),
                    _ => throw Refusal.BadRequest($"unsupported WCL: {statement}"),
                };
                if (!removed)
                {
                    throw Refusal.BadRequest($"{kind} '{name}' not found");
                }
                return new JsonObject { ["name"] = name };
            }
            case "RENAME" when tokens.Count > 4
                && tokens[3].Equals("TO", StringComparison.OrdinalIgnoreCase):
            {
                var current = tokens[2];
                var renamed = tokens[4];
                var moved = kind switch
                {
                    "DOMAIN" => MoveMap(_domains, current, renamed),
                    "ROLE" => MoveMap(_roles, current, renamed),
                    "FEED" => MoveMap(_feeds, current, renamed),
                    "WRITER" => MoveMap(_writers, current, renamed),
                    "READER" => MoveMap(_readers, current, renamed),
                    "SUBSCRIPTION" => MoveMap(_subscriptions, current, renamed),
                    _ => throw Refusal.BadRequest($"unsupported WCL: {statement}"),
                };
                if (!moved)
                {
                    throw Refusal.BadRequest($"{kind} '{current}' not found");
                }
                return new JsonObject { ["name"] = renamed };
            }
            case "DESCRIBE" when tokens.Count > 2:
            {
                return Describe(kind, tokens[2]);
            }
            case "SHOW" when tokens.Count > 1:
            {
                return Show(kind);
            }
            default:
                return new JsonObject();
        }

        static bool MoveMap<TValue>(Dictionary<string, TValue> map, string current, string renamed)
        {
            if (!map.Remove(current, out var value))
            {
                return false;
            }
            map[renamed] = value!;
            return true;
        }
    }

    /// <summary><c>START AT</c> tokens → the declared kind.</summary>
    private static string StartKind(List<string> tokens)
    {
        for (var i = 0; i + 2 < tokens.Count; i++)
        {
            if (tokens[i].Equals("START", StringComparison.OrdinalIgnoreCase)
                && tokens[i + 1].Equals("AT", StringComparison.OrdinalIgnoreCase))
            {
                var operand = tokens[i + 2].ToUpperInvariant();
                if (operand.StartsWith("NOW")) return "now";
                if (operand.StartsWith("CURSOR")) return "cursor";
                if (operand.StartsWith("TIMESTAMP")) return "timestamp";
                return "beginning";
            }
        }
        return "beginning";
    }

    /// <summary>The catalog row SHOW/DESCRIBE surfaces for one resource —
    /// field names match the server's <c>space_id</c>/<c>feed_id</c>
    /// join keys.</summary>
    private JsonObject ResourceRow(string kind, string name)
    {
        switch (kind)
        {
            case "DOMAIN":
            case "DOMAINS":
            case "SPACE":
            {
                var d = _domains[name];
                return new JsonObject
                {
                    ["name"] = name,
                    ["space_id"] = d.Id.ToString(),
                    ["domain_id"] = d.Id.ToString(),
                    ["status"] = "active",
                    ["created_at_ns"] = d.CreatedAtNs.ToString(),
                };
            }
            case "FEED":
            case "FEEDS":
            {
                var f = _feeds[name];
                return new JsonObject
                {
                    ["name"] = name,
                    ["feed_id"] = f.Id.ToString(),
                    ["space_id"] = SpaceIdOf(name),
                    ["status"] = "active",
                    ["created_at_ns"] = f.CreatedAtNs.ToString(),
                };
            }
            case "WRITER":
            case "WRITERS":
            {
                var w = _writers[name];
                return new JsonObject
                {
                    ["name"] = name,
                    ["writer_id"] = w.Id.ToString(),
                    ["feed"] = _feeds.First(p => p.Value == w.Feed).Key,
                    ["feed_id"] = w.Feed.Id.ToString(),
                    ["status"] = "active",
                    ["created_at_ns"] = w.CreatedAtNs.ToString(),
                    ["session_epoch"] = w.Epoch,
                    ["session_active"] = w.Epoch > 0,
                };
            }
            case "READER":
            case "READERS":
            {
                var r = _readers[name];
                return new JsonObject
                {
                    ["name"] = name,
                    ["reader_id"] = r.Id.ToString(),
                    ["feed"] = _feeds.First(p => p.Value == r.Feed).Key,
                    ["feed_id"] = r.Feed.Id.ToString(),
                    ["start"] = new JsonObject { ["kind"] = r.StartKind },
                    ["status"] = "active",
                    ["created_at_ns"] = r.CreatedAtNs.ToString(),
                    ["session_epoch"] = r.Epoch,
                    ["session_active"] = r.Open,
                    ["session_capacity"] = r.Capacity,
                };
            }
            case "SUBSCRIPTION":
            case "SUBSCRIPTIONS":
            {
                var s = _subscriptions[name];
                return new JsonObject
                {
                    ["name"] = name,
                    ["subscription_id"] = s.Id.ToString(),
                    ["space_id"] = SpaceIdOf(s.Feed),
                    ["feed"] = s.Feed,
                    ["start"] = new JsonObject { ["kind"] = s.StartKind },
                    ["stage"] = "declared",
                    ["status"] = "active",
                    ["created_at_ns"] = s.CreatedAtNs.ToString(),
                };
            }
            case "STATESTORE":
            case "STATE_STORE":
            case "STATESTORES":
            case "STATE_STORES":
            {
                var s = _stores[name];
                return new JsonObject
                {
                    ["name"] = name,
                    ["store_id"] = s.Id.ToString(),
                    ["space_id"] = SpaceIdOf(name),
                    ["status"] = "active",
                    ["created_at_ns"] = s.CreatedAtNs.ToString(),
                };
            }
            case "ROLE":
            case "ROLES":
            {
                return new JsonObject
                {
                    ["name"] = name,
                    ["role_id"] = _roles[name].ToString(),
                    ["status"] = "active",
                };
            }
            default:
                return new JsonObject { ["name"] = name };
        }
    }

    /// <summary><c>DESCRIBE &lt;kind&gt; &lt;name&gt;</c> — the row, or a
    /// refusal when missing.</summary>
    private JsonObject Describe(string kind, string name)
    {
        var known = kind switch
        {
            "DOMAIN" or "SPACE" => _domains.ContainsKey(name),
            "ROLE" => _roles.ContainsKey(name),
            "FEED" => _feeds.ContainsKey(name),
            "WRITER" => _writers.ContainsKey(name),
            "READER" => _readers.ContainsKey(name),
            "SUBSCRIPTION" => _subscriptions.ContainsKey(name),
            "STATESTORE" or "STATE_STORE" => _stores.ContainsKey(name),
            _ => false,
        };
        if (!known)
        {
            throw Refusal.BadRequest($"resource not found: {name}");
        }
        return ResourceRow(kind, name);
    }

    /// <summary><c>SHOW &lt;kind&gt;</c> — the bare row array, matching
    /// the server's data shape.</summary>
    private JsonArray Show(string kind)
    {
        IEnumerable<string> names = kind switch
        {
            "DOMAINS" or "SPACES" => _domains.Keys,
            "ROLES" => _roles.Keys,
            "FEEDS" => _feeds.Keys,
            "WRITERS" => _writers.Keys,
            "READERS" => _readers.Keys,
            "SUBSCRIPTIONS" => _subscriptions.Keys,
            "STATESTORES" or "STATE_STORES" => _stores.Keys,
            _ => Array.Empty<string>(),
        };
        var rows = new JsonArray();
        foreach (var name in names.OrderBy(n => n, StringComparer.Ordinal))
        {
            rows.Add(ResourceRow(kind, name));
        }
        return rows;
    }

    private JsonNode ExecuteCommand(JsonObject command)
    {
        var name = command["command"]?.GetValue<string>() ?? "";
        switch (name)
        {
            case "define_state_store":
            {
                var storeName = command["name"]?.GetValue<string>() ?? "";
                if (_stores.TryGetValue(storeName, out var existing))
                {
                    return new JsonObject
                    {
                        ["name"] = storeName,
                        ["store_id"] = existing.Id.ToString(),
                    };
                }
                var store = new StoreState
                {
                    Manual = command["source"]?["kind"]?.GetValue<string>() != "feed",
                    CreatedAtNs = NowNs(),
                };
                if (command["named_keys"] is JsonArray keys)
                {
                    foreach (var key in keys)
                    {
                        var keyName = key!["name"]!.GetValue<string>();
                        store.NamedKeys[keyName] =
                            key["unique"]?.GetValue<bool>() ?? false;
                    }
                }
                _stores[storeName] = store;
                return new JsonObject
                {
                    ["name"] = storeName,
                    ["store_id"] = store.Id.ToString(),
                };
            }
            case "drop" when command["kind"]?.GetValue<string>() == "state_store":
            {
                var storeName = command["name"]?.GetValue<string>() ?? "";
                if (!_stores.Remove(storeName))
                {
                    throw Refusal.BadRequest($"state store '{storeName}' not found");
                }
                return new JsonObject { ["name"] = storeName };
            }
            case "describe" when command["kind"]?.GetValue<string>() == "pipe":
            {
                var pipeName = command["name"]?.GetValue<string>() ?? "";
                return new JsonObject
                {
                    ["name"] = pipeName,
                    ["pipe_id"] = Guid.NewGuid().ToString(),
                    ["stage"] = "declared",
                    ["subscription_id"] = Guid.NewGuid().ToString(),
                    ["space_id"] = SpaceIdOf(pipeName),
                    ["status"] = "ok",
                    ["created_at_ns"] = NowNs(),
                };
            }
            case "describe":
                return Describe(
                    (command["kind"]?.GetValue<string>() ?? "").ToUpperInvariant(),
                    command["name"]?.GetValue<string>() ?? "");
            case "show":
                return Show(
                    (command["kind"]?.GetValue<string>() ?? "").ToUpperInvariant());
            default:
                // define_pipe / pause_pipe / resume_pipe / grant / unknown
                // lifecycle — canned success.
                return new JsonObject();
        }
    }
}
