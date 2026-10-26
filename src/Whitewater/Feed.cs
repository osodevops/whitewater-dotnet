using System.Runtime.CompilerServices;
using Whitewater.Internal;

namespace Whitewater;

/// <summary>
/// One committed Feed record with its key and payload already decoded
/// through the bound codecs — the <see cref="FeedRecord"/> fields carry
/// through; only the payloads are typed. Times are signed 64-bit Unix
/// epoch nanoseconds; <paramref name="Cursor"/> is opaque — pass it back
/// verbatim.
/// </summary>
public sealed record FeedMessage<TKey, TValue>(
    Cursor Cursor,
    Guid MessageId,
    Guid ProducerId,
    long Sequence,
    long EventTimeNs,
    long IngestTimeNs,
    /// <summary>Decoded key, or null when the record carries no key.</summary>
    TKey? Key,
    TValue Value,
    IReadOnlyDictionary<string, byte[]> Metadata)
{
    /// <summary>Decode one Metadata entry as UTF-8; null when absent.</summary>
    public string? MetadataText(string name)
        => Metadata.TryGetValue(name, out var bytes) ? System.Text.Encoding.UTF8.GetString(bytes) : null;
}

/// <summary>A decoded temporary-fetch page: messages plus the opaque
/// continuation frontier.</summary>
public sealed record FeedPage<TKey, TValue>(
    IReadOnlyList<FeedMessage<TKey, TValue>> Records,
    Cursor? NextCursor);

/// <summary>One bounded page of committed records, decoded. Acknowledge
/// <paramref name="DeliveredCursor"/> — not the last record's cursor —
/// to durably commit progress.</summary>
public sealed record FeedReaderPage<TKey, TValue>(
    IReadOnlyList<FeedMessage<TKey, TValue>> Records,
    Cursor? DeliveredCursor,
    Cursor? AcknowledgedCursor,
    int RecommendedCapacity,
    int RetryAfterMs);

/// <summary>A typed record to append: request identity, key, payload,
/// optional event time and Metadata. The bound codecs serialize
/// <paramref name="Key"/>/<paramref name="Value"/> at write time —
/// application code never touches bytes.</summary>
public sealed record FeedEvent<TKey, TValue>(
    TValue Value,
    TKey? Key = default,
    /// <summary>Null generates a fresh identity; pin it for
    /// caller-driven retries (identical retries dedupe server-side).</summary>
    Guid? RequestId = null,
    long? EventTimeNs = null,
    IReadOnlyDictionary<string, byte[]>? Metadata = null);

/// <summary>A session-bound Writer with the feed's codecs applied —
/// the session epoch and request identities stay inside the handle.</summary>
public sealed class FeedWriter<TKey, TValue>
{
    private readonly WriterSessionClient _session;
    private readonly ICodec<TKey> _keyCodec;
    private readonly ICodec<TValue> _valueCodec;

    internal FeedWriter(
        WriterSessionClient session, ICodec<TKey> keyCodec, ICodec<TValue> valueCodec)
    {
        _session = session;
        _keyCodec = keyCodec;
        _valueCodec = valueCodec;
    }

    /// <summary>The session's fencing epoch.</summary>
    public long SessionEpoch => _session.SessionEpoch;

    /// <summary>Enable wire compression on subsequent appends.</summary>
    public FeedWriter<TKey, TValue> UseCompression(bool enabled = true)
        => new(_session.UseCompression(enabled), _keyCodec, _valueCodec);

    /// <summary>Encode one record through the bound codecs; request
    /// identity and event time auto-generate. For a keyless record or
    /// pinned identity use <see cref="AppendEventAsync"/>.</summary>
    public Task<AppendResult> AppendAsync(
        TKey key, TValue value, CancellationToken ct = default)
        => AppendEventAsync(new FeedEvent<TKey, TValue>(value, key), ct);

    /// <summary>Append one typed event.</summary>
    public Task<AppendResult> AppendEventAsync(
        FeedEvent<TKey, TValue> evt, CancellationToken ct = default)
    {
        var (key, payload) = Encode(evt);
        return _session.AppendAsync(key, payload, evt.RequestId, evt.EventTimeNs, evt.Metadata, ct);
    }

    /// <summary>One epoch-fenced batch; per-record results plus server
    /// batching guidance.</summary>
    public Task<AppendBatchResult> AppendAllAsync(
        IReadOnlyList<FeedEvent<TKey, TValue>> events, CancellationToken ct = default)
    {
        var records = new List<WriterSessionClient.AppendInput>(events.Count);
        foreach (var evt in events)
        {
            var (key, payload) = Encode(evt);
            records.Add(new WriterSessionClient.AppendInput(
                key, payload, evt.RequestId, evt.EventTimeNs, evt.Metadata));
        }
        return _session.AppendBatchAsync(records, ct);
    }

    private (byte[]? Key, byte[] Payload) Encode(FeedEvent<TKey, TValue> evt)
    {
        var key = evt.Key is null ? null : _keyCodec.Encode(evt.Key);
        return (key, _valueCodec.Encode(evt.Value));
    }
}

/// <summary>A session-bound named Reader with the feed's codecs
/// applied — <see cref="FetchAsync"/> returns a bounded page of decoded
/// messages; <see cref="AckAsync"/> commits the page's delivered Cursor
/// durably.</summary>
public sealed class FeedReader<TKey, TValue> : IAsyncDisposable
{
    private readonly ReaderSessionClient _client;
    private readonly ICodec<TKey> _keyCodec;
    private readonly ICodec<TValue> _valueCodec;
    private ReaderSession _session;
    private FeedReaderPage<TKey, TValue>? _lastPage;
    private bool _closed;

    internal FeedReader(
        ReaderSessionClient client,
        ReaderSession session,
        ICodec<TKey> keyCodec,
        ICodec<TValue> valueCodec)
    {
        _client = client;
        _session = session;
        _keyCodec = keyCodec;
        _valueCodec = valueCodec;
    }

    /// <summary>The session's fencing epoch.</summary>
    public long SessionEpoch => _session.SessionEpoch;

    /// <summary>Up to <paramref name="limit"/> committed records after
    /// the frontier, decoded through the bound codecs.</summary>
    public async Task<FeedReaderPage<TKey, TValue>> FetchAsync(
        int? limit = null, CancellationToken ct = default)
    {
        var page = await _client.FetchAsync(_session, limit, ct: ct).ConfigureAwait(false);
        var decoded = new FeedReaderPage<TKey, TValue>(
            DecodeMessages(page.Records), page.DeliveredCursor,
            page.AcknowledgedCursor, page.RecommendedCapacity, page.RetryAfterMs);
        _lastPage = decoded;
        return decoded;
    }

    /// <summary>Durably commit the last fetched page's delivered Cursor.
    /// Call after processing a page; anything unacknowledged may
    /// replay.</summary>
    public Task<ReaderSession> AckAsync(CancellationToken ct = default)
    {
        var cursor = _lastPage?.DeliveredCursor;
        if (cursor is null) return Task.FromResult(_session);
        return AckAsync(cursor.Value, ct);
    }

    /// <summary>Durably commit an explicit Cursor.</summary>
    public async Task<ReaderSession> AckAsync(Cursor cursor, CancellationToken ct = default)
    {
        _session = await _client.AckAsync(_session, cursor, ct: ct).ConfigureAwait(false);
        return _session;
    }

    /// <summary>
    /// Deliver every record to <paramref name="handler"/> — one at a
    /// time — acking each fetched page after its records are handled.
    /// The pump paces empty fetches on the server's
    /// <c>retry_after_ms</c> and exits when the reader is closed or
    /// <paramref name="ct"/> is cancelled. A handler returning
    /// <c>false</c> — or throwing — leaves the in-flight page unacked
    /// so its tail replays: at-least-once delivery, never acknowledge
    /// undelivered records.
    /// </summary>
    public async Task ForEachAsync(
        Func<FeedMessage<TKey, TValue>, ValueTask<bool>> handler,
        CancellationToken ct = default)
    {
        while (!_closed)
        {
            ct.ThrowIfCancellationRequested();
            var page = await FetchAsync(ct: ct).ConfigureAwait(false);
            if (page.Records.Count == 0)
            {
                await Task.Delay(Math.Max(1, page.RetryAfterMs), ct)
                    .ConfigureAwait(false);
                continue;
            }
            foreach (var message in page.Records)
            {
                if (!await handler(message).ConfigureAwait(false)) return;
            }
            if (_closed) return;
            await AckAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>Records as an async stream — <c>await foreach (var m in
    /// reader.MessagesAsync(ct))</c> — acking each page once consumed.
    /// Breaking out leaves the in-flight page unacked so its tail
    /// replays.</summary>
    public async IAsyncEnumerable<FeedMessage<TKey, TValue>> MessagesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        while (!_closed)
        {
            ct.ThrowIfCancellationRequested();
            var page = await FetchAsync(ct: ct).ConfigureAwait(false);
            if (page.Records.Count == 0)
            {
                await Task.Delay(Math.Max(1, page.RetryAfterMs), ct)
                    .ConfigureAwait(false);
                continue;
            }
            foreach (var message in page.Records) yield return message;
            if (_closed) yield break;
            await AckAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>Release the session fence; durable acknowledged
    /// progress survives.</summary>
    public async Task CloseAsync(CancellationToken ct = default)
    {
        if (_closed || _session.SessionEpoch == 0) return;
        _closed = true;
        var session = _session;
        _session = _session with { SessionEpoch = 0 };
        await _client.CloseAsync(session, ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        try { await CloseAsync().ConfigureAwait(false); }
        catch { /* best-effort release */ }
    }

    private IReadOnlyList<FeedMessage<TKey, TValue>> DecodeMessages(
        IReadOnlyList<FeedRecord> records)
        => Feed<TKey, TValue>.DecodeMessages(records, _keyCodec, _valueCodec);
}

/// <summary>
/// A typed facade over one Feed — the immutable temporal history on the
/// Nodes. The bound codecs serialize on write and deserialize on read,
/// so application code stays in its own types:
///
/// <code>
/// var orders = client.Feed("orders.events", Codecs.Json&lt;Order&gt;());
/// var writer = await orders.WriterAsync("ingest");
/// await writer.AppendAsync("order-1", new Order("order-1", 42));
/// </code>
/// </summary>
public sealed class Feed<TKey, TValue>
{
    private readonly WhitewaterClient _client;
    private readonly ICodec<TKey> _keyCodec;
    private readonly ICodec<TValue> _valueCodec;
    private readonly bool _compression;

    internal Feed(
        WhitewaterClient client,
        string name,
        ICodec<TKey> keyCodec,
        ICodec<TValue> valueCodec,
        bool compression = false)
    {
        _client = client;
        Name = name;
        _keyCodec = keyCodec;
        _valueCodec = valueCodec;
        _compression = compression;
    }

    /// <summary>The Feed's dotted name.</summary>
    public string Name { get; }

    /// <summary>Enable wire compression on fetches and ad-hoc appends
    /// through this facade.</summary>
    public Feed<TKey, TValue> UseCompression(bool enabled = true)
        => new(_client, Name, _keyCodec, _valueCodec, enabled);

    /// <summary>Rebind the same feed under different codecs — no wire
    /// call: <c>feed.View(Codecs.Utf8, Codecs.Bytes)</c> reads raw.</summary>
    public Feed<TKey2, TValue2> View<TKey2, TValue2>(
        ICodec<TKey2> keyCodec, ICodec<TValue2> valueCodec)
        => new(_client, Name, keyCodec, valueCodec, _compression);

    /// <summary>Committed records from the beginning or after an opaque
    /// <paramref name="after"/> Cursor, decoded through the bound
    /// codecs.</summary>
    public async Task<IReadOnlyList<FeedMessage<TKey, TValue>>> RecordsAsync(
        int? limit = null, Cursor? after = null, CancellationToken ct = default)
    {
        var records = await Feeds().RecordsAsync(Name, limit, after, ct).ConfigureAwait(false);
        return DecodeMessages(records);
    }

    /// <summary>Anonymous temporary Reader fetch: decoded records plus
    /// the opaque continuation frontier. Retains no server-side
    /// progress.</summary>
    public async Task<FeedPage<TKey, TValue>> FetchAsync(
        TemporaryFetchOptions? options = null, CancellationToken ct = default)
    {
        var page = await Feeds()
            .TemporaryFetchAsync(Name, options ?? new TemporaryFetchOptions(), ct)
            .ConfigureAwait(false);
        return new FeedPage<TKey, TValue>(DecodeMessages(page.Records), page.NextCursor);
    }

    /// <summary>Bulk-read committed history once, from
    /// <paramref name="start"/>, delivering each decoded page to
    /// <paramref name="sink"/> until the frontier is drained.</summary>
    public Task<long> LoadAsync(
        ReaderStart start,
        Func<IReadOnlyList<FeedMessage<TKey, TValue>>, Task> sink,
        CancellationToken ct = default)
        => Feeds().LoadAsync(Name, start,
            records => sink(DecodeMessages(records)), ct);

    /// <summary>
    /// A session-bound Writer on this Feed: declares the Writer when
    /// missing (reusing an existing one), opens a session, and binds
    /// the feed's codecs so <c>writer.AppendAsync(key, value)</c> just
    /// works.
    /// </summary>
    public async Task<FeedWriter<TKey, TValue>> WriterAsync(
        string writerName, CancellationToken ct = default)
    {
        try
        {
            await _client.CreateWriterAsync(writerName, Name, ct).ConfigureAwait(false);
        }
        catch (WhitewaterException error) when (error.Message.Contains("exists"))
        {
            // An existing Writer is reused as-is.
        }
        var setup = await _client.OpenWriterSessionAsync(writerName, ct).ConfigureAwait(false);
        var epoch = setup.Results.Count > 0
            ? setup.Results[0].Data["session_epoch"]?.GetValue<long>() ?? 0
            : 0;
        if (epoch == 0)
        {
            throw new InvalidOperationException(
                $"open_writer_session returned no session_epoch for {writerName}");
        }
        return new FeedWriter<TKey, TValue>(
            _client.Writer(writerName, epoch), _keyCodec, _valueCodec);
    }

    /// <summary>
    /// A session-bound named Reader on this Feed: declares the Reader
    /// when missing (at <paramref name="start"/>, default BEGINNING —
    /// an existing declaration keeps its own start and durable
    /// progress), opens a session with the bounded fetch capacity, and
    /// returns decoded messages.
    /// </summary>
    public async Task<FeedReader<TKey, TValue>> ReaderAsync(
        string readerName, int capacity = 64, ReaderStart? start = null,
        CancellationToken ct = default)
    {
        try
        {
            await _client.CreateReaderAsync(
                    readerName, Name, start ?? ReaderStart.Beginning(), ct)
                .ConfigureAwait(false);
        }
        catch (WhitewaterException error) when (error.Message.Contains("exists"))
        {
            // An existing Reader is reused as-is.
        }
        var session = await _client.Reader(readerName).OpenAsync(capacity, ct: ct)
            .ConfigureAwait(false);
        return new FeedReader<TKey, TValue>(
            _client.Reader(readerName), session, _keyCodec, _valueCodec);
    }

    /// <summary>An ad-hoc single append with a caller-owned writer
    /// identity — codecs serialize key/payload. For durable named
    /// producers prefer <see cref="WriterAsync"/>.</summary>
    public Task<AppendResult> AppendAsync(
        Guid writerSessionId,
        long writerEpoch,
        long sequence,
        TKey? key,
        TValue value,
        CancellationToken ct = default)
        => Feeds().AppendAsync(Name, new FeedAppend(
            Guid.NewGuid(), writerSessionId, writerEpoch, sequence,
            key is null ? null : _keyCodec.Encode(key),
            _valueCodec.Encode(value)), ct);

    /// <summary>A running subscription on this Feed: a named Reader
    /// polled in the background, delivering decoded batches and
    /// acknowledging durable progress after each.</summary>
    public Subscription Subscribe(
        string reader,
        ReaderStart start,
        Func<IReadOnlyList<FeedMessage<TKey, TValue>>, Task> onRecords,
        Action<WhitewaterException>? onError = null)
        => SubscribeDecoded(reader, start, null, onRecords, onError);

    /// <summary>Decoded subscribe with capacity override — the
    /// FeedConsumer seam.</summary>
    internal Subscription SubscribeDecoded(
        string reader,
        ReaderStart start,
        int? capacity,
        Func<IReadOnlyList<FeedMessage<TKey, TValue>>, Task> onRecords,
        Action<WhitewaterException>? onError = null)
        => _client.Subscribe(reader, Name, start,
            records => onRecords(DecodeMessages(records)), onError,
            capacity ?? 128);

    /// <summary>A permanent, supervised consumer bound to this Feed's
    /// named Reader — iterate decoded records via
    /// <see cref="FeedConsumer{TKey, TValue}.Messages"/> or
    /// <see cref="FeedConsumer{TKey, TValue}.RunAsync(Func{FeedMessage{TKey, TValue}, ValueTask}, CancellationToken)"/>
    /// a handler; open/fetch/ack/reopen/backoff stay internal.</summary>
    public FeedConsumer<TKey, TValue> Consumer(string reader)
        => new(this, reader);

    /// <summary>Convenience for
    /// <c>Consumer(reader).Messages(ct)</c> — the one-line permanent
    /// loop:</summary>
    /// <code>
    /// await foreach (var msg in feed.ConsumeAsync("enricher", ct))
    /// {
    ///     await ProcessAsync(msg);
    /// }
    /// </code>
    public IAsyncEnumerable<FeedMessage<TKey, TValue>> ConsumeAsync(
        string reader, CancellationToken ct = default)
        => Consumer(reader).Messages(ct);

    internal static IReadOnlyList<FeedMessage<TKey, TValue>> DecodeMessages(
        IReadOnlyList<FeedRecord> records, ICodec<TKey> keyCodec, ICodec<TValue> valueCodec)
    {
        var messages = new List<FeedMessage<TKey, TValue>>(records.Count);
        foreach (var record in records)
        {
            messages.Add(new FeedMessage<TKey, TValue>(
                record.Cursor, record.MessageId, record.ProducerId, record.Sequence,
                record.EventTimeNs, record.IngestTimeNs,
                record.Key is null ? default : keyCodec.Decode(record.Key),
                valueCodec.Decode(record.Payload), record.Metadata));
        }
        return messages;
    }

    private IReadOnlyList<FeedMessage<TKey, TValue>> DecodeMessages(
        IReadOnlyList<FeedRecord> records)
        => DecodeMessages(records, _keyCodec, _valueCodec);

    private FeedClient Feeds() => _client.Feeds.UseCompression(_compression);
}
