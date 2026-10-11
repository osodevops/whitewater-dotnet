using System.Collections.Concurrent;
using System.Text;

namespace Whitewater;

/// <summary>
/// A named, thread-safe in-memory store of Feed records — the target of
/// <see cref="WhitewaterClient.LoadStore"/> and a typical sink for
/// <see cref="WhitewaterClient.Subscribe"/>:
///
/// <code>
/// var store = RecordStore.Named("employee-store")
///     .AddKey("passport").AddKey("department");
/// await client.LoadStore(store).FromFeedAsync("hr.employees",
///     ReaderStart.Beginning(),
///     (s, record) => s.Upsert(record)
///         .WithKey(record.Key)
///         .WithNamedKey("passport", record.Metadata["pp"]));
/// </code>
///
/// The primary map keys rows on the key chosen by <see cref="UpsertBuilder.WithKey(byte[])"/>
/// (the record's own Key via Put/PutAll, the Cursor for unkeyed records).
/// Each declared named key maintains a separate postings map — conceptually
/// employee-store.passport — of value → primary keys, so named lookups are
/// not unique and return every matching row.
/// </summary>
public sealed class RecordStore : IEnumerable<RecordStore.Entry>
{
    /// <summary>Named secondary keys per store; matches the server Index field budget.</summary>
    public const int MaxNamedKeys = 10;

    private readonly ConcurrentDictionary<string, FeedRecord> _rows = new();
    private readonly List<string> _declaredKeys = new();
    /// <summary>namedKey → base64 value → primary ids (non-unique postings).</summary>
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, HashSet<string>>> _namedIndex = new();

    private RecordStore(string name) => Name = name;

    public static RecordStore Named(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("store name is required", nameof(name));
        }
        return new RecordStore(name);
    }

    /// <summary>Declare a named secondary key (at most <see cref="MaxNamedKeys"/>).</summary>
    public RecordStore AddKey(string namedKey)
    {
        if (string.IsNullOrWhiteSpace(namedKey))
        {
            throw new ArgumentException("named key is required", nameof(namedKey));
        }
        lock (_declaredKeys)
        {
            if (!_declaredKeys.Contains(namedKey))
            {
                if (_declaredKeys.Count >= MaxNamedKeys)
                {
                    throw new InvalidOperationException(
                        $"a RecordStore supports at most {MaxNamedKeys} named keys");
                }
                _declaredKeys.Add(namedKey);
            }
        }
        _namedIndex.TryAdd(namedKey, new ConcurrentDictionary<string, HashSet<string>>());
        return this;
    }

    /// <summary>The declared named keys, in declaration order.</summary>
    public IReadOnlyList<string> NamedKeys
    {
        get { lock (_declaredKeys) { return _declaredKeys.ToArray(); } }
    }

    public string Name { get; }

    /// <summary>A stored row: its primary key and the record value.</summary>
    public sealed record Entry(byte[] PrimaryKey, FeedRecord Record);

    /// <summary>Start a row write: choose the primary key and named-key values.</summary>
    public UpsertBuilder Upsert(FeedRecord record) => new(this, record);

    /// <summary>Insert or replace the row for this record's key.</summary>
    public void Put(FeedRecord record) => CommitUpsert(record, DefaultId(record), new Dictionary<string, byte[]>());

    public void PutAll(IEnumerable<FeedRecord> records)
    {
        foreach (var record in records)
        {
            Put(record);
        }
    }

    /// <summary>The latest record for a key, when one has been loaded.</summary>
    public FeedRecord? Get(byte[] key) =>
        _rows.TryGetValue(Convert.ToBase64String(key), out var record) ? record : null;

    public FeedRecord? Get(string key) => Get(Encoding.UTF8.GetBytes(key));

    /// <summary>Every row whose named key equals <paramref name="value"/> — named keys are not unique.</summary>
    public IReadOnlyList<FeedRecord> Get(string namedKey, byte[] value)
    {
        if (!_namedIndex.TryGetValue(namedKey, out var postings))
        {
            throw new ArgumentException($"undeclared named key: {namedKey}", nameof(namedKey));
        }
        var result = new List<FeedRecord>();
        if (postings.TryGetValue(Convert.ToBase64String(value), out var ids))
        {
            lock (ids)
            {
                foreach (var id in ids)
                {
                    if (_rows.TryGetValue(id, out var record))
                    {
                        result.Add(record);
                    }
                }
            }
        }
        return result;
    }

    public IReadOnlyList<FeedRecord> Get(string namedKey, string value) =>
        Get(namedKey, Encoding.UTF8.GetBytes(value));

    /// <summary>Remove the row for a key plus its named-key postings.</summary>
    public bool Delete(byte[] key)
    {
        var id = Convert.ToBase64String(key);
        if (!_rows.TryRemove(id, out _))
        {
            return false;
        }
        RemovePostings(id);
        return true;
    }

    public bool Delete(string key) => Delete(Encoding.UTF8.GetBytes(key));

    /// <summary>Empty the primary map and every named-key index.</summary>
    public void Clear()
    {
        _rows.Clear();
        foreach (var postings in _namedIndex.Values)
        {
            postings.Clear();
        }
    }

    /// <summary>
    /// Copy every row into <paramref name="target"/>, re-attaching named-key
    /// postings; missing named keys are declared on the target.
    /// </summary>
    public void Copy(RecordStore target)
    {
        foreach (var namedKey in NamedKeys)
        {
            target.AddKey(namedKey);
        }
        foreach (var (id, record) in _rows)
        {
            target.CommitUpsert(record, id, NamedValuesOf(id));
        }
    }

    public int Count => _rows.Count;

    /// <summary>Apply <paramref name="action"/> to a stable snapshot of every row.</summary>
    public void ForEach(Action<Entry> action)
    {
        foreach (var entry in Snapshot())
        {
            action(entry);
        }
    }

    /// <summary><see cref="ForEach"/> spread over <paramref name="threads"/> worker threads.</summary>
    public void ForEachParallel(int threads, Action<Entry> action)
    {
        if (threads < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(threads), "threads must be >= 1");
        }
        Parallel.ForEach(Snapshot(), new ParallelOptions { MaxDegreeOfParallelism = threads },
            entry => action(entry));
    }

    public IEnumerator<Entry> GetEnumerator() => Snapshot().GetEnumerator();
    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>Row-level write builder returned by <see cref="RecordStore.Upsert(FeedRecord)"/>.</summary>
    public sealed class UpsertBuilder
    {
        private readonly RecordStore _store;
        private readonly FeedRecord _record;
        private string _primaryId;
        private readonly Dictionary<string, byte[]> _named = new();

        internal UpsertBuilder(RecordStore store, FeedRecord record)
        {
            _store = store;
            _record = record;
            _primaryId = DefaultId(record);
        }

        /// <summary>Choose the row's primary key.</summary>
        public UpsertBuilder WithKey(byte[]? key)
        {
            var id = key is { } bytes ? Convert.ToBase64String(bytes) : DefaultId(_record);
            if (id != _primaryId)
            {
                _store._rows.TryRemove(_primaryId, out _);
                _store.RemovePostings(_primaryId);
                _primaryId = id;
            }
            Commit();
            return this;
        }

        public UpsertBuilder WithKey(string key) => WithKey(Encoding.UTF8.GetBytes(key));

        /// <summary>Add a named-key value (must be declared via <see cref="AddKey"/>).</summary>
        public UpsertBuilder WithNamedKey(string namedKey, byte[] value)
        {
            _named[namedKey] = (byte[])value.Clone();
            Commit();
            return this;
        }

        public UpsertBuilder WithNamedKey(string namedKey, string value) =>
            WithNamedKey(namedKey, Encoding.UTF8.GetBytes(value));

        private void Commit() => _store.CommitUpsert(_record, _primaryId, _named);
    }

    internal void CommitUpsert(FeedRecord record, string primaryId, IReadOnlyDictionary<string, byte[]> named)
    {
        RemovePostings(primaryId);
        foreach (var (key, value) in named)
        {
            if (!_namedIndex.TryGetValue(key, out var postings))
            {
                throw new ArgumentException($"undeclared named key: {key}", nameof(named));
            }
            var ids = postings.GetOrAdd(Convert.ToBase64String(value), _ => new HashSet<string>());
            lock (ids)
            {
                ids.Add(primaryId);
            }
        }
        _rows[primaryId] = record;
    }

    private Dictionary<string, byte[]> NamedValuesOf(string primaryId)
    {
        var named = new Dictionary<string, byte[]>();
        foreach (var (key, postings) in _namedIndex)
        {
            foreach (var (valueId, ids) in postings)
            {
                bool present;
                lock (ids)
                {
                    present = ids.Contains(primaryId);
                }
                if (present)
                {
                    named[key] = Convert.FromBase64String(valueId);
                }
            }
        }
        return named;
    }

    private void RemovePostings(string primaryId)
    {
        foreach (var postings in _namedIndex.Values)
        {
            foreach (var (valueId, ids) in postings)
            {
                lock (ids)
                {
                    ids.Remove(primaryId);
                }
                if (IsEmptySet(ids))
                {
                    postings.TryRemove(valueId, out _);
                }
            }
        }
    }

    private static bool IsEmptySet(HashSet<string> ids)
    {
        lock (ids)
        {
            return ids.Count == 0;
        }
    }

    private List<Entry> Snapshot() =>
        _rows.Select(pair => new Entry(Convert.FromBase64String(pair.Key), pair.Value)).ToList();

    private static string DefaultId(FeedRecord record) =>
        Convert.ToBase64String(record.Key is { } key
            ? key
            : Encoding.UTF8.GetBytes("cursor:" + record.Cursor.Value));
}

/// <summary>
/// A running feed subscription: a named Reader session polled on a
/// background task, invoking <c>onRecords</c> per fetched page and
/// acknowledging durable progress after each delivered batch.
///
/// Error policy: transient refusals (<see cref="WhitewaterException.Unavailable"/>,
/// <see cref="WhitewaterException.AmbiguousResult"/>) retry after a short pause.
/// <see cref="WhitewaterException.StaleSession"/> — another client opened the
/// same Reader — terminates the subscription rather than fencing back. Any
/// other failure also terminates. Terminal errors reach <c>onError</c> and
/// <see cref="Failure"/>.
/// </summary>
public sealed class Subscription : IAsyncDisposable
{
    private static readonly TimeSpan EmptyPoll = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(1);

    private readonly CancellationTokenSource _stop = new();
    private readonly Task _loop;

    internal Subscription(
        WhitewaterClient client,
        string readerName,
        string feed,
        ReaderStart start,
        int capacity,
        Func<IReadOnlyList<FeedRecord>, Task> onRecords,
        Action<WhitewaterException>? onError)
    {
        _loop = Run(client, readerName, feed, start, capacity, onRecords, onError);
    }

    /// <summary>False once the loop has exited (closed or failed).</summary>
    public bool Running => !_loop.IsCompleted;

    /// <summary>The terminal error, when the loop stopped on a failure.</summary>
    public WhitewaterException? Failure { get; private set; }

    private int _disposed;

    /// <summary>Signal stop, release the Reader session, and await the loop.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }
        await _stop.CancelAsync().ConfigureAwait(false);
        await _loop.ConfigureAwait(false);
        _stop.Dispose();
    }

    private async Task Run(
        WhitewaterClient client,
        string readerName,
        string feed,
        ReaderStart start,
        int capacity,
        Func<IReadOnlyList<FeedRecord>, Task> onRecords,
        Action<WhitewaterException>? onError)
    {
        var ct = _stop.Token;
        try
        {
            await EnsureReader(client, readerName, feed, start, ct).ConfigureAwait(false);
            var reader = client.Reader(readerName);
            var session = await OpenWithRetry(reader, capacity, ct).ConfigureAwait(false);
            if (session is null)
            {
                return;         // stopped while waiting to open
            }
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var page = await reader.FetchAsync(session, capacity, ct: ct).ConfigureAwait(false);
                        if (page.Records.Count > 0)
                        {
                            await onRecords(page.Records).ConfigureAwait(false);
                            if (page.DeliveredCursor.HasValue)
                            {
                                session = await reader
                                    .AckAsync(session, page.DeliveredCursor.Value, ct: ct)
                                    .ConfigureAwait(false);
                            }
                        }
                        else
                        {
                            await Task.Delay(EmptyPoll, ct).ConfigureAwait(false);
                        }
                    }
                    catch (WhitewaterException error) when (Retryable(error))
                    {
                        await Task.Delay(RetryDelay, ct).ConfigureAwait(false);
                    }
                }
            }
            finally
            {
                try
                {
                    await reader.CloseAsync(session).ConfigureAwait(false);
                }
                catch (WhitewaterException)
                {
                    // Session may already be superseded; release is best-effort.
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Stop requested — not a failure.
        }
        catch (WhitewaterException error)
        {
            Failure = error;
            onError?.Invoke(error);
        }
    }

    /// <summary>Declare the Reader when missing; an existing Reader is reused as-is.</summary>
    private static async Task EnsureReader(
        WhitewaterClient client, string readerName, string feed, ReaderStart start,
        CancellationToken ct)
    {
        try
        {
            await client.CreateReaderAsync(readerName, feed, start, ct).ConfigureAwait(false);
        }
        catch (WhitewaterException.Refused error)
        {
            if (!error.Message.Contains("exists", StringComparison.OrdinalIgnoreCase))
            {
                throw;
            }
        }
    }

    private static async Task<ReaderSession?> OpenWithRetry(
        ReaderSessionClient reader, int capacity, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                return await reader.OpenAsync(capacity, ct).ConfigureAwait(false);
            }
            catch (WhitewaterException error) when (Retryable(error))
            {
                await Task.Delay(RetryDelay, ct).ConfigureAwait(false);
            }
        }
        return null;
    }

    private static bool Retryable(WhitewaterException error) =>
        error is WhitewaterException.Unavailable or WhitewaterException.AmbiguousResult;
}
