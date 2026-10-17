namespace Whitewater;

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
                                // The stop token may already be cancelled —
                                // DisposeAsync waits for this page, so its ack
                                // must still reach the Node (graceful drain).
                                session = await reader
                                    .AckAsync(session, page.DeliveredCursor.Value)
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
