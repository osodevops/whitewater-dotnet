using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Whitewater;

/// <summary>
/// A permanent, supervised consumer bound to a Feed's named Reader —
/// the durable-Reader equivalent of Kafka's <c>while (true) poll()</c>
/// loop with the plumbing removed:
///
/// <code>
/// await foreach (var msg in feed.Consumer("enricher").Messages(ct))
/// {
///     await ProcessAsync(msg);
/// }
///
/// // or a handler, completing only on DisposeAsync/ct:
/// await feed.Consumer("enricher").RunAsync(ProcessAsync, ct);
/// </code>
///
/// Semantics:
/// <list type="bullet">
/// <item>The named Reader's acked cursor is durable server-side — a
/// restart (app or session) resumes where it stopped; no offset
/// management.</item>
/// <item>Each fetched page is delivered record-by-record; its
/// delivered cursor acks only after every record's delivery succeeded —
/// a thrown handler (or an abandoned enumeration) leaves the page
/// unacked so it redelivers (at-least-once). Poison pills are the app's
/// job — route them to a DLQ feed.</item>
/// <item>Transient failures retry with exponential backoff inside the
/// supervisor; a fenced session re-opens and resumes. Startup refusals
/// (misconfiguration, denied auth) and silent deaths surface on
/// <see cref="Failure"/>/RunAsync — fail fast, never loop.</item>
/// <item><see cref="DisposeAsync"/> finishes the in-flight page,
/// releases the session, and lets Messages/RunAsync return — the
/// SIGTERM path. Safe inside a handler: signal-only there.</item>
/// </list>
/// </summary>
public sealed class FeedConsumer<TKey, TValue> : IAsyncDisposable
{
    private static readonly TimeSpan Poll = TimeSpan.FromMilliseconds(25);

    private readonly Feed<TKey, TValue> _feed;
    private readonly CancellationTokenSource _stop = new();

    private ReaderStart _start = ReaderStart.Beginning();
    private int _capacity = 128;
    private TimeSpan _initialBackoff = TimeSpan.FromMilliseconds(250);
    private TimeSpan _maxBackoff = TimeSpan.FromSeconds(30);
    private Func<FeedMessage<TKey, TValue>, ValueTask>? _onRecord;
    private Action<Exception>? _onError;
    private Task? _supervise;
    private bool _inHandler;
    private int _started;
    private int _disposed;

    internal FeedConsumer(Feed<TKey, TValue> feed, string reader)
    {
        _feed = feed;
        Reader = reader;
    }

    /// <summary>The declared Reader's name — its durable identity.</summary>
    public string Reader { get; }

    /// <summary>The last supervised failure (plumbing or handler).</summary>
    public Exception? LastError { get; private set; }

    /// <summary>The terminal error — a startup refusal or a silent
    /// subscription death. Null on a clean stop.</summary>
    public Exception? Failure { get; private set; }

    /// <summary>True while the supervise loop is active.</summary>
    public bool IsRunning { get; private set; }

    /// <summary>Where the Reader starts on first declaration (default
    /// BEGINNING); ignored once the Reader exists — it resumes from
    /// durable progress.</summary>
    public FeedConsumer<TKey, TValue> StartAt(ReaderStart start)
    {
        _start = start;
        return this;
    }

    /// <summary>Records fetched per page (default 128).</summary>
    public FeedConsumer<TKey, TValue> WithCapacity(int capacity)
    {
        _capacity = capacity;
        return this;
    }

    /// <summary>The per-record handler — required for
    /// <see cref="RunAsync(Func{FeedMessage{TKey, TValue}, ValueTask}, CancellationToken)"/>.
    /// A thrown exception leaves the page unacked so it redelivers;
    /// poison pills are the app's job (route to a DLQ feed).</summary>
    public FeedConsumer<TKey, TValue> OnRecord(
        Func<FeedMessage<TKey, TValue>, ValueTask> handler)
    {
        _onRecord = handler;
        return this;
    }

    /// <summary>Error sink for visibility — plumbing failures and
    /// handler errors alike. The loop continues regardless.</summary>
    public FeedConsumer<TKey, TValue> OnError(Action<Exception> handler)
    {
        _onError = handler;
        return this;
    }

    /// <summary>Restart backoff bounds (default 250ms → 30s).</summary>
    public FeedConsumer<TKey, TValue> WithBackoff(
        TimeSpan initial, TimeSpan max)
    {
        _initialBackoff = initial;
        _maxBackoff = max;
        return this;
    }

    /// <summary>Iterate decoded records forever — supervised like
    /// <see cref="RunAsync(Func{FeedMessage{TKey, TValue}, ValueTask}, CancellationToken)"/>.
    /// Breaking out (or a loop-body throw) disposes the consumer; a
    /// startup refusal propagates out of the enumeration.</summary>
    public async IAsyncEnumerable<FeedMessage<TKey, TValue>> Messages(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        Claim();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            ct, _stop.Token);
        var channel = Channel.CreateUnbounded<FeedMessage<TKey, TValue>>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
        _supervise = Task.Run(async () =>
        {
            try
            {
                await SuperviseAsync(
                    m => channel.Writer.WriteAsync(m, linked.Token), linked.Token)
                    .ConfigureAwait(false);
                channel.Writer.TryComplete();
            }
            catch (Exception error)
            {
                channel.Writer.TryComplete(error);
            }
        });
        try
        {
            await foreach (var message in channel.Reader
                .ReadAllAsync(linked.Token).ConfigureAwait(false))
            {
                yield return message;
            }
        }
        finally
        {
            linked.Cancel();
            _stop.Cancel();
            try
            {
                await _supervise.ConfigureAwait(false);
            }
            catch
            {
                // Terminal errors already surfaced through the channel.
            }
        }
    }

    /// <summary>Deliver decoded records to <paramref name="handler"/>
    /// one at a time until DisposeAsync or <paramref name="ct"/> —
    /// throwing only for startup refusals or a silent subscription
    /// death. Everything else is supervised forever.</summary>
    public async Task RunAsync(
        Func<FeedMessage<TKey, TValue>, ValueTask> handler,
        CancellationToken ct = default)
    {
        Claim();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            ct, _stop.Token);
        _supervise = SuperviseAsync(handler, linked.Token);
        await _supervise.ConfigureAwait(false);
    }

    /// <summary>Deliver decoded records to the configured
    /// <see cref="OnRecord"/> handler.</summary>
    public Task RunAsync(CancellationToken ct = default)
        => RunAsync(
            _onRecord ?? throw new InvalidOperationException(
                "OnRecord handler is required"),
            ct);

    /// <summary>Graceful stop: the in-flight page finishes and acks,
    /// the session releases, Messages/RunAsync return. Inside a handler
    /// it only signals — the supervise loop releases the session when
    /// the handler returns.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }
        await _stop.CancelAsync().ConfigureAwait(false);
        var supervise = _supervise;
        if (supervise is not null && !_inHandler)
        {
            try
            {
                await supervise.ConfigureAwait(false);
            }
            catch
            {
                // Shutdown must not rethrow a supervised failure; the
                // caller observed it via Failure/RunAsync.
            }
        }
        _stop.Dispose();
    }

    private void Claim()
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
        {
            throw new InvalidOperationException(
                $"consumer '{Reader}' already started — one supervise " +
                "lifetime per instance");
        }
    }

    private async Task SuperviseAsync(
        Func<FeedMessage<TKey, TValue>, ValueTask> deliver,
        CancellationToken ct)
    {
        IsRunning = true;
        var backoff = _initialBackoff;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                Exception? handlerError = null;
                Exception? disposeError = null;
                var sub = _feed.SubscribeDecoded(
                    Reader, _start, _capacity,
                    async messages =>
                    {
                        foreach (var message in messages)
                        {
                            // No ct check here: a page already being delivered
                            // runs to completion so it can ack — graceful
                            // drain beats mid-page abandonment (at-least-once
                            // would replay the whole page anyway).
                            try
                            {
                                _inHandler = true;
                                await deliver(message).ConfigureAwait(false);
                            }
                            catch (Exception error)
                            {
                                LastError = error;
                                handlerError = error;
                                throw;
                            }
                            finally
                            {
                                _inHandler = false;
                            }
                        }
                    });
                try
                {
                    while (sub.Running && !ct.IsCancellationRequested)
                    {
                        try
                        {
                            await Task.Delay(Poll, ct).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                        }
                    }
                }
                finally
                {
                    try
                    {
                        await sub.DisposeAsync().ConfigureAwait(false);
                    }
                    catch (Exception fault)
                    {
                        // A faulted subscription rethrows its death
                        // here — it is the cause, not a new failure.
                        disposeError = fault;
                    }
                }
                if (ct.IsCancellationRequested)
                {
                    return;
                }
                var failure = sub.Failure;
                if (failure is WhitewaterException.Refused)
                {
                    Failure = failure;   // refused = fix the request
                    throw failure;
                }
                var error = (Exception?)failure ?? handlerError ?? disposeError;
                if (error is null)
                {
                    // Died with nothing to supervise — a local error
                    // swallowed by the loop. Surface it instead of
                    // restarting silently forever.
                    Failure = new WhitewaterException(
                        0, null,
                        $"subscription '{Reader}' terminated without " +
                        "an error to supervise");
                    throw Failure;
                }
                if (handlerError is null && error is not WhitewaterException)
                {
                    // A local fault (validation, parse, programming) —
                    // restarting only replays the same instant death.
                    Failure = error;
                    throw error;
                }
                try
                {
                    _onError?.Invoke(error);
                }
                catch
                {
                    // Error sinks never break supervision.
                }
                try
                {
                    await Task.Delay(backoff, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                backoff = backoff < _maxBackoff
                    ? TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, _maxBackoff.Ticks))
                    : _maxBackoff;
            }
        }
        finally
        {
            IsRunning = false;
        }
    }
}
