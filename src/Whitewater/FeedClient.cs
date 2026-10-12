using System.Text.Json.Nodes;
using Whitewater.Internal;

namespace Whitewater;

/// <summary>
/// Ad-hoc Feed operations: committed record reads, anonymous temporary
/// fetches, and single-shot appends with a caller-supplied writer
/// identity — no named session required.
/// </summary>
public sealed class FeedClient
{
    private readonly Transport _transport;

    internal FeedClient(Transport transport) => _transport = transport;

    /// <summary>Committed record history via GET /v1/feeds/records.</summary>
    public async Task<IReadOnlyList<FeedRecord>> RecordsAsync(
        string feed, int? limit = null, Cursor? after = null, CancellationToken ct = default)
    {
        var path = "/v1/feeds/records?feed=" + Uri.EscapeDataString(feed);
        if (limit.HasValue)
        {
            path += "&limit=" + limit.Value;
        }
        if (after.HasValue)
        {
            path += "&after=" + Uri.EscapeDataString(after.Value.Value);
        }
        return Wire.Records(await _transport.GetAsync(path, ct).ConfigureAwait(false));
    }

    /// <summary>
    /// Anonymous temporary Reader fetch: bounded page after a Cursor,
    /// <see cref="TemporaryFetchOptions.NewOnly"/> positioning, or
    /// <see cref="TemporaryFetchOptions.Tail"/> + wait long-poll.
    /// </summary>
    public async Task<TemporaryFetchPage> TemporaryFetchAsync(
        string feed, TemporaryFetchOptions? options = null, CancellationToken ct = default)
    {
        options ??= new TemporaryFetchOptions();
        var body = new JsonObject { ["feed"] = feed };
        if (options.After.HasValue)
        {
            body["after"] = options.After.Value.Value;
        }
        if (options.Limit.HasValue)
        {
            body["limit"] = options.Limit.Value;
        }
        if (options.NewOnly)
        {
            body["new_only"] = true;
        }
        if (options.Tail)
        {
            body["tail"] = true;
        }
        if (options.WaitMs.HasValue)
        {
            body["wait_ms"] = options.WaitMs.Value;
        }
        if (options.AfterEventTimeNs.HasValue)
        {
            body["after_event_time_ns"] = options.AfterEventTimeNs.Value
                .ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
        return Wire.TemporaryFetchPage(await _transport
            .PostAsync("/v1/readers/temporary/fetch", body, ct)
            .ConfigureAwait(false));
    }

    /// <summary>Ad-hoc append; identical <c>RequestId</c> retries dedupe.</summary>
    public async Task<AppendResult> AppendAsync(
        string feed, FeedAppend record, CancellationToken ct = default)
    {
        return Wire.AppendResultOf(await _transport
            .PostAsync("/v1/feeds/append", Wire.FeedAppendRequest(feed, record), ct)
            .ConfigureAwait(false));
    }

    /// <summary>
    /// Bulk-read committed Feed history once, from <paramref name="start"/>,
    /// delivering each page to <paramref name="sink"/> until the current
    /// frontier is drained. Returns the number of records delivered. Use
    /// <see cref="WhitewaterClient.Subscribe"/> when the sink should keep
    /// receiving.
    ///
    /// <see cref="ReaderStart.Now"/> loads nothing (the frontier is empty at
    /// call time); <c>Cursor</c>/<c>TimestampNs</c> resume from that point.
    /// </summary>
    public async Task<long> LoadAsync(
        string feed,
        ReaderStart start,
        Func<IReadOnlyList<FeedRecord>, Task> sink,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(start);
        ArgumentNullException.ThrowIfNull(sink);
        var options = FirstPage(start);
        long loaded = 0;
        while (true)
        {
            var page = await TemporaryFetchAsync(feed, options, ct).ConfigureAwait(false);
            if (page.Records.Count == 0)
            {
                return loaded;
            }
            await sink(page.Records).ConfigureAwait(false);
            loaded += page.Records.Count;
            options = TemporaryFetchOptions.Page(page.NextCursor, LoadPage);
        }
    }

    private const int LoadPage = 512;

    private static TemporaryFetchOptions FirstPage(ReaderStart start) => start.Kind switch
    {
        ReaderStart.StartKind.Now => TemporaryFetchOptions.AtEnd(),
        ReaderStart.StartKind.Cursor =>
            TemporaryFetchOptions.Page(new Cursor(start.CursorValue!), LoadPage),
        ReaderStart.StartKind.Timestamp =>
            new TemporaryFetchOptions(Limit: LoadPage, AfterEventTimeNs: start.TimestampNsValue),
        _ => TemporaryFetchOptions.Page(null, LoadPage),
    };
}
