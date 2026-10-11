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
}
