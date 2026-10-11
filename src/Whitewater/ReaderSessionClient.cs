using System.Text.Json.Nodes;
using Whitewater.Internal;

namespace Whitewater;

/// <summary>
/// One named-Reader session: open/fetch/ack/close with durable resume.
/// Delivery is at-least-once; acknowledge the page's delivered cursor.
/// </summary>
public sealed class ReaderSessionClient
{
    private readonly Transport _transport;
    private readonly string _reader;

    internal ReaderSessionClient(Transport transport, string reader)
    {
        _transport = transport;
        _reader = reader;
    }

    public Task<ReaderSession> OpenAsync(int capacity = 64, CancellationToken ct = default)
        => OpenAsync(capacity, Guid.NewGuid(), ct);

    public async Task<ReaderSession> OpenAsync(
        int capacity, Guid requestId, CancellationToken ct = default)
    {
        var body = new JsonObject
        {
            ["request_id"] = requestId.ToString(),
            ["reader"] = _reader,
            ["capacity"] = capacity,
        };
        return Wire.ReaderSession(await _transport
            .PostAsync("/v1/readers/open", body, ct)
            .ConfigureAwait(false));
    }

    public async Task<ReaderPage> FetchAsync(
        ReaderSession session,
        int? limit = null,
        Guid? requestId = null,
        CancellationToken ct = default)
    {
        var body = new JsonObject
        {
            ["request_id"] = (requestId ?? Guid.NewGuid()).ToString(),
            ["reader"] = _reader,
            ["session_epoch"] = session.SessionEpoch,
        };
        if (limit.HasValue)
        {
            body["limit"] = limit.Value;
        }
        return Wire.ReaderPage(await _transport
            .PostAsync("/v1/readers/fetch", body, ct)
            .ConfigureAwait(false));
    }

    /// <summary>Durably commit progress at a page's delivered cursor.</summary>
    public async Task<ReaderSession> AckAsync(
        ReaderSession session,
        Cursor cursor,
        Guid? requestId = null,
        CancellationToken ct = default)
    {
        var body = new JsonObject
        {
            ["request_id"] = (requestId ?? Guid.NewGuid()).ToString(),
            ["reader"] = _reader,
            ["session_epoch"] = session.SessionEpoch,
            ["cursor"] = cursor.Value,
        };
        return Wire.ReaderSession(await _transport
            .PostAsync("/v1/readers/ack", body, ct)
            .ConfigureAwait(false));
    }

    public async Task<ReaderSession> CloseAsync(
        ReaderSession session, CancellationToken ct = default)
    {
        var body = new JsonObject
        {
            ["request_id"] = Guid.NewGuid().ToString(),
            ["reader"] = _reader,
            ["session_epoch"] = session.SessionEpoch,
        };
        return Wire.ReaderSession(await _transport
            .PostAsync("/v1/readers/close", body, ct)
            .ConfigureAwait(false));
    }
}
