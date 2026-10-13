using System.Text.Json.Nodes;
using Whitewater.Internal;

namespace Whitewater;

/// <summary>One named-Writer session: idempotent appends fenced by epoch.</summary>
public sealed class WriterSessionClient
{
    private readonly Transport _transport;
    private readonly string _writer;
    private readonly long _sessionEpoch;
    private readonly bool _compression;

    internal WriterSessionClient(Transport transport, string writer, long sessionEpoch, bool compression = false)
    {
        _transport = transport;
        _writer = writer;
        _sessionEpoch = sessionEpoch;
        _compression = compression;
    }

    public WriterSessionClient UseCompression(bool enabled = true) =>
        new(_transport, _writer, _sessionEpoch, enabled);

    /// <summary>Idempotent single append; the same requestId dedups on the server.</summary>
    public async Task<AppendResult> AppendAsync(
        byte[]? key,
        byte[] payload,
        Guid? requestId = null,
        long? eventTimeNs = null,
        IReadOnlyDictionary<string, byte[]>? metadata = null,
        CancellationToken ct = default)
    {
        var body = Wire.AppendRequest(
            requestId ?? Guid.NewGuid(),
            _writer,
            _sessionEpoch,
            key,
            payload,
            eventTimeNs,
            metadata ?? new Dictionary<string, byte[]>());
        if (_compression) body["use_compression"] = true;
        return Wire.AppendResultOf(await _transport
            .PostAsync("/v1/writers/append", body, ct)
            .ConfigureAwait(false));
    }

    /// <summary>One epoch-fenced batch; each record carries its own requestId.</summary>
    public async Task<AppendBatchResult> AppendBatchAsync(
        IReadOnlyList<AppendInput> records, CancellationToken ct = default)
    {
        var array = new JsonArray();
        foreach (var record in records)
        {
            var body = Wire.AppendRequest(
                record.RequestId ?? Guid.NewGuid(),
                _writer,
                _sessionEpoch,
                record.Key,
                record.Payload,
                record.EventTimeNs,
                record.Metadata ?? new Dictionary<string, byte[]>());
            if (_compression) body["use_compression"] = true;
            array.Add(body);
        }
        var response = await _transport
            .PostAsync("/v1/writers/append-batch", new JsonObject { ["records"] = array }, ct)
            .ConfigureAwait(false);
        var results = new List<AppendResult>();
        if (response?["results"] is JsonArray resultsNode)
        {
            foreach (var item in resultsNode)
            {
                results.Add(Wire.AppendResultOf(item));
            }
        }
        return new AppendBatchResult(results, Wire.Feedback(response?["feedback"]));
    }

    /// <summary>One record in a batch append.</summary>
    public sealed record AppendInput(
        byte[]? Key,
        byte[] Payload,
        Guid? RequestId = null,
        long? EventTimeNs = null,
        IReadOnlyDictionary<string, byte[]>? Metadata = null);
}
