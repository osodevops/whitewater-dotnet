using System.Text.Json.Nodes;

namespace Whitewater.Internal;

/// <summary>
/// Wire codec: the only place request/response shapes are interpreted.
/// Binary fields are base64; signed 64-bit nanoseconds are decimal
/// strings; UUIDs serialize as lowercase hyphenated strings; metadata
/// maps to <c>metadata_base64</c> entries.
/// </summary>
internal static class Wire
{
    public static JsonObject AppendRequest(
        Guid requestId,
        string writer,
        long sessionEpoch,
        byte[]? key,
        byte[] payload,
        long? eventTimeNs,
        IReadOnlyDictionary<string, byte[]> metadata)
    {
        var metadataNode = new JsonObject();
        foreach (var (name, value) in metadata)
        {
            metadataNode[name] = Convert.ToBase64String(value);
        }
        var body = new JsonObject
        {
            ["request_id"] = requestId.ToString(),
            ["writer"] = writer,
            ["session_epoch"] = sessionEpoch,
            ["payload_base64"] = Convert.ToBase64String(payload),
            ["metadata_base64"] = metadataNode,
        };
        if (key != null)
        {
            body["key_base64"] = Convert.ToBase64String(key);
        }
        if (eventTimeNs.HasValue)
        {
            body["event_time_ns"] = eventTimeNs.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
        return body;
    }

    public static JsonObject FeedAppendRequest(string feed, FeedAppend record)
    {
        var metadata = new JsonObject();
        if (record.Metadata != null)
        {
            foreach (var (name, value) in record.Metadata)
            {
                metadata[name] = Convert.ToBase64String(value);
            }
        }
        var body = new JsonObject
        {
            ["request_id"] = record.RequestId.ToString(),
            ["feed"] = feed,
            ["writer_session_id"] = record.WriterSessionId.ToString(),
            ["writer_epoch"] = record.WriterEpoch,
            ["sequence"] = record.Sequence,
            ["key_base64"] = Convert.ToBase64String(record.Key ?? Array.Empty<byte>()),
            ["payload_base64"] = Convert.ToBase64String(record.Payload),
            ["metadata_base64"] = metadata,
        };
        if (record.EventTimeNs.HasValue)
        {
            body["event_time_ns"] = record.EventTimeNs.Value
                .ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
        return body;
    }

    public static TemporaryFetchPage TemporaryFetchPage(JsonNode? node)
    {
        return new TemporaryFetchPage(
            Records(node?["records"]),
            Cursor.Of(node?["next_cursor"]?.GetValue<string>()));
    }

    public static ServerFeedback Feedback(JsonNode? node)
    {
        return new ServerFeedback(
            node?["recommended_batch_count"]?.GetValue<int>() ?? 0,
            node?["recommended_batch_bytes"]?.GetValue<int>() ?? 0,
            node?["pressure"]?.GetValue<double>() ?? 0.0,
            node?["retry_after_ms"]?.GetValue<int>() ?? 0,
            node?["max_frame_bytes"]?.GetValue<long>() ?? 0,
            node?["hot_key"]?.GetValue<bool>() ?? false);
    }

    public static AppendResult AppendResultOf(JsonNode? node)
    {
        return new AppendResult(
            Guid.Parse(node?["message_id"]?.GetValue<string>() ?? throw Corrupt("message_id")),
            new Cursor(node["cursor"]!.GetValue<string>()),
            node["deduplicated"]?.GetValue<bool>() ?? false,
            Feedback(node["feedback"]));
    }

    public static FeedRecord Record(JsonNode? node)
    {
        if (node == null)
        {
            throw Corrupt("record");
        }
        var metadata = new Dictionary<string, byte[]>();
        if (node["metadata_base64"] is JsonObject metadataNode)
        {
            foreach (var entry in metadataNode)
            {
                metadata[entry.Key] = Convert.FromBase64String(entry.Value!.GetValue<string>());
            }
        }
        var keyNode = node["key_base64"];
        return new FeedRecord(
            new Cursor(node["cursor"]!.GetValue<string>()),
            Guid.Parse(node["message_id"]!.GetValue<string>()),
            Guid.Parse(node["producer_id"]!.GetValue<string>()),
            node["sequence"]!.GetValue<long>(),
            long.Parse(node["event_time_ns"]!.GetValue<string>(),
                System.Globalization.CultureInfo.InvariantCulture),
            long.Parse(node["ingest_time_ns"]!.GetValue<string>(),
                System.Globalization.CultureInfo.InvariantCulture),
            keyNode == null ? null : Convert.FromBase64String(keyNode.GetValue<string>()),
            Convert.FromBase64String(node["payload_base64"]!.GetValue<string>()),
            metadata);
    }

    public static List<FeedRecord> Records(JsonNode? node)
    {
        if (node is not JsonArray array)
        {
            return new List<FeedRecord>();
        }
        var records = new List<FeedRecord>(array.Count);
        foreach (var item in array)
        {
            records.Add(Record(item));
        }
        return records;
    }

    public static ReaderSession ReaderSession(JsonNode? node)
    {
        return new ReaderSession(
            Guid.Parse(node?["reader_id"]?.GetValue<string>() ?? throw Corrupt("reader_id")),
            Guid.Parse(node["feed_id"]!.GetValue<string>()),
            node["session_epoch"]!.GetValue<long>(),
            node["capacity"]?.GetValue<int>() ?? 0,
            Cursor.Of(node["delivered_cursor"]?.GetValue<string>()),
            Cursor.Of(node["acknowledged_cursor"]?.GetValue<string>()));
    }

    public static ReaderPage ReaderPage(JsonNode? node)
    {
        return new ReaderPage(
            Records(node?["records"]),
            Cursor.Of(node?["delivered_cursor"]?.GetValue<string>()),
            Cursor.Of(node?["acknowledged_cursor"]?.GetValue<string>()),
            node?["recommended_capacity"]?.GetValue<int>() ?? 0,
            node?["retry_after_ms"]?.GetValue<int>() ?? 0);
    }

    public static ControlExecution ControlExecution(JsonNode? node)
    {
        var results = new List<StatementResult>();
        if (node?["results"] is JsonArray array)
        {
            foreach (var item in array)
            {
                results.Add(new StatementResult(
                    item?["statement"]?.GetValue<string>() ?? "",
                    item?["message"]?.GetValue<string>() ?? "",
                    item?["data"]?.DeepClone() ?? new JsonObject()));
            }
        }
        return new ControlExecution(
            Guid.Parse(node?["request_id"]?.GetValue<string>() ?? throw Corrupt("request_id")),
            node?["revision"]?.GetValue<long>() ?? 0,
            node?["authority"]?.GetValue<string>() ?? "",
            node?["warning"]?.GetValue<string>() ?? "",
            results);
    }

    public static MemberPage MemberPage(JsonNode? node)
    {
        var positions = new Dictionary<string, string>();
        if (node?["positions"] is JsonObject positionsNode)
        {
            foreach (var entry in positionsNode)
            {
                positions[entry.Key] = entry.Value!.GetValue<string>();
            }
        }
        return new MemberPage(
            Records(node?["records"]),
            Cursor.Of(node?["cursor"]?.GetValue<string>()),
            positions,
            node?["ownership_epoch"]?.GetValue<long>() ?? 0);
    }

    private static InvalidOperationException Corrupt(string field)
    {
        return new InvalidOperationException($"Riverbed response missing {field}");
    }
}
