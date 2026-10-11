using System.Text.Json.Nodes;
using Whitewater.Internal;
using Xunit;

namespace Whitewater.Tests;

/// <summary>Wire-shape tests: golden request encodings and response decodes.</summary>
public class WireTests
{
    [Fact]
    public void AppendRequestEncodesScalarsOnTheWire()
    {
        var requestId = Guid.Parse("019a0000-0000-7000-8000-000000000001");
        var body = Wire.AppendRequest(
            requestId, "orders.wr", 7,
            "key-1"u8.ToArray(), "payload"u8.ToArray(),
            long.MinValue,
            new Dictionary<string, byte[]> { ["source"] = "unit"u8.ToArray() });
        Assert.Equal(requestId.ToString(), body["request_id"]!.GetValue<string>());
        Assert.Equal("orders.wr", body["writer"]!.GetValue<string>());
        Assert.Equal(7, body["session_epoch"]!.GetValue<long>());
        Assert.Equal("a2V5LTE=", body["key_base64"]!.GetValue<string>());
        Assert.Equal("cGF5bG9hZA==", body["payload_base64"]!.GetValue<string>());
        // i64 nanoseconds serialize as a decimal string, never a JSON number
        Assert.Equal("-9223372036854775808", body["event_time_ns"]!.GetValue<string>());
        Assert.Equal("dW5pdA==", body["metadata_base64"]!["source"]!.GetValue<string>());
    }

    [Fact]
    public void RecordDecodesOpaqueCursorAndNanosecondStrings()
    {
        var node = JsonNode.Parse("""
            {
              "cursor": "AAAABBBBCCCCDDDD",
              "message_id": "019a0000-0000-7000-8000-000000000002",
              "producer_id": "019a0000-0000-7000-8000-000000000003",
              "sequence": 42,
              "event_time_ns": "9223372036854775807",
              "ingest_time_ns": "-5",
              "key_base64": "3q2+7w==",
              "payload_base64": "AAEC",
              "metadata_base64": {"m": "eA=="}
            }
            """);
        var record = Wire.Record(node);
        Assert.Equal("AAAABBBBCCCCDDDD", record.Cursor.Value);
        Assert.Equal(long.MaxValue, record.EventTimeNs);
        Assert.Equal(-5L, record.IngestTimeNs);
        Assert.Equal(new byte[] { 0xde, 0xad, 0xbe, 0xef }, record.Key);
        Assert.Equal(new byte[] { 0, 1, 2 }, record.Payload);
        Assert.Equal("x"u8.ToArray(), record.Metadata["m"]);
    }

    [Fact]
    public void CursorIsOpaqueAndNeverParsed()
    {
        const string opaque = "TSLWCDHsmGBMXtKGSBS3vfBmPrsJnt1cYxWFYi4TpTA";
        var cursor = new Cursor(opaque);
        Assert.Equal(opaque, cursor.Value);
        Assert.Equal(opaque, cursor.ToString());
        Assert.Null(Cursor.Of(null));
    }

    [Fact]
    public void FeedbackDecodesHotKeyFlagWithDefault()
    {
        var flagged = Wire.Feedback(JsonNode.Parse("""
            {"recommended_batch_count": 8, "recommended_batch_bytes": 65536,
             "pressure": 0.9, "retry_after_ms": 50,
             "max_frame_bytes": 67108864, "hot_key": true}
            """));
        Assert.True(flagged.HotKey);
        // Older servers omit the field; default is false.
        var cold = Wire.Feedback(JsonNode.Parse("""{"recommended_batch_count": 100}"""));
        Assert.False(cold.HotKey);
    }
}
