using System.Text.Json.Nodes;
using Whitewater.Internal;
using Xunit;

namespace Whitewater.Tests;

/// <summary>Error classification: wire (status, code) -> typed exceptions.</summary>
public class ErrorTests
{
    private static WhitewaterException Map(int status, string body)
    {
        return WhitewaterException.MapError(status, JsonNode.Parse(body) as JsonObject);
    }

    [Fact]
    public void WireCodesMapToTypedExceptions()
    {
        var stale = Map(409, """{"error":"epoch superseded","code":"stale_epoch"}""");
        Assert.IsType<WhitewaterException.StaleSession>(stale);
        Assert.Equal("stale_epoch", stale.Code);

        foreach (var code in new[] { "lease.busy", "lease.stale_lease", "lease.capacity" })
        {
            var error = Map(409, $"{{\"error\":\"x\",\"code\":\"{code}\"}}");
            Assert.IsType<WhitewaterException.LeaseConflict>(error);
            Assert.Equal(code, error.Code);
        }
    }

    [Fact]
    public void StatusClassIsTheFallbackWithoutACode()
    {
        Assert.IsType<WhitewaterException.Conflict>(Map(409, """{"error":"fencing"}"""));
        Assert.IsType<WhitewaterException.Unavailable>(
            Map(503, """{"error":"frozen for split cutover; retry safely"}"""));
        Assert.IsType<WhitewaterException.AmbiguousResult>(Map(500, """{"error":"internal"}"""));
        Assert.IsType<WhitewaterException.Refused>(Map(400, """{"error":"bad name"}"""));
    }

    [Fact]
    public void StaleSequenceCodeStaysAConflictForRawAppends()
    {
        var error = Map(409, """{"error":"seq hole","code":"writer.stale_sequence"}""");
        Assert.IsType<WhitewaterException.Conflict>(error);
        Assert.Equal("writer.stale_sequence", error.Code);
    }

    [Fact]
    public void MalformedBodyFallsBackToStatusText()
    {
        var error = Map(502, "null");
        Assert.IsType<WhitewaterException.AmbiguousResult>(error);
        Assert.Contains("502", error.Message);
    }
}
