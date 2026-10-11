using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Whitewater.Internal;

/// <summary>
/// Minimal JSON-over-HTTP transport: round-robins the configured Node
/// endpoints, attaches the bearer key, and maps (status, code) pairs to
/// typed exceptions via <see cref="WhitewaterException.MapError"/>.
/// </summary>
internal sealed class Transport
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DictionaryKeyPolicy = JsonNamingPolicy.SnakeCaseLower,
    };

    internal static JsonSerializerOptions Json => JsonOptions;

    private readonly IReadOnlyList<Uri> _endpoints;
    private readonly HttpClient _http;
    private int _next;

    public Transport(IReadOnlyList<Uri> endpoints, string apiKey, TimeSpan timeout)
    {
        if (endpoints.Count == 0)
        {
            throw new ArgumentException("at least one endpoint is required", nameof(endpoints));
        }
        _endpoints = endpoints;
        _http = new HttpClient { Timeout = timeout };
        _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
        _http.DefaultRequestHeaders.Accept.ParseAdd("application/json");
    }

    private Uri Endpoint()
    {
        var index = (int)((uint)Interlocked.Increment(ref _next) % (uint)_endpoints.Count);
        return _endpoints[index];
    }

    public async Task<JsonNode?> GetAsync(string path, CancellationToken ct = default)
    {
        return await RoundTrip(() => new HttpRequestMessage(HttpMethod.Get, new Uri(Endpoint(), path)), ct)
            .ConfigureAwait(false);
    }

    public async Task<JsonNode?> PostAsync(string path, JsonNode body, CancellationToken ct = default)
    {
        return await RoundTrip(() =>
        {
            var request = new HttpRequestMessage(HttpMethod.Post, new Uri(Endpoint(), path))
            {
                Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json"),
            };
            return request;
        }, ct).ConfigureAwait(false);
    }

    private async Task<JsonNode?> RoundTrip(
        Func<HttpRequestMessage> requestFactory,
        CancellationToken ct)
    {
        HttpResponseMessage response;
        using (var request = requestFactory())
        {
            try
            {
                response = await _http.SendAsync(request, ct).ConfigureAwait(false);
            }
            catch (Exception error) when (error is HttpRequestException or TaskCanceledException)
            {
                throw new WhitewaterException.Unavailable(0, null, $"Node unreachable: {error.Message}");
            }
        }
        using (response)
        {
            var bytes = await response.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false);
            var body = Parse(bytes);
            var status = (int)response.StatusCode;
            if (status is >= 200 and < 300)
            {
                return body;
            }
            throw WhitewaterException.MapError(status, body as JsonObject);
        }
    }

    private static JsonNode? Parse(byte[] bytes)
    {
        if (bytes.Length == 0)
        {
            return null;
        }
        try
        {
            return JsonNode.Parse(bytes);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
