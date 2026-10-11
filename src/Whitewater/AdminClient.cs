using System.Text.Json.Nodes;
using Whitewater.Internal;

namespace Whitewater;

/// <summary>WCL script execution and Node health.</summary>
public sealed class AdminClient
{
    private readonly Transport _transport;

    internal AdminClient(Transport transport) => _transport = transport;

    public Task<ControlExecution> WclAsync(string script, CancellationToken ct = default)
        => WclAsync(script, Guid.NewGuid(), ct);

    public async Task<ControlExecution> WclAsync(
        string script, Guid requestId, CancellationToken ct = default)
    {
        var body = new JsonObject
        {
            ["request_id"] = requestId.ToString(),
            ["script"] = script,
        };
        return Wire.ControlExecution(await _transport.PostAsync("/v1/admin/wcl", body, ct)
            .ConfigureAwait(false));
    }

    /// <summary>
    /// Execute typed control commands, e.g.
    /// <c>[{"command": "show", "kind": "feeds"}]</c>.
    /// </summary>
    public async Task<ControlExecution> CommandsAsync(
        IReadOnlyList<JsonObject> commands, Guid? requestId = null, CancellationToken ct = default)
    {
        var array = new JsonArray();
        foreach (var command in commands)
        {
            array.Add(command.DeepClone());
        }
        var body = new JsonObject
        {
            ["request_id"] = (requestId ?? Guid.NewGuid()).ToString(),
            ["commands"] = array,
        };
        return Wire.ControlExecution(await _transport
            .PostAsync("/v1/admin/commands", body, ct)
            .ConfigureAwait(false));
    }

    /// <summary>Liveness of the contacted Node.</summary>
    public async Task<bool> HealthyAsync(CancellationToken ct = default)
    {
        var health = await _transport.GetAsync("/health", ct).ConfigureAwait(false);
        return health?["status"]?.GetValue<string>() == "ok";
    }
}
