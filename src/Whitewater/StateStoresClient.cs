using System.Text.Json.Nodes;
using Whitewater.Internal;

namespace Whitewater;

/// <summary>
/// Collection-level StateStore operations: declaration and drop. Row
/// operations live on the bound <see cref="StateStore"/> facade returned
/// by <see cref="WhitewaterClient.StateStore"/>.
/// </summary>
public sealed class StateStoresClient
{
    private readonly Transport _transport;

    internal StateStoresClient(Transport transport) => _transport = transport;

    /// <summary>
    /// Declare <paramref name="spec"/> on the Control Plane and return it
    /// bound to this client. <paramref name="source"/> names a same-Domain
    /// Feed the store materializes from; null means a manual store
    /// (required when the spec declares named keys).
    /// </summary>
    public async Task<StateStore> DefineAsync(
        StateStore spec, string? source = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(spec);
        if (spec.NamedKeys.Count > 0 && source != null)
        {
            throw new ArgumentException(
                "named keys require a manual StateStore until a declared "
                + "source-Feed projection exists", nameof(source));
        }
        var namedKeys = new JsonArray();
        foreach (var key in spec.NamedKeys)
        {
            namedKeys.Add(new JsonObject { ["name"] = key.Name, ["unique"] = key.Unique });
        }
        var sourceNode = new JsonObject();
        if (source != null)
        {
            sourceNode["kind"] = "feed";
            sourceNode["feed"] = WhitewaterClient.DottedName(source);
        }
        else
        {
            sourceNode["kind"] = "manual";
        }
        var body = new JsonObject
        {
            ["request_id"] = Guid.NewGuid().ToString(),
            ["commands"] = new JsonArray(new JsonObject
            {
                ["command"] = "define_state_store",
                ["name"] = spec.Name,
                ["source"] = sourceNode,
                ["named_keys"] = namedKeys,
            }),
        };
        Wire.ControlExecution(await _transport
            .PostAsync("/v1/admin/commands", body, ct).ConfigureAwait(false));
        return spec.Bind(_transport);
    }

    /// <summary>Tombstone the named store.</summary>
    public Task<ControlExecution> DropAsync(string name, CancellationToken ct = default) =>
        StateStore.Bound(_transport, name).DropAsync(ct);
}
