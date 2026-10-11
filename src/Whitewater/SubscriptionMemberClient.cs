using System.Text.Json.Nodes;
using Whitewater.Internal;

namespace Whitewater;

/// <summary>
/// One cooperating member of a named Subscription. Delivery is
/// at-least-once and fenced at commit: fetch under a held lease, then
/// ack the page's <c>Cursor</c>/<c>Positions</c> verbatim.
/// </summary>
public sealed class SubscriptionMemberClient
{
    private readonly Transport _transport;
    private readonly string _subscription;

    internal SubscriptionMemberClient(Transport transport, string subscription)
    {
        _transport = transport;
        _subscription = subscription;
    }

    private JsonObject LeaseBody(Joined member, Guid workId, long? leaseEpoch)
    {
        var body = new JsonObject
        {
            ["subscription"] = _subscription,
            ["request_id"] = Guid.NewGuid().ToString(),
            ["member_id"] = member.MemberId.ToString(),
            ["member_epoch"] = member.MemberEpoch,
            ["work_id"] = workId.ToString(),
        };
        if (leaseEpoch.HasValue)
        {
            body["lease_epoch"] = leaseEpoch.Value;
        }
        return body;
    }

    /// <summary>Join the Subscription; returns the fencing member epoch.</summary>
    public async Task<Joined> JoinAsync(Guid memberId, CancellationToken ct = default)
    {
        var body = new JsonObject
        {
            ["subscription"] = _subscription,
            ["request_id"] = Guid.NewGuid().ToString(),
            ["member_id"] = memberId.ToString(),
        };
        var response = await _transport
            .PostAsync("/v1/subscriptions/members/join", body, ct)
            .ConfigureAwait(false);
        return new Joined(
            memberId,
            response?["member_epoch"]?.GetValue<long>() ?? 0,
            response?["ownership_epoch"]?.GetValue<long>() ?? 0,
            response?["tick"]?.GetValue<long>() ?? 0);
    }

    /// <summary>Claim a work item under a bounded lease.</summary>
    public Task<WorkLease> ClaimAsync(
        Joined member, Guid workId, int? leaseTicks = null, CancellationToken ct = default)
        => LeaseAsync("/v1/subscriptions/members/claim", member, workId, null, leaseTicks, ct);

    /// <summary>Renew an outstanding lease.</summary>
    public Task<WorkLease> RenewAsync(
        Joined member, WorkLease lease, int? leaseTicks = null, CancellationToken ct = default)
        => LeaseAsync("/v1/subscriptions/members/renew", member, lease.WorkId,
            lease.LeaseEpoch, leaseTicks, ct);

    private async Task<WorkLease> LeaseAsync(
        string path, Joined member, Guid workId, long? leaseEpoch,
        int? leaseTicks, CancellationToken ct)
    {
        var body = LeaseBody(member, workId, leaseEpoch);
        if (leaseTicks.HasValue)
        {
            body["lease_ticks"] = leaseTicks.Value;
        }
        var response = await _transport.PostAsync(path, body, ct).ConfigureAwait(false);
        var lease = response?["lease"];
        if (lease == null)
        {
            throw new WhitewaterException.Conflict(
                409, "lease.refused", "work claim was not granted");
        }
        return new WorkLease(
            Guid.Parse(lease["work_id"]!.GetValue<string>()),
            Guid.Parse(lease["member_id"]!.GetValue<string>()),
            lease["member_epoch"]!.GetValue<long>(),
            lease["lease_epoch"]!.GetValue<long>(),
            lease["expires_at_tick"]?.GetValue<long>() ?? 0);
    }

    /// <summary>Release a held lease without acknowledging.</summary>
    public async Task ReleaseAsync(
        Joined member, WorkLease lease, CancellationToken ct = default)
    {
        await _transport
            .PostAsync("/v1/subscriptions/members/release",
                LeaseBody(member, lease.WorkId, lease.LeaseEpoch), ct)
            .ConfigureAwait(false);
    }

    /// <summary>Fetch a bounded page after the shared frontier.</summary>
    public async Task<MemberPage> FetchAsync(
        Joined member, WorkLease lease, int? limit = null, CancellationToken ct = default)
    {
        var body = LeaseBody(member, lease.WorkId, lease.LeaseEpoch);
        if (limit.HasValue)
        {
            body["limit"] = limit.Value;
        }
        return Wire.MemberPage(await _transport
            .PostAsync("/v1/subscriptions/members/fetch", body, ct)
            .ConfigureAwait(false));
    }

    /// <summary>Commit the fetched frontier; overlapping members are fenced here.</summary>
    public async Task AckAsync(
        Joined member, WorkLease lease, MemberPage page, CancellationToken ct = default)
    {
        var body = LeaseBody(member, lease.WorkId, lease.LeaseEpoch);
        if (page.Cursor.HasValue)
        {
            body["cursor"] = page.Cursor.Value.Value;
        }
        var positions = new JsonObject();
        foreach (var (range, cursor) in page.Positions)
        {
            positions[range] = cursor;
        }
        body["positions"] = positions;
        await _transport
            .PostAsync("/v1/subscriptions/members/ack", body, ct)
            .ConfigureAwait(false);
    }
}
