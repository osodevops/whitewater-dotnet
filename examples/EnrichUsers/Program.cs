using Whitewater;

// The permanent enrichment consumer: read users.update.request, look up
// the profile in a StateStore (primary key, then the declared "user_id"
// named key), emit users.update.enriched.request — or the DLQ when no
// profile matches — and bump day/week/month counters.
//
// The whole consumer loop is one statement:
//
//   await foreach (var msg in feed.ConsumeAsync("enricher", ct))
//   {
//       await ProcessAsync(msg);
//   }
//
// Open/fetch/ack/reopen/backoff/session-close stay inside the SDK — the
// named Reader's durable cursor resumes across restarts; a page acks
// only after every record delivered cleanly, so an exception redelivers.

var client = new WhitewaterClient(
    "whitewater-local-development-admin-key",
    new[]
    {
        new Uri("http://localhost:7371"),
        new Uri("http://localhost:7372"),
        new Uri("http://localhost:7373"),
    });

await client.CreateDomainAsync("app");
await client.CreateFeedAsync("app.users.update.request");
await client.CreateFeedAsync("app.users.update.enriched.request");
await client.CreateFeedAsync("app.users.update.dlq");
// Manual stores: profiles declares the "user_id" named secondary key the
// fallback lookup queries; stats takes named values on each Put.
await client.StateStores.DefineAsync(
    StateStore.Named("app.users.profiles").AddKey("user_id", unique: true));
await client.StateStores.DefineAsync(StateStore.Named("app.users.stats"));

var input = client.Feed(
    "app.users.update.request", Codecs.Json<UpdateRequest>());
var output = client.Feed(
    "app.users.update.enriched.request", Codecs.Json<EnrichedUpdate>());
var dlq = client.Feed("app.users.update.dlq", Codecs.Json<DeadLetter>());

var profiles = client.Store<string, UserProfile>(
    "app.users.profiles", Codecs.Utf8, Codecs.Json<UserProfile>());
var counters = client.Store<string, long>(
    "app.users.stats", Codecs.Utf8, Codecs.Json<long>());

var outWriter = await output.WriterAsync("enricher");
var dlqWriter = await dlq.WriterAsync("enricher");

// CTRL+C / SIGTERM drains the in-flight page, acks, closes the session.
using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    stop.Cancel();
};

await foreach (var msg in input.ConsumeAsync("enricher", stop.Token))
{
    var request = msg.Value;
    if (request is null)
    {
        continue;
    }

    // Value-level lookups: find by primary key (email), fall back to
    // the declared "user_id" named key — the second index.
    var profile = await profiles.FindAsync(request.UserEmail)
        ?? await profiles.FindByAsync("user_id", request.UserId);

    // Replay-safe effects: the request id pins the append so a crash
    // between append and ack redelivers the input but dedupes the emit.
    var effectId = DeterministicGuid(msg.MessageId);

    if (profile is null)
    {
        await dlqWriter.AppendEventAsync(new FeedEvent<string, DeadLetter>(
            new DeadLetter(
                "user not found on users.profiles",
                input.Name, msg.MessageId, msg.Cursor.Value, request),
            request.UserEmail, effectId));
        continue;
    }

    await outWriter.AppendEventAsync(new FeedEvent<string, EnrichedUpdate>(
        new EnrichedUpdate(
            request.RequestId, profile.UserId, profile.Email,
            profile.DisplayName, profile.Region, request.Changes),
        profile.UserId, effectId));

    var day = DateOnly.FromDateTime(DateTime.UtcNow);
    var isoWeek = System.Globalization.ISOWeek.GetWeekOfYear(day.ToDateTime(default));
    foreach (var bucket in new[]
    {
        $"updates.day.{day:yyyy-MM-dd}",
        $"updates.week.{day:yyyy}-W{isoWeek:D2}",
        $"updates.month.{day:yyyy-MM}",
    })
    {
        // getOrDefault: a miss counts as zero — counters stay one line.
        await counters.PutAsync(
            bucket, await counters.GetOrDefaultAsync(bucket, 0) + 1);
    }
}

static Guid DeterministicGuid(Guid seed)
{
    var bytes = seed.ToByteArray();
    bytes[7] = (byte)((bytes[7] & 0x0F) | 0x50);   // v5-style marker
    bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80);
    return new Guid(bytes);
}

public sealed record UpdateRequest(
    string RequestId, string UserEmail, string UserId,
    IReadOnlyDictionary<string, string>? Changes);

public sealed record UserProfile(
    string UserId, string Email, string DisplayName, string Region);

public sealed record EnrichedUpdate(
    string RequestId, string UserId, string Email, string DisplayName,
    string Region, IReadOnlyDictionary<string, string>? Changes);

public sealed record DeadLetter(
    string Reason, string SourceFeed, Guid SourceMessageId,
    string SourceCursor, UpdateRequest Original);
