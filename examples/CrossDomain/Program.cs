using Whitewater;

// Cross-Domain Pipes: a Pipe is always declared inside one Domain — its
// name resolves there — but its Subscription source and output Feed or
// StateStore can point at resources in ANY Domain.
//
//   purchasing: orders feed + fulfil subscription (what sales commits)
//   warehouse:  inbound feed (what fulfilment works from)
//
// A plain string argument resolves inside the Pipe's owning Domain;
// a cross-Domain target is passed as a handle — warehouse.Feed("inbound"),
// purchasing.Subscription("fulfil") — or Scoped.Absolute("name"). The
// reference carries its full dotted name, so it can never be silently
// re-prefixed into the wrong Domain.
//
//   purchasing.DefineForwardPipeAsync(
//       "relay", "fulfil", warehouse.Feed("inbound"));
//   // DEFINE PIPE purchasing.relay FROM purchasing.fulfil
//   //   TO FEED warehouse.inbound
//
// The reverse ownership is just as natural — warehouse declares the
// Pipe and pulls from purchasing's Subscription:
//
//   warehouse.DefineForwardPipeAsync(
//       "pull", purchasing.Subscription("fulfil"), "inbound");
//   // DEFINE PIPE warehouse.pull FROM purchasing.fulfil
//   //   TO FEED warehouse.inbound

var client = new WhitewaterClient(
    "whitewater-local-development-admin-key",
    new[]
    {
        new Uri("http://localhost:7371"),
        new Uri("http://localhost:7372"),
        new Uri("http://localhost:7373"),
    });

var purchasing = await client.CreateDomainAsync("purchasing");
var warehouse = await client.CreateDomainAsync("warehouse");

await purchasing.CreateFeedAsync("orders");
await purchasing.CreateSubscriptionAsync("fulfil", "orders");
await warehouse.CreateFeedAsync("inbound");

// Declared in purchasing — the team that owns the orders feed —
// writing into the warehouse Domain's intake feed.
await purchasing.DefineForwardPipeAsync(
    "relay", "fulfil", warehouse.Feed("inbound"));

// Equivalent cross-Domain reference without a handle —
// Scoped.Absolute keeps the name verbatim wherever it is used.
await purchasing.DefineForwardPipeAsync(
    "relay2", "fulfil", Scoped.Absolute("warehouse.inbound"));

var pipe = purchasing.Pipe("relay");
var info = await pipe.DescribeAsync();
Console.WriteLine($"{info.Name} stage={info.Stage} status={info.Status}");
