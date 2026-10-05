# Compare a computed verdict to its oracle over one snapshot, never two reads

`roll_health_matches_dashboard_ladder` first asked the admin endpoint for the verdict and
then read `Metadata` separately to compute the expected ladder counts. `SimCluster::admin`
advances simulated time, and the repair loop moved the dead member's replica away in
between, so the oracle saw a healthy tablet while the endpoint had seen a quorum-lost one:
a deterministic-looking but wrong failure that no seed replay explains. Fix: expose the
verdict function over a caller-supplied snapshot (`SimClusterHandle::roll_health_over`) and
compute the oracle over that same snapshot with no simulated time between. General rule:
when a test compares a derived view against an independently computed expectation, both must
read one captured input, and the HTTP round trip is tested separately for shape only.

Related: when a Rust function ports a client-side (JS) rule, test equality by running the
real JS over an enumerated state table (`node` + extract the function text) rather than
restating the rule's expected outputs by hand; hand-written expectations drift with the port.
