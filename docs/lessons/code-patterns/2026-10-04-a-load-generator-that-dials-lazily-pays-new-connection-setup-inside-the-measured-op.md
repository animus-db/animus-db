# A load generator that dials lazily pays connection setup inside the measured op; pre-dial before the phase clock

Context: adding a TLS client to `animus-bench` (B-01 follow-up, ADR 0064).

Each worker took a pooled connection if one existed and otherwise dialled
*after* the op's `started` timestamp, on its first op. With plain TCP that
cost is a loopback `connect()` and nobody noticed. A TLS handshake is orders of
magnitude larger (key exchange, certificate verification), and a worker that
first meets an op during the steady phase would put it in that op's latency
(and, being open-loop, in the queueing of everything behind it). The warm-up
phase hides this only when warm-up is long enough for every worker to see an op.

Fix: `run_phase` calls `Cluster::prewarm(connections)` before taking `t0`, so
the pool holds handshaken connections when the clock starts; the handshake lives
in `Conn::connect`, which is also the only thing a post-failure redial calls
(that one stays in the op's latency, deliberately, as before). Rule: when a
change makes connection establishment expensive, audit every place a
connection is created and ask which side of the measurement clock it is on;
"the pool is usually warm" is not a guarantee. The real-cluster TLS test
proves the wire works, not this property, so it is stated in the module docs
and the crate guide.
