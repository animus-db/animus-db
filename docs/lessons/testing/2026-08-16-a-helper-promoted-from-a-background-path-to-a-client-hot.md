# A helper promoted from a background path to a client hot path carries its background-tuned cadence with it — bench the promoted path itself, never assume the promotion is free

**A helper promoted from a background path to a client hot path carries
its background-tuned cadence with it — bench the promoted path itself,
never assume the promotion is free** (2026-08-16, ADR 0049 Train A rung
5's bench gate). `cp_kind_raw_local` was built for the GSI drain's
footprint/cursor writes, where its flat 10ms confirm-poll sleep was
irrelevant; ADR 0049 quietly made it the confirm for *every* plain
Dynamo/CQL/raw-protocol write, so nearly every sequential client write
ate one whole 10ms tick — a 2.9× sequential-latency regression
(13.6 ms/op vs 4.7 pre-train on the ADR's own bench) that every
correctness gate ran green through, caught only because the ADR had
demanded a before/after measurement as a shipping gate. The plain path's
own confirm had used a 200µs-start exponential back-off all along; the
fix was giving the promoted helper the same one. Corollary: when an ADR
names an expected perf envelope ("low single-digit percent"), build the
measurement into the train as a gate — this one paid for itself on its
first run.
