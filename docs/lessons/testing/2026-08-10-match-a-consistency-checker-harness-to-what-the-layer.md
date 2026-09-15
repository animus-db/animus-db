# Match a consistency-checker harness to what the layer *offers*; don't shoehorn a transactional workload onto a non-transactional layer — build a sibling harness that reuses the *checkers*, not the workload.

**Match a consistency-checker harness to what the layer *offers*; don't shoehorn
a transactional workload onto a non-transactional layer — build a sibling harness
that reuses the *checkers*, not the workload.** Adding an Elle corpus for the
leaderful Raft KV plane (ADR 0017), the obvious move was a `Topology` variant of
the Accord corpus — but that harness drives **multi-key transactions** and the
Raft plane is **single-tablet, non-transactional KV** (one key per op), so the
workload simply doesn't map; forcing it would mean an enum fork through every
method *and* a workload that misrepresents the plane. Instead a self-contained
`raftkv_linearizable.rs` reuses just the proven `check_cycles`/durability/
convergence + `Recorder` model over a single-key list-append workload. And note
the counter-intuitive soundness: **serializability is a sound, meaningful check
on a single linearizable Raft group** (not only on Accord) — the group *is* the
serialization authority, so a forked/stale read shows as a cycle; there's no
eventually-consistent read path to manufacture torn-read false positives (the
hazard that bans `check_cycles` on the AP `Frontier`). The teeth-proof
(`negative_control.rs`) is shared because the checker is.
