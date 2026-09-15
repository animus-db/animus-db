# Split assertions by *property class*, not just by layer: safety scales to adversarial depth, eventual/liveness properties do not.

**Split assertions by *property class*, not just by layer: safety scales to
adversarial depth, eventual/liveness properties do not.** Serializability is a
*safety* property — it must hold on every interleaving, so it is sound to assert
as a hard check across a deep, fault-heavy, many-seed corpus (it held 7,560/7,560
in the Elle deep tier). Convergence + durability are *eventual* properties
(anti-entropy + coordinator retry) — "did it converge within the test's fixed
post-heal drain?" is only sound on a bounded, non-pathological set. At seed-depth
a compound fault (`lossy`+`stop_restart`) can legitimately leave convergence in
flight when the drain ends — observed on **both** the pure-Accord and
data-plane-frontier topologies (opposite seeds), with **no** safety violation. So
scale the safety check to depth; keep the eventual checks bounded (or, later,
give them a *converged-or-timeout* poll instead of a fixed-drain snapshot). A
fixed-deadline assertion on an eventual property reads as a flaky test, not a
bug. (ADR 0014 deep-tier findings.)
