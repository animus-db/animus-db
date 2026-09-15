# A serializability checker must observe the layer that *claims* serializability, not an eventually-consistent projection of it.

**A serializability checker must observe the layer that *claims* serializability,
not an eventually-consistent projection of it.** The Elle corpus observed Accord
through the **AP data-plane frontier** (a current quorum read); under a
data-replica fault a committed multi-key write is acked *before* it is
quorum-durable (fire-and-forget), so a later read can see one key's new value but
not the other's — a torn read that `check_cycles` correctly flags as a cycle,
even though **Accord's order is fine**. The signature is unmistakable: cycle-only
failures, **never** no-fault, convergence + durability always green. Fix is *not*
to weaken the checker — point it at the serialization authority (pure Accord:
local execution + versioned-snapshot reads, `Topology::Authoritative`) and check
the AP frontier for **convergence + durability** only. (ADR 0014 topology split.)
