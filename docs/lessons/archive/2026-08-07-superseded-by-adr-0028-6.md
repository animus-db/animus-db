# Superseded by ADR 0028

**Superseded by ADR 0028**: a fresh split child no longer needs handoff
seeding at all (it's a `StorageScope` over already-present shared-engine
data), so it forms exactly like a fresh whole-keyspace tablet — there is no
more "fresh split child vs. join" distinction to make. Retained for
historical record. **Distinguish "seed a fresh child" from "join an existing group empty" by a durable
monotonic signal, not a race.** A node *added* to a tablet's replica set by the
reconciler must host an **empty** group and catch up via `InstallSnapshot`; an
*original* replica of a fresh split must **seed** from its local handed-off data —
starting empty there loses data. Don't let a polling host-loop race the split hook
to decide which; gate on the tablet **epoch** (`INITIAL` = fresh split → leave it to
the hook; bumped by a reconfigure → a join → host empty). A deterministic signal
turns a data-loss race into a clean branch. (ADR 0017 D1 join-hosting.)
