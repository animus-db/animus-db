# When a staged intermediate value can be re-observed as "current state," evaluate-at-apply must recognize its own prior output, not just its own prior input (ADR 0054 step 4a, `TxnStage::pending`)

Moving `TransactWriteItems`' own evaluation into `KvCommand::TxnStage`'s
apply arm (mirroring step 3's `KvCommand::KindEval`) looked like a pure
copy of that arm's own "read the current value, evaluate, derive" shape —
until tracing what "the current value" can mean for a *staged* write, not a
directly-committed one. A `KindEval` write commits straight to the base
kind scope; nothing else in the system can make its own key look like "my
own prior output" before the entry itself even applies. A `TxnStage` write
instead merges a **provisional intent** onto the same physical key — and
that intent's own bytes *are* this entry's own already-computed new value,
sitting right where "the current committed value" would be read from on
any later re-processing of the identical log entry (an ordinary crash
restart with no intervening compaction routinely does this — see this
repo's own "engine_applied vs last_applied" note: the persisted applied
watermark advances only at compaction/install, not on every commit, so an
uncompacted restart replays the whole WAL against an already-intact
engine). A naive port of `KindEval`'s read-then-evaluate shape would read
that intent as if it were the pre-stage baseline and re-run `op` against
it — for a non-idempotent update (`ADD`), silently doubling the effect on
every restart that happens to replay the entry, with no error and no
visible symptom short of a wrong final number.

**The generalizable shape**: whenever moving evaluation into apply for a
producer that stages its result as an intermediate/held state (an intent,
a pending record, any "provisional" envelope a later step commits) rather
than writing the final value directly, check whether that intermediate
state can ever be the thing "read current state" reads back — and if it
can, the apply arm needs an explicit identity check (here: "does the key's
current envelope belong to *this exact* transaction already?") that
short-circuits evaluation and reuses the already-computed result verbatim,
rather than trusting a single "read → evaluate → write" shape to be safe
just because it was safe for a producer that commits directly. The fix
does not need a new mechanism — the already-computed value is *right
there* in the intent's own envelope, decoded by the same read that would
otherwise misinterpret it — but the bug is invisible to a differential
test built the `KindEval` way (propose once, compare to a hand-evaluated
`KindBatch`): the failure only appears if the *same log entry* is
processed a second time against a state its own first processing already
changed, which requires a same-engine restart/replay scenario specifically
(`tests/txn_kind_writes.rs::
pending_add_stage_survives_a_same_engine_restart_without_double_applying`),
not the "propose twice with two different entries" shape most concurrency
tests reach for.
