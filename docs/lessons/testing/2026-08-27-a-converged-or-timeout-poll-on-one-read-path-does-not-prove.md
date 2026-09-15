# A converged-or-timeout poll on one read path does not prove convergence on a *different* read path over the same data — check which consistency contract each assertion actually rides before trusting an earlier poll's green result (issue #400).

**A converged-or-timeout poll on one read path does not prove convergence
on a *different* read path over the same data — check which consistency
contract each assertion actually rides before trusting an earlier poll's
green result (issue #400).** `tests/update_table_drop_index.rs::
create_drop_recreate_same_index_name_backfills_from_scratch` polled
`await_row_count` (the raw client protocol's linearizable `Scan`,
`stale: false`, direct against the recreated GSI's hidden table) to
convergence, then immediately followed it with a **fixed, one-shot**
per-row `Query` through the real DynamoDB wire — `assert!` on the very
first response, no poll at all. That `Query` can never request
`ConsistentRead: true` against a GSI (ADR 0041 §5 — a `ValidationException`),
so it is unconditionally served by ADR 0055's eventually-consistent,
replica-local path: the linearizable Scan proving the row durably present
says nothing about whether the *specific replica this Query happens to
land on* has caught up yet, and under real load (slower replica apply) it
routinely hadn't — the CI failure shape was `Count:0` reading back a row
the immediately-prior linearizable Scan had just confirmed present.
**General rule**: when a test chains two assertions against what looks
like "the same data," don't assume the second inherits the first's
convergence — trace which consistency mode each request actually rides
(a raw client-protocol call vs. the DynamoDB wire's `ConsistentRead`
default, and whether the operation in question can even ask for the
strong mode at all) and give **every** eventually-consistent assertion its
own converged-or-timeout poll, never borrow one poll's outcome for a
later, differently-served read. Fixed by wrapping each per-row `Query` in
a poll across every node's dynamo address (`await_gsi_query`/
`await_gsi_query_everywhere`, duplicated from `dynamo_query_pagination.rs`
per this file's own "sibling test modules keep their own fixtures
independent" convention — see that file's identical helpers for the
general pattern any GSI-`Query`-asserting test should follow).
(`crates/animusd/tests/update_table_drop_index.rs`.)
