# An ADR's prose is not the source of truth for whether a gap still exists — grep the actual code (and the test tree) for the mechanism before implementing the fix it describes.

**An ADR's prose is not the source of truth for whether a gap still exists —
grep the actual code (and the test tree) for the mechanism before implementing
the fix it describes.** Assigned to close ADR 0013's "cross-process schema-DDL
proposal forwarding is future work" gap, a `CLAUDE.md` engineering-practices
scan (the `is_relayable_command`/`propose_schema` entries already documented
above) plus a direct grep turned up that the relay (`ClientCtx::propose_schema`
— propose locally on the leader, else relay `ClientRequest::ProposeSchema` one
hop, with a broadcast fallback for a leader-less ADR 0030 growth node), the
gating allowlist, and a **dedicated per-process regression test**
(`animusd/tests/schema_ddl_relay.rs`, proving `CreateTableSchema`/
`SetTableMode`/the atomic `ReplaceTableSchema` all commit via a
follower-connected node) were already implemented and merged — the ADR text
had simply never been updated after the feature landed (probably in the same
PR that added `is_relayable_command` itself, under a different task name that
didn't reference ADR 0013 by number). No code changes were needed; the actual
work was updating ADR 0013's Decision/Consequences to state reality (it also
had two *other* stale "future work" claims in the same document — CQL keyspace
replication and atomic `ALTER TABLE`, both also already shipped — found only
by cross-checking every claim in the file, not just the one paragraph the task
named). **General check before starting any "close gap X" task: does the
mechanism already exist? A stale ADR describing a gap as open is itself a bug
to fix (a doc PR), and shipping a redundant/parallel implementation on top of
an already-working one would be worse than doing nothing.**
