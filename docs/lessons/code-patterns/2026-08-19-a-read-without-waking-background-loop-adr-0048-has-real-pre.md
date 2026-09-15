# A read-without-waking background loop (ADR 0048) has real, pre-existing building blocks — verify against the source before assuming a wake is unavoidable, don't just gate it and hope.

**A read-without-waking background loop (ADR 0048) has real, pre-existing
building blocks — verify against the source before assuming a wake is
unavoidable, don't just gate it and hope.** Building the TTL reaper
(ADR 0051 §6, `crates/animusd/src/ttl_reaper.rs`) needed a scan that
never wakes a quiesced `CpGroup`. Rather than trust the ADR's prose,
reading `animus-cp-data`'s actual source confirmed `local_get_kind`/
`local_scan_kind`/`pending_changes` are pure `self.storage.{get,scan}`
calls with **no** path anywhere near `RaftKvNode::wake`/`WakeSignal`/
`RaftCore` — they never touch the consensus loop at all, so they
structurally cannot reset a group's idle-activity clock. That made the
"scan without waking, wake only to act" design directly buildable with
existing primitives, not a new mechanism. The general rule: before
reporting a documented contract undeliverable (or silently violating it),
read the primitive's own implementation — a `local_*`-prefixed accessor
in this codebase is a strong (but still worth confirming) naming signal
that it bypasses the network/consensus path entirely.
