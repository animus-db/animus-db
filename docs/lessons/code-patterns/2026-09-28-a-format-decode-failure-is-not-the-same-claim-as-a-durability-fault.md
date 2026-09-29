# A format-decode failure is not the same claim as a durability fault — but check what the caller already committed before choosing "skip"

**What happened.** Wiring the ADR 0073 Phase 0 `CSN1` envelope onto the
control plane's system-keyspace `InstallSnapshot` image (`node.rs`'s
`install_syskv_image`), two wrong answers were each tempting in turn:

1. **Reuse the function's existing failure shape** — `assert!(halted, …)`,
   tolerated only while tearing down, a hard panic otherwise. That shape is
   right for the function's pre-existing failure, `engine.merge_batch(..)`
   returning `Err` (a physical engine-write fault), but a decode failure is
   a different claim: the bytes never touched the engine. ADR 0073 also
   requires a pre-baseline/unknown-version input to be a named `Err`, never
   a panic.
2. **"Refuse this transfer and let it be retried"** — log, install nothing,
   return `false`. This looks safe but is not: `pending_install` is only
   handed to the driver *after* the Raft core has finished the transfer and
   adopted the snapshot's `last_index`. Nothing re-sends it. Skipping the
   install leaves the node running with its engine silently behind its own
   Raft state — exactly the silent divergence the format tag exists to
   prevent.

The fix: log the named `FormatError` at `error`, install nothing, **set
`halted`**, and return `false` — the same treatment `drive` gives an
undecodable WAL. Loud, no panic, no divergence.

**What to do.**

- **Name the claim a failure makes before choosing how loud to make it.**
  "The engine failed to write" (durability) and "these bytes don't parse"
  (provenance) can flow through the same function and still deserve
  different responses.
- **Before choosing "skip and let it retry", find the retry.** Check what
  state the caller has *already* committed on the strength of this input
  (here: the Raft core adopted the snapshot before the driver ever saw the
  bytes). If nothing re-drives the operation, "skip" is a silent divergence,
  and a halt latch is the loud-but-non-panicking answer.
