# A new per-node handle belongs on `ClusterEdgeState`, and a free dial helper takes its value from a pure function

**Context.** ADR 0073 P2-C needed a per-node `ClusterFeatures` handle, a per-node version profile
and a halt cell reachable from `ClientCtx` consumers, plus a handshake `ext` for dial helpers that
have no ctx (`client_request_pipelined`, `connect_client`).

**Lessons.**
- `ClusterEdgeState::new()` is already constructed once per node at every site (production
  assembly, data-only, every `SimCluster` ctx builder, in-crate tests). A field there adds the
  handle everywhere with zero struct-literal fan-out; a new `ClientCtx` field would have meant
  touching ~11 literals (compiler-enumerated E0063s).
- Never a `static`/`OnceLock` for it: `SimEnv` runs many nodes (different "binaries") in one
  process. A free helper with no ctx gets its value from a *pure function of compile-time
  constants* (`own_ext()`), which is correct precisely because one process is one binary; the
  per-node variation (tests) goes through the ctx-bearing sites (`ctx.edge.version().profile()`).
- A write-once cell in the process-boundary `main.rs` (the version-halt exit code) is fine where
  threading a typed error through seven `Result<(), String>` entry points (all mapped to "usage")
  would be a large unrelated change.
