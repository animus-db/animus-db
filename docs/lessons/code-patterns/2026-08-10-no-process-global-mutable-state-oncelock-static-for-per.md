# No process-global mutable state (`OnceLock`/`static`) for per-instance concerns.

**No process-global mutable state (`OnceLock`/`static`) for per-instance
concerns.** It leaks across tests in one binary (multiple in-process clusters
share it) and conflates instances in any multi-tenant context. Thread state
through a per-instance context instead (the wire edges' `ClusterEdgeState` via
`ClientCtx`, not process statics). If you must keep a static, make sure tests
tear instances down (`Node::shutdown()`) and use unique names/keys per test.
