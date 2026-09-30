# `provision_tablet` returning is not "this node hosts the group"

**Context.** `issue_298_conflict_tests::a_fresh_stage_pushes_a_decided_blockers_resolution_instead_of_conflicting`
(real-thread `ProdEnv`) flaked in CI with `this single node hosts the tablet`, from
`provision_and_await_leader`'s one-shot `node.edge.local_cp(tablet).expect(..)`.

**Lesson.** `ctx.provision_tablet(table)` returns once the tablet's `CreateTablet` and policy are in
control-plane `Metadata`. Hosting is a separate, async step: this node's `tablet_host_reconciler_loop`
must wake (a `metadata_watch` change, or the 500ms fallback tick), host the group and register it in
`ClusterEdgeState.raftkv`. Until then `local_cp` is `None`. "Metadata shows the tablet" is not "this
node hosts the group".

- Any `local_cp(..)` that follows `provision_tablet` or a Metadata read, with no successful write or
  leader wait in between, is asserting an eventual property one-shot. Make it a bounded
  converged-or-timeout poll (poll `local_cp` until `Some`, then `is_leader`, inside the same
  timeout budget). Do not widen the timeout to paper over it.
- A flake that recurs after a "fixed" race may be a second race in a different phase. The #734
  closure (`abort_background_tasks_for_test`) was correct and unrelated here. Read the panic's
  file:line before assuming it is the old failure.
- Red/green proof: temporarily sleep (e.g. 300ms, env-gated) before `reconciler.tick(&view)` in the
  reconciler loop. The unfixed fixture then fails every run with the same panic. Remove the shim
  before committing.
