# When a seam trait is driven by a multi-threaded async runtime (`kube::runtime::Controller::run`, here), prefer `#[async_trait]` over hand-written RPITIT

**When a seam trait is driven by a multi-threaded async runtime
(`kube::runtime::Controller::run`, here), prefer `#[async_trait]` over
hand-written RPITIT** (`-> impl Future<Output = ...> + Send`) **for a new
test-fakeable seam, even though the trait is only ever used generically
and never as `dyn`** — RPITIT's implicit-capture rules and the fact that
`async fn` sugar in a trait does not itself require the resulting future
to be `Send` make it easy to write a seam that compiles standalone but
fails only when a caller needs `Send` (a multi-threaded reconciler, a
`tokio::spawn`ed task), several call sites away from the trait
definition. `async_trait` boxes the future and is already the
established pattern for every other seam trait in this workspace
(`animus-env`'s `Env`/`StorageEngine`, ADR 0061 rung E1's
`ClusterApi`/`AdminOps`) — matching it costs one small dependency and one
boxed allocation per call, never on a hot path for a controller
reconcile loop.
