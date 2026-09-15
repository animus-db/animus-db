# Making a simulator/executor handle `Clone`-able (when its fields are already `Arc`-backed shared state) is a small, safe, additive change worth making the moment a test needs to carry fault-injection capability *into* a spawned async task

**Making a simulator/executor handle `Clone`-able (when its fields are
already `Arc`-backed shared state) is a small, safe, additive change worth
making the moment a test needs to carry fault-injection capability *into* a
spawned async task** — don't route around the missing `Clone` with an
awkward workaround (a channel back to the outer synchronous scope, a
second parallel handle type, restructuring every scenario to interleave
fault injection from the outside). `animus-sim::Simulator` held only an
`Arc<Shared>` + a `u64` seed, had no `Drop`, and its per-node handle
(`SimEnv`) was already `Clone` for exactly this reason — so adding
`#[derive(Clone)]` to `Simulator` itself cost nothing and immediately
unblocked a harness where each scenario's own spawned "driver" task needs
to call `&self` fault methods (`stop`/`crash`/`partition_pair`/`heal`/
`env`) while the outer test thread keeps a separate handle for the `&mut
self` `run_for`/`run_until` driving loop. Check for a `Drop` impl and
whether every field is itself cheaply `Clone`-able before assuming a type
wasn't made `Clone` for a real reason — here it clearly wasn't, it just
hadn't been needed yet. (`animus-sim::Simulator`;
`animus-cp-data/tests/reconciler_corpus.rs`.)
