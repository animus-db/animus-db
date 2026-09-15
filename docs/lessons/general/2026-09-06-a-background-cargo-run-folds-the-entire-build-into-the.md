# A background `cargo run` folds the ENTIRE build into the diagnostic log it is meant to make useful — build synchronously first, then exec the already-compiled binary (S-07d, `scripts/e2e-kind.sh` operator log, issue #661)

Companion finding to the entry above, surfaced investigating the same
incident: the out-of-cluster operator's log
(`scripts/e2e-kind.sh`'s `$OPERATOR_LOG`) was reported to contain "only
cargo output and zero tracing lines" even with `RUST_LOG=info` exported —
suspicious, since `EnvFilter::from_default_env()` with `RUST_LOG=info`
set does correctly enable `info!` output (confirmed directly: reading
`tracing-subscriber` 0.3.23's own source, `from_default_env()`'s default
directive is `LevelFilter::ERROR` *only when the env var is absent*; with
it set to `info` every `info!`/`warn!` in `animus-operator::controller`,
including its own `"reconciling AnimusCluster"` `info!` line, passes the
filter as expected — reproduced directly against the built binary). The
real mechanism was mundane and had nothing to do with the filter: the
script's `phase "run operator out-of-cluster"` block backgrounded `exec
cargo run -p animus-operator -- run` directly, with **nothing earlier in
the script warming the build cache** — `animus-operator` is the *only*
`cargo` invocation in the whole script, so `kube-rs`'s heavy dependency
tree (`rustls`/`hyper`/`k8s-openapi`/`kube-runtime` and friends) compiles
for the first time exactly at this point, measured here at 40-50s even
with several of those crates already warm from other builds in the same
session — genuinely cold, on a slower or more contended CI box, easily
several minutes. Every line that landed in `$OPERATOR_LOG` during that
window was `cargo`'s own `Compiling ...` chatter; a process that is
*still compiling* is indistinguishable, from the log alone, from one that
is stuck.

**Fix**: split the phase in two — `cargo build -p animus-operator --bin
animus-operator` runs synchronously (its own output goes straight to the
terminal, not into `$OPERATOR_LOG`) *before* anything is backgrounded;
the background phase then `exec`s the already-compiled binary directly
(`$CARGO_TARGET_DIR/debug/animus-operator`, resolved the same way `cargo`
itself would) instead of `cargo run`. Every line that can ever land in
`$OPERATOR_LOG` is now genuine runtime tracing output. Side benefit: this
also removes the `cargo run`-as-supervisor indirection `cleanup()`'s own
comment had already had to work around with a belt-and-suspenders `pkill`
(kept, now clearly redundant, since `kill "$OPERATOR_PID"` reaches the
real process directly).

**General form**: backgrounding `cargo run` (rather than a pre-built
binary) folds build time into whatever log/timeout budget the *run* was
supposed to get — indistinguishable from a hang unless you already know
to discount it. Any script that (a) backgrounds a `cargo run` and (b)
treats its stdout/stderr as a liveness signal should build first,
synchronously, and run the resulting binary directly.
