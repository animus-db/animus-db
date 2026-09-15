# A shared CARGO_TARGET_DIR across concurrent agent sessions can produce phantom "method not found" errors for code that is actually present and correct

Found while building issue #667's fix, on a box running multiple concurrent
agent sessions all pointed at the same `CARGO_TARGET_DIR`.

## The symptom

A `cargo test -p animus-control` run failed with `E0599: no method named
'begin_cluster_check'/'refused_as_voter'/... found`, and `no variant named
'ClusterProbeResp' found`, even though:

- `grep`-ing the actual source file on disk showed every one of those
  methods/variants present, correctly, in the same `impl` block as other
  methods (`tick`, `handle`, `propose`) the SAME compile error run had no
  complaint about.
- The exact same command had SUCCEEDED moments earlier with no source
  changes in between, and later SUCCEEDED again after a `sleep` and retry
  with still no source changes — flapping between compiling and not
  compiling on an unchanged source tree.
- Building the identical crate/test against a **separate, temporary,
  isolated `CARGO_TARGET_DIR`** (not the shared one) succeeded immediately
  and consistently.

## Root cause

`ls $CARGO_TARGET_DIR/debug/deps/` showed **multiple different content-hash
versions of `libanimus_control-*.rlib`/`.rmeta`** coexisting — evidence of
concurrent `cargo` invocations (this session and at least one other,
sharing the directory) racing to write build artifacts for the same crate
at different points in its edit history. Cargo's fingerprinting sometimes
resolved a test binary's dependency against a **stale** rlib hash from
before the relevant edit, rather than the freshest one — hence "compiles
now, fails a minute later with no source change," not a real compile error
at all.

## The fix

Removing just that one crate's own stale artifacts —
`rm debug/deps/libanimus_control-*.{rlib,rmeta}` and the top-level
`debug/libanimus_control.{rlib,d}` copy — forced a clean, correct rebuild
without a full `cargo clean` (never do that to a directory other sessions
share live). This is safe: cargo regenerates whatever it actually needs on
the next build, and no other crate's artifacts are touched.

## The generalizable rule

On a shared `CARGO_TARGET_DIR`, a "method not found" or "variant not
found" error for code you can see is correct and present in the file
**is not proof of a real compile error** — it can be build-artifact
corruption from a concurrent writer. Before spending time debugging
"why doesn't Rust see my method": (1) `grep` the source to confirm the
API really is there, (2) try a build against a scratch, isolated
`CARGO_TARGET_DIR` to check whether the shared one is the problem, and
(3) if so, remove just the affected crate's own stale `libfoo-*.{rlib,
rmeta}` files from `debug/deps/` (plus the top-level `libfoo.{rlib,d}`
copy) rather than a wholesale `cargo clean` or an extended wait-and-retry
loop.
