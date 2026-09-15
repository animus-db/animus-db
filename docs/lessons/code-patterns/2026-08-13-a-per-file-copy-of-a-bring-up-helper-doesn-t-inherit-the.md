# A per-file copy of a bring-up helper doesn't inherit the shared helper's hard-won mitigations — and "it has its own helper" is invisible at the call site.

**A per-file copy of a bring-up helper doesn't inherit the shared helper's
hard-won mitigations — and "it has its own helper" is invisible at the call
site.** `control_mirror_restart.rs` carried a local `free_addr()` +
`start()` pair rather than using `tests/support`, so it never picked up
`restart_same_addrs`' bounded rebind retry for the documented port-TOCTOU. It
flaked under `cargo test --workspace` (`.expect("control-only node starts")`)
while passing in isolation and at 6× self-concurrency, because the thief is
*another binary's* probe. Two distinct defects, worth separating:
**(1) the missing retry**, fixed by the same bounded-rebind loop
`support::restart_same_addrs` uses — a same-address restart test structurally
*cannot* re-allocate around a thief, since rebinding the captured addresses is
the thing under test. **(2) A latent bug the retry would have masked**: the
file allocated its five ports with five *sequential* `free_addr()` calls, each
binding `:0`, reading the port, and dropping the listener before the next call
— so the OS was free to hand the same port back twice and configure a node with
`internal == client`. `support::free_addrs(n)` holds all `n` listeners until
they are all read precisely to prevent that; the local copy had lost the
reason. **The practices**: when a shared test helper exists, a per-file
reimplementation needs a stated reason (here the real one was "control-only
bring-up isn't `run_node_with`" — legitimate, but it should then have *ported*
the retry, not omitted it); and when you fix a flake in a copied helper, diff
it against the shared original rather than only patching the symptom, because
the copy has likely drifted in more than one way. Corollary: a helper whose
correctness rests on *when a resource is released* (holding listeners) is
especially prone to being "simplified" into a broken loop, so say so in the
doc comment. (`animusd/tests/control_mirror_restart.rs`.)
