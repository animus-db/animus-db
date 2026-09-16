# A trait method taking `&mut dyn FnMut(&mut T)` still forces every captured value to be `Clone`, even when the closure only ever runs once (docs/roadmap.md U-07, `GET /admin/backup-store`)

Publishing a background loop's own progress into a shared, admin-readable
slot (`animus_node::backup_janitor`'s new `JanitorProgress`) needed a way
for that loop — generic over a host trait, not `ClientCtx` itself — to
mutate the shared value under a short-held lock without the trait
depending on `async_trait` for a purely synchronous operation. The natural
signature is `fn update_backup_janitor_progress(&self, update: &mut dyn
FnMut(&mut JanitorProgress))`: a trait-object closure parameter avoids
adding a generic type parameter to the trait itself (which would have
forced every caller to spell it out) while still letting a caller mutate
several fields in one lock acquisition.

The gotcha: `dyn FnMut`, unlike a generic `F: FnOnce`, cannot be satisfied
by a closure that *moves* a captured value out of itself — even at a call
site that only ever invokes the closure once. `host.update_backup_janitor_
progress(&mut |p| { p.last_error = tick_error; })` (moving a local
`Option<String>` into the field) is `error[E0507]: cannot move out of
tick_error, a captured variable in an FnMut closure` — `FnMut` closures
must remain callable repeatedly by the trait's own contract, regardless of
how the one real call site happens to use them, so the compiler refuses a
move-out capture unconditionally. The fix is a plain `.clone()` at the
capture site (`p.last_error = tick_error.clone();`) — cheap here (a small
`Option<String>`, called at most a few times per tick), but worth knowing
before reaching for `&mut dyn FnMut` as the "avoid a generic on the trait"
default: if the mutation needs to move an expensive owned value in, either
accept the clone, switch the trait method itself to take the value
directly (`fn set_last_error(&self, err: Option<String>)`) instead of a
closure, or make the trait method generic over `F: FnOnce` (losing
`dyn`-safety, harmless here since nothing calls this trait through a `dyn`
reference) and pass ownership through a `Box<dyn FnOnce>` if a boxed
closure is unavoidable.
