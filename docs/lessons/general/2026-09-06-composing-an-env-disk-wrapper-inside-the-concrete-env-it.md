# Composing an `Env`/`Disk` wrapper *inside* the concrete env it wraps creates a self-referential `Arc` cycle unless the wrapped-side state is deliberately kept independent (ADR 0069, encryption at rest)

`ProdEnv` is `Clone`-cheap because it is one `Arc<Inner>` handle; every
clone shares the same `Inner`. The natural way to add encryption to its
`Disk` impl looked like: store an `EncryptedDisk<ProdEnv, ProdEnv>` (disk
+ rng both supplied by `ProdEnv` itself) as a field on `Inner`. That is a
genuine reference cycle: `Inner` (behind `Arc`) would own an
`EncryptedDisk` that owns a `ProdEnv` clone, which is itself another
`Arc<Inner>` pointing at the *same* `Inner` — the strong-count never
drops to zero, so the whole node's `Inner` (sockets, peer book, metrics,
everything) leaks for the life of the process, invisible until a test
loop that constructs many short-lived `ProdEnv`s (a temp-dir-per-attempt
retry loop, a corpus, a long test suite) shows unbounded memory growth.

**General form**: before storing "a wrapper over `Self`" as a field on a
type that is itself an `Arc`-backed handle, check whether the wrapper's
generic parameters actually need `Self` — or just a narrower capability
`Self` happens to provide (here: real OS randomness for a per-file salt,
not literally `ProdEnv`'s full `Disk`/`Network`/`Clock` surface). The fix
was a minimal, purpose-scoped type (`DiskSaltRng`, a zero-sized `Rng`
implementor drawing from the identical `OsRng` source `ProdEnv`'s own
`Rng` impl and `PreBindRng` already use) instead of reusing `ProdEnv`
itself — it satisfies the generic bound with no reference back to
`Inner` at all, so no cycle is possible by construction rather than by
discipline. The same shape generalizes: a type wrapping `Self` inside its
own `Arc`-shared state is worth a five-second "does this actually need
the whole handle, or just one capability off it" check before writing the
field.
