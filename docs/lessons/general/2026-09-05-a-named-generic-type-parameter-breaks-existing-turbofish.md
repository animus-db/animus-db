# A named generic type parameter breaks existing turbofish call sites; an `impl Trait` argument doesn't (ADR 0064, S-01 commit 2)

Generalizing `animusd::write_frame`/`read_frame` from a concrete
`&mut TcpStream` to something that also accepts `&mut
animus_env::MaybeTlsStream` looked like the obvious move: add a second
type parameter, `S: AsyncRead + Unpin` (or `AsyncWrite`), alongside the
existing `T: Serialize`/`T: DeserializeOwned`. That compiled the function
itself fine, but broke every one of the ~100 pre-existing
`read_frame::<SomeType>(&mut stream)` call sites across the test suite
with "function takes 2 generic arguments but 1 generic argument was
supplied" — Rust does **not** infer an unspecified *trailing* explicit
type parameter from context, no matter which position it's declared in
relative to the one the caller does specify. `read_frame::<T, S>`
(caller-specified-first) and `read_frame::<S, T>` (caller-specified-last)
both broke every existing `read_frame::<ClientResponse>(..)` turbofish
identically — the *number* of parameters is what matters to arity
checking, not which one the caller happened to name.

The fix: make the stream parameter an anonymous `impl AsyncRead + Unpin`
**argument** instead of a named type parameter at all. An `impl Trait`
argument is generic under the hood but never participates in turbofish, so
`T`'s own explicit-generic slot stays exactly where it always was and
every existing call site kept compiling unchanged. `write_frame`'s `T`
(inferred from the `&T` argument, never turbofished anywhere) didn't
strictly need this, but was changed the same way for consistency and
because the identical reasoning would bite the moment anyone *did* start
turbofishing it.

**General form**: before adding a second type parameter to a function that
already has turbofish call sites, check whether any of those sites specify
fewer type arguments than the function will end up declaring. If so, an
`impl Trait` argument (when the new parameter is only ever used in
argument position, never returned or named in a bound elsewhere) sidesteps
the whole arity-break — it costs nothing at every existing call site and
still gets full monomorphization. Reordering the parameter list does
*not* help; only making it non-turbofishable does.
