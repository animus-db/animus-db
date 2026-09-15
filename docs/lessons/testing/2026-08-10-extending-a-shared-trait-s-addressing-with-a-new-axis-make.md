# Extending a shared trait's addressing with a new axis: make the primitive methods the ones every implementor must write, and re-derive the old surface as *default* methods over a well-known constant.

**Extending a shared trait's addressing with a new axis: make the primitive
methods the ones every implementor must write, and re-derive the old surface
as *default* methods over a well-known constant.** Adding multiplexed
`(node, stream)` addressing to `Network` (ADR 0026, replacing the
`Coresident` sibling-pool escape hatch's rationale) needed every existing
call site (`env.send(to, payload)` / `env.recv()`, nearly the whole
codebase) to keep compiling and behaving identically. Making `send_stream`/
`recv_stream` the trait's required methods and `send`/`recv` **default**
methods that forward to them with a `PRIMARY_STREAM` constant meant the only
code that had to change was the *three* concrete `Network` implementors
(`SimEnv`, `ProdEnv`, and one test double) — every caller was untouched,
because a default method is in scope exactly like a required one once the
trait is in scope. Grep every `impl <Trait> for` site *before* estimating
blast radius; it is often far smaller than "everywhere the trait's methods
are called." (PR #34.)
