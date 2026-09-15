# Generalizing a type over a state machine: prefer *two plain type params* (`<C, S>`) over *one param with an associated type* (`<SM: Trait<Command=C>>`) — `#[derive]` can't see through associated types.

**Generalizing a type over a state machine: prefer *two plain type params*
(`<C, S>`) over *one param with an associated type* (`<SM: Trait<Command=C>>`) —
`#[derive]` can't see through associated types.** Making `RaftCore` generic over
its command + state machine (ADR 0016 step 2), a one-param `RaftCore<SM>` would
force manual `Clone`/`Debug`/`Serialize` impls on every container holding
`SM::Command` (the derive generates `impl<SM: Clone>`, which does *not* imply
`SM::Command: Clone`). Two plain params (`C = MetaCommand`, `S = Metadata`) let
every derive Just Work, and **defaulted** params keep all existing references
source- and serialization-compatible (the generic is erased in JSON, so the WAL
bytes are unchanged). One residual gotcha: `#[derive(Default)]` still adds a
spurious `C: Default` bound — hand-write `Default` where a field is
`Vec<_>`/`Option<_>` (needs no inner `Default`). And a no-arg constructor like
`RaftCore::new()` needs a type annotation at call sites that don't otherwise pin
the params (bare `let x: RaftCore = …` or `Vec<WalRecord>` on a `decode`).
