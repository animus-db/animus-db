# To store a generic type behind one registry/handle field, fix the concrete type parameter when the variation isn't needed at the call site — don't reach for a trait object.

**To store a generic type behind one registry/handle field, fix the concrete
type parameter when the variation isn't needed at the call site — don't reach for
a trait object.** Routing a CP-mode table to a hosted `RaftKvNode<E, S>` (ADR 0017
#3a) needed the `animusd` edge state to hold the group handle and call
`put`/`linearizable_get`/`is_leader` on it. `RaftKvNode` is generic over its
engine `S`, so a `Vec<RaftKvNode<ProdEnv, _>>` field would need an
`async_trait` object (the methods are async) — extra machinery for variation that
doesn't exist here: the CP plane is *always* durable, so `S = LsmEngine<ProdEnv>`
is the only instantiation. Fixing it (a `type CpGroup = RaftKvNode<ProdEnv,
LsmEngine<ProdEnv>>` alias — also silences `clippy::type_complexity`) kept the
edge registry a plain `Vec<CpGroup>`, no trait object, no async-trait dep. The
AP data replica *is* type-erased (`Box<dyn Any>`) because its backend genuinely
varies (LSM vs Memory); the CP group's does not.
