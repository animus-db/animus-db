# A type alias at the concrete instantiation site can keep a "genericize this type" rung from cascading into its every consumer

ADR 0061 rung C3c genericized `animusd`'s `ControlHandle`/
`RemoteControlClient` (`Local(RaftNode<ProdEnv>)` → `Local(RaftNode<E>)`;
`RemoteControlClient`'s one real-I/O method reaching a new `RelayClient`
trait instead of a concrete `relay_request` free function) and moved both
whole into `animus-node`. The natural worry going in: `ClientCtx.control:
ControlHandle` is a field on the crate's ~5,500-line hub struct, so
"genericize `ControlHandle`" sounds like it should force `ClientCtx` itself
to grow `<E, R>` parameters — which would have meant threading them through
every function that takes or returns a `ClientCtx`, the exact C5-shaped
blast radius this rung was explicitly scoped to avoid.

It didn't happen, because `animusd` only ever needs *one* concrete
instantiation of the newly-generic type: `ControlHandle<ProdEnv,
AnimusdRelayClient>`. A crate-local type alias (`pub(crate) type
ControlHandle = animus_node::control_handle::ControlHandle<ProdEnv,
AnimusdRelayClient>;`) binds the generic type back down to a concrete one at
its single point of use, so every existing `ControlHandle::Local(..)`/
`ClientCtx.control: ControlHandle` site keeps compiling completely
unchanged — `ClientCtx` never sees a type parameter, because from its point
of view the alias *is* a concrete type. Only the two real constructor call
sites (`RemoteControlClient::new`/`with_mirror`) needed a one-line update
(pass the new `relay`/`timeout` arguments) since a constructor's arity
genuinely changed; every read-only accessor call site was untouched.

General lesson: when scoping "genericize type `T` and move it to a lower
crate," check whether the *consuming* crate only ever needs one concrete
instantiation of `T` before assuming the generic parameters must propagate
into every struct that holds a `T`. If so, a local type alias at the
instantiation site is the seam that contains the diff to "the type moved
and gained parameters" rather than "half the crate grew two more generic
parameters" — the same shape `animus-node`'s own `ProdEnv`-free boundary
already relies on one level up (this crate depends on `animus-control`
un-genericized from *its* point of view too, via `animus-control`'s own
`E`-generic types resolving concretely wherever `animusd` instantiates
them). This is a smaller instance of the same "ask what narrow thing the
caller actually needs" question ADR 0061's standing C4/C5 guidance already
names for capability traits — here applied to a type's own generic
parameters rather than a trait's method set.
