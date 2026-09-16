# Don't copy a sibling corpus's engine-tiering generic ceremony without first checking whether the new node type is generic over the storage engine at all (`animus-control`'s `control_corpus.rs`)

`raftkv_linearizable.rs` (the explicit template `control_corpus.rs` was
told to copy the *architecture*, not the content, of) is generic over both
`E: Env` and `S: StorageEngine` because `animus-cp-data::RaftKvNode<E, S>`
itself is — its `EngineFactory<S>` type alias and the `Group<S>`/`Node<S>`
plumbing exist to let the corpus run the identical scenario set over both
`MemoryEngine` and `LsmEngine<SimEnv>`. A first draft of the new corpus
started copying that same `<S: StorageEngine>` shape onto `Group`/`Node`
before checking whether it was needed — it wasn't:
`animus_control::RaftNode<E>` is generic **only** over `E`; `start<S:
StorageEngine>(..)` is a generic *associated function*, not a type
parameter of `RaftNode` itself, so the engine type is erased the moment a
node is constructed and there is nothing for a second generic parameter to
thread through. Carrying the extra `<S>` ceremony over unused would have
meant a `PhantomData` or a spurious "this corpus supports engine tiers"
claim the harness never actually exercises. General lesson: **before
copying a template corpus's generic shape onto a new one, check whether the
concrete type the new corpus actually drives is generic the same way the
template's was — a sibling module's own genericity is a property of *that*
module's dependency, not a fixed feature of "how a corpus harness in this
repo looks."**
