# A driver spawned fire-and-forget has no `Result` to propagate a newly-fallible step into — decide the failure mode at the call site, don't assume you can bubble it

**While making `PersistedState::decode` return `Result<_, FormatError>`**
(ADR 0073 Phase 0 workstream B, the shared tagged-envelope convention), the
two production callers — `animus_control::node::drive` and
`animus_cp_data`'s own per-tablet `drive` — both looked like they should
simply propagate the new `Err` upward with a `?`. Neither can: both are
`async fn drive(..) -> ()`, spawned via `EnvExt::spawn_task`, whose bound is
`F: Future<Output = ()>` — there is no `Result` slot to return into. Their
own callers (`RaftNode::start*`/`RaftKvNode::start_hosted*`) are
**synchronous** functions returning `Self`, not `Result<Self, _>`, across
~150 call sites in this workspace; widening that signature to fix one new
fallible decode inside an already-spawned task is a different, much larger
change than the format reset itself.

**What to do instead:** when a change makes some deeply-nested async step
fallible and its immediate caller has no `Result` to return through
(because it was already spawned fire-and-forget), don't reflexively try to
grow a `Result` up the call chain — check whether the call site can
propagate at all before assuming it can, and if it genuinely can't, pick a
failure mode *at that site* and document why:

- If the surrounding crate has an existing "this node/group is being torn
  down, an I/O error from here on is a teardown artifact" latch (this
  codebase's `halted: Arc<AtomicBool>`, already threaded into both of these
  `drive` functions for an unrelated reason), a genuine unrecoverable boot
  failure can reuse it as a **permanent** latch: log loudly at `error`,
  flip it, and return before doing anything that would let the node
  masquerade as freshly-empty (grant a vote, campaign, serve). This keeps
  the process alive and the failure discoverable, without silently treating
  real corruption as "nothing was ever written here."
- Do **not** reach for `panic!` instead, even though `animus-env`'s
  `Spawner` catches spawned-task panics (issue #939): ADR 0073 requires a
  pre-baseline or unknown-version input to be a named, loud `Err`, never a
  panic, and a halted-and-logged group has the same observable effect
  without turning a data-format condition into a crash signature. Both
  `drive` functions use the `halted` latch above.

The one wrong answer is silently discarding the new
`Err` and falling back to the pre-existing infallible behavior (e.g.
`.unwrap_or_default()`) — that reintroduces exactly the "genuine corruption
is indistinguishable from a genuine empty/fresh start" failure mode the
fallible return was added to close. Name the constraint in a comment at the
call site (why there's no `Result` to propagate into, and why the chosen
fallback is safe) so the next person doesn't "fix" it by reaching for `?`
and hitting a type error with no explanation.
