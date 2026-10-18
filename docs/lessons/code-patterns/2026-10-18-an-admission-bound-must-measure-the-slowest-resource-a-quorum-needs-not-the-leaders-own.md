# An admission bound must measure the resource a quorum depends on, not only the leader's own

**Where it bit (#1330).** ADR 0080's per-group propose bound read `last_log_index - leader_engine_applied`. In a
regime where the leader applies as fast as it accepts and the followers apply slower, that number stays near zero:
nothing is refused and the followers' unapplied backlog (log retention, restart replay, stale eventual reads) grows
without limit. "Every queue is bounded" held for the leader's queue and silently not for the replicas'.

**Why no local fix exists.** The leader only knows what a follower reports. `match_index` is log *append*; the cost
was in *apply*. If the signal you need is not in the protocol, the fix is a protocol field, hence a cross-node wire
change (ADR 0073 class G: a new frame version selected by a `Gate`), not a clever leader-side proxy.

**The semantics that matter.**
- Bound on the **quorum-th** value, not the minimum: a single slow or dead follower outside the quorum must not
  stall writes, because commit already proceeds without it (`n2_one_slow_blocks` is the negative control).
- Treat *unknown* (never reported, stale, old binary, quiesced) as "as caught up as the leader", never as `0`; a
  `0` default turns every restart, mixed-version roll and quiesced group into a total write refusal.
- Keep the rig honest: the **leader must be fast** or its own backlog hides what the new input does, and the
  negative control (gate closed) must show unbounded growth, or the test proves nothing.

**Mechanism pitfall.** A first draft excluded followers behind the leader's compacted prefix ("InstallSnapshot will
repair them"). In the rig that excluded every lagging follower, because the leader compacts right behind its own
applied index, and the bound never engaged. A debug print of the reports against the result (`own`, reports, `q`)
found it in one run; print the computed input before reasoning about why a bound "does not bite".
