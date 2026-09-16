# Building the ADR 0018 multi-tablet transaction corpus (PR6) surfaced a real protocol bug and four harness bugs, all in the same short investigation, with a common thread: "the same outcome, computed a second time by a different actor, is not automatically the same value."

**Building the ADR 0018 multi-tablet transaction corpus (PR6) surfaced a
real protocol bug and four harness bugs, all in the same short
investigation, with a common thread: "the same outcome, computed a second
time by a different actor, is not automatically the same value."**
1. **`TxnCommit`'s apply arm treated "already Committed, different
   `commit_ts`" as impossible-by-construction and hard-asserted on it —
   it wasn't impossible.** `txn_commit_at_least`'s own `mint_at_least`
   mints a *fresh* timestamp every call; a still-live coordinator's own
   commit round trip and the recovery resolver's independent post-grace
   push can each legitimately decide "commit" for the same transaction
   with *different* minted values, and `animusd`'s own `CLIENT_TIMEOUT`
   (10s) being longer than `RECOVERY_GRACE` (5s) makes the overlap window
   reachable under nothing more exotic than an ordinary leader election —
   found live, deterministically, on the corpus's first fault-injection
   scenario. Fixed by extending the existing `Committed`-vs-`Aborted`
   duelling-decider no-op to also cover same-outcome-different-ts (first
   log position still wins, unconditionally) — see ADR 0018's PR5
   amendment §1 corrective note and `animus-cp-data/CLAUDE.md`'s
   "In-doubt recovery + decision semantics" entry. **The generalizable
   rule**: when a design lets two independent deciders each reach a
   conclusion (not just "commit vs. abort" but the *exact value* of a
   commit), a hard assert on "impossible for them to disagree" needs a
   stronger argument than "only one entity ever decides" — audit what
   happens when a *second*, equally legitimate decider computes the
   *same* answer through a *different* computation.
2. **A resolve-side helper's OWN caller resolving with a *hardcoded*
   outcome, computed before checking what actually happened, is a torn
   resolve waiting to happen** — my own corpus coordinator's abort path
   proposed an abort, then unconditionally resolved every staged key as
   `Aborted` without re-reading the record's actual decided status first
   (a concurrent recovery commit could have already won). Fixed by
   re-reading before resolving, matching the discipline `ClientCtx::
   cp_txn`/`txn_recover` already follow in production (confirmed by
   auditing every real resolve call site — none of them had this bug;
   only my own test harness did).
3. **A read-resolution helper that only *serves one read correctly* is
   not the same thing as a helper that *durably fixes storage*** —
   `RaftKvNode::resolve_intent_given_status` (and `animusd::ClientCtx::
   cp_get_local_resolving`, which calls it) compute the right answer for
   *this one read* without ever proposing a `TxnResolve`; the physical
   envelope stays an unresolved intent forever unless something else
   (the proactive resolver loop) does the durable rewrite. This is
   documented, accepted production behavior (`TxnTracker::
   unresolved_decided`'s own doc: an anchor stops tracking a transaction
   once *its own* keys resolve, even if a participant's intent on a
   different tablet never gets a proactive fan-out — "still resolved on
   demand the moment any reader hits it" means the *read* is correct,
   not that storage settles) — but a test harness's own "read the final
   state" check that uses a **raw, non-resolving** read (as this corpus's
   `final_state` deliberately does, to keep a meaningful cross-replica
   comparison) will never trigger that on-demand path for a key nobody
   reads again, and will misreport a durably-committed-but-never-resolved
   value as data loss. Fixed with a test-only helper that, unlike the
   production read path, *does* propose an actual `TxnResolve` once a
   foreign intent's status is known. **The general lesson**: when a
   system's "eventual consistency" story rests on "any reader passing by
   will fix it," a test that deliberately never reads the data again
   needs its own explicit "make sure something reads it" step — don't
   assume a converged-or-timeout poll alone reproduces that guarantee if
   the poll's own read path doesn't exercise the same code path a real
   reader would.
4. **A helper that picks "the first replica reporting `is_leader() ==
   true`" must exclude replicas known to be faulted, or it can talk to a
   frozen, isolated node instead of the genuine leader** — a crashed
   replica keeps answering `is_leader() == true` from its last-known,
   pre-crash state forever (it never learns it lost the term; it's
   muted, not shut down). `raftkv_linearizable.rs`'s own `leader_among`
   helper already excludes known-crashed indices for exactly this
   reason; a new harness written independently (this corpus's own
   `leader_of`) didn't replicate it. Fixed more robustly than
   "thread a crashed-set through every call site": pick the reporting
   replica with the **highest `term()`** instead of the first by array
   index — any real election strictly increments the term, so a frozen
   replica's stale term can never out-rank a genuine new leader,
   without needing any external fault-tracking state at all.
5. **A multi-participant intent must carry the ANCHOR's own table name,
   never the participant's own** — `record_table` (stamped into every
   `Envelope::Intent`) exists precisely so a reader hitting a foreign
   intent knows where to route its `TxnStatus` query; passing the
   participant's own table name there instead (an easy copy-paste-shaped
   mistake when the staging loop's own iteration variable is already
   named `table`) means that query always looks for the record in the
   wrong tablet's scope, finds nothing, and the intent never resolves —
   on demand or otherwise. Caught only by tracing one specific stuck
   key's own `IntentInfo` byte-for-byte back to which key's 8-byte token
   the record's own key was actually derived from, since the symptom
   (durability check reports one committed append as lost) looks
   identical to several other, unrelated causes.
**Diagnostic lesson**: every one of these was found by adding a
temporary, narrowly-targeted `eprintln!` at the exact decision point
(which arm of a match fired, what a specific key's own `FastRead`
variant was, what a specific txn_id's tracker state was on each resolver
tick) and re-running the *one* failing scenario in isolation — never by
guessing from the failure message alone. Four of these five bugs
produced the *same* durability-check symptom ("lost acknowledged
append") with completely different root causes; only tracing the actual
runtime state, one hypothesis at a time, distinguished them — a lesson
worth restating from this file's own Hlc/`propose_ordered` entry above,
now at the scale of "chasing a bug through several confounding layers,"
not just one.
