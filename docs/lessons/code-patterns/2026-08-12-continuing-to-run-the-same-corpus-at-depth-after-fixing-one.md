# Continuing to run the same corpus at depth after fixing one corpus-found bug found a second, unrelated one — and then a third layer, once the fix for the second bug was itself checked at depth.

**Continuing to run the same corpus at depth after fixing one corpus-found
bug found a second, unrelated one — and then a third layer, once the
fix for the second bug was itself checked at depth.** Three separate
findings, each only visible once the previous one stopped masking it:
1. **The happy-path commit-report footgun is the mirror image of the
   abort-path one, and re-auditing every "decide, then report" call
   site after finding one instance doesn't catch the sibling** — the
   corpus's own coordinator (`run_txn`) fixed its *abort* path's
   torn-resolve bug (item 2 in the entry above) but its *commit* path
   still reported success straight off `txn_commit_at_least`'s
   `Some(ts)`, the identical "entry applied ≠ my decision won" mistake,
   just on the opposite branch. Found by a *different* corpus scenario
   (`anchor_leader_kill_mid`) than the one that found the abort-path bug
   — the two bugs happened to need different fault shapes to trigger.
   Fixed by always re-reading the record's actual status after every
   decide attempt, commit or abort alike, matching what a full
   re-audit of every *production* decide call site
   (`txn_decide_anchor`, `txn_recover`, and the apply-time `TxnTracker`
   bookkeeping itself) already confirmed they do correctly. **The
   lesson**: fixing one branch of a two-branch bug class doesn't mean
   the other branch got checked — a hypothesis this specific ("does the
   code re-read before reporting, at *every* decide point, not just the
   one the failing test happened to exercise") is worth stating and
   checking explicitly, not inferred from one green test.
2. **A multi-key snapshot-read heuristic needed three redesigns before
   it stopped producing false-positive torn reads, and each of the
   first two replacements introduced a *new*, narrower race that only
   the next depth run exposed** — `animus-test`'s cross-tablet
   transaction corpus's read-only shape: (a) a single future-padded
   `read_at` snapshot ts turned out to be structurally undermined by
   the write-conflict-push mechanism (`RaftKvNode::mint_pushed`) itself
   — a write can be stamped *above* whatever ceiling an **earlier**
   read already pushed that group's clock to, and since `Hlc::mint` is
   monotonic that's a **permanent** floor, so no margin (fixed or
   dynamically sampled from the group's own state) can close it; (b)
   replacing it with "force-resolve once, then read every key
   sequentially" fixed that but introduced a *narrower* race — a slow
   key's own resolve/read can itself take real time, so a transaction
   touching an *earlier*, already-read key can still land before a
   *later* key in the same list is read; (c) making both passes
   **concurrent** (`futures::future::join_all`) narrowed the window to
   one round trip but *still* didn't eliminate it — group-to-group
   ReadIndex latency doesn't start in perfect lockstep even when every
   future is spawned at the same instant. The design that actually
   closed it: read **twice**, concurrently, and only accept the result
   once two consecutive rounds agree byte-for-byte — a positive proof
   of quiescence (nothing was in flight during the whole window),
   rather than a narrower and narrower guess at "surely nothing changed
   this fast." **The generalizable lesson**: when a "make this
   consistent" fix for a distributed read keeps getting narrower races
   rather than zero races, the fixable-margin approach is probably the
   wrong shape entirely — look for a **verifiable stability condition**
   (two independent observations agreeing) instead of a **tighter
   timing bound**, which can always be beaten by one more layer of
   concurrency the previous fix didn't anticipate.
3. **Overwriting another transaction's still-unresolved intent doesn't
   erase it — MVCC keeps the old version — and a later transaction's
   own abort-restore only ever looks *one hop* back, so it can land on
   that stale intent instead of a real committed value, permanently
   hiding it.** Found at seed depth (`ANIMUS_TXN_SEEDS=10`), not in the
   frozen corpus — needs three sequential same-key transactions from one
   client (single-writer-per-key workloads make this the *ordinary*
   case, not a contrived one): the first commits, the second overwrites
   it and is abandoned before deciding, the third stages over the
   second's still-live intent (silently succeeding, pre-fix) and later
   gets decided `Aborted` — its restore's one-hop-back `get_at` finds
   the *second* transaction's intent, not the first's real value, and
   blindly re-merges it at a timestamp *higher* than the first
   transaction's own eventual correct `commit_ts`, so a later correct
   resolve can never win that race via ordinary LWW. Chasing the
   version chain back *multiple* hops on the read side was the obvious
   first fix and the wrong one: an intermediate hop skipped over could
   belong to a transaction that *later commits*, moving the identical
   unrepairable-LWW-loss corruption onto a *different* transaction
   rather than removing it. The fix that actually closes it structurally
   is CockroachDB's writers-push-intents discipline: reject the
   overwrite at **apply time** (a target key already holding a
   *different* transaction's unresolved intent makes the whole stage a
   no-op, whole-or-nothing, exactly like a fence/seal miss), so a key
   can hold at most one live intent at a time and a one-hop-back
   lookback is *always* sound. **This required a second, proposer-side
   fix to be safe at all**: since a stage call returning `Some(ts)` only
   ever meant "the entry applied," a coordinator that didn't check would
   go on to commit a transaction *without one of its own writes ever
   having happened* — worse than the original bug. Every coordinator
   (production `animusd::ClientCtx::txn_prepare_pushing` and the
   corpus's own `stage_anchor_pushing`/`stage_participant_pushing`) now
   verifies each staged key genuinely landed (`txn_verify_staged`, the
   same primitive a recovery push already uses) and retries, bounded,
   before giving up. **The generalizable lesson**: "reject the bad write
   at apply time" and "the proposer must not assume Some(ts) means my
   content is really there" are not two independent hardening options to
   pick between — a system with the second discipline already
   established (task #15's fix, above) needs it applied to *every* new
   apply-time rejection too, or the rejection alone just moves the false
   success from "wrong outcome" to "silently missing write."
