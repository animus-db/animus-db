//! ClientCtx's 2PC transaction-coordinator cluster (ADR 0061 rung C5 step 2):
//! stage/prepare/decide/resolve (`txn_prepare*`, `txn_decide_anchor`,
//! `txn_resolve_participant`, `txn_status`, `txn_record_view`,
//! `txn_verify`), orphan/intent recovery (`recovery_resolve`,
//! `txn_recover`) and the top-level 2PC coordinator (`cp_txn`). Moved
//! verbatim out of `lib.rs`'s `impl<E: Env> ClientCtx<E>` blocks -- no
//! logic changes -- `cp_txn`'s narrow retry allowlist moved unmodified.

use std::collections::{BTreeMap, BTreeSet};

use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{ResolveOutcome, StageOutcome, TxnDecisionStatus, TxnId, TxnOutcome};
use animus_dynamo::capacity;
use animus_env::{Env, EnvExt, Metric};
use animus_node::host::RelayClient;
use animus_tablet::{KeyRange, TOKEN_BYTES, TabletId};

use crate::{
    CLIENT_TIMEOUT, ClientCtx, ClientRequest, ClientResponse, CpGroup, CpRoute, PendingKindWrite,
    ReadConsistency, StageConditions, TXN_RESOLVE_ALL_AWAIT_BUDGET,
    TXN_RESOLVE_FENCED_RETRY_ATTEMPTS, TXN_STAGE_PUSH_ATTEMPTS, TXN_STAGE_PUSH_BACKOFF,
    TxnAbortReason, TxnPrecondition, TxnTableWrite, TxnWrite, TxnWriteCondition, decide, dynamo,
    outcome_to_status,
};

impl<E: Env, R: RelayClient> ClientCtx<E, R> {
    /// **The one place a stage actually executes on the leader's own node**
    /// (ADR 0046 U3, `TxnStage` kind-writes stack PR2) — shared by
    /// [`txn_prepare`](Self::txn_prepare)'s own `CpRoute::Local` branch (no
    /// forward needed) and `cp_serve_forwarded`'s `TxnPrepare` arm (a
    /// forwarded hop just landed on the real leader).
    ///
    /// **ADR 0054 step 4a: no evaluation happens here any more.** Every
    /// `pending_kind_writes` entry is turned into a self-contained
    /// [`TxnWrite::pending`] payload — the schema slice this leader's own
    /// `Metadata` read supplies, plus `pk`/`sk`/`op`/`condition` copied
    /// straight through — and appended to `writes` unevaluated. No read, no
    /// `rmw_lock`, no leader-side condition/update evaluation: the
    /// propose→apply staleness window this used to leave open (closed only
    /// by the mandatory own-key `conditions` seatbelt, ADR 0046 Fork C1) no
    /// longer exists to close, because `KvCommand::TxnStage`'s own apply arm
    /// now evaluates every pending write itself, reading the item's current
    /// committed value **in commit order** — the identical win ADR 0054
    /// already gave the ordinary (non-transactional) write path
    /// (`kind_write_item_at_leader`/`KvCommand::KindEval`), extended here to
    /// a transaction's own stage. `stage_marker` is still built here (a pure
    /// function of `pk`/`sk`, never state that could go stale) via
    /// [`dynamo::item_stage_marker_change_log`].
    #[allow(clippy::too_many_arguments)] // mirrors ClientRequest::TxnPrepare's own field count
    pub(crate) async fn txn_stage_local(
        &self,
        leader: &CpGroup<E>,
        table: &str,
        anchor: Option<(TxnId, Vec<u8>, String)>,
        mut writes: Vec<TxnWrite>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        participant_spans: Vec<(String, KeyRange)>,
        pending_kind_writes: Vec<PendingKindWrite>,
    ) -> Result<(TxnId, Vec<u8>, String, HlcTimestamp, StageOutcome), TxnAbortReason> {
        decide::frozen_refusal(leader.is_frozen()).map_err(TxnAbortReason::Other)?;
        if !pending_kind_writes.is_empty() {
            let meta = self.effective_metadata();
            let schema = dynamo::write_schema_for(&meta, table);
            for p in pending_kind_writes {
                let key = dynamo::item_key(&p.pk, p.sk.as_ref());
                // ADR 0065 §2/§3/§6: check each participant write BEFORE
                // ever staging, at **2x** the ordinary pre-charge — a
                // transactional write costs double, `ConsumedCapacity::
                // scaled(2.0)`'s existing factor. A refusal aborts this
                // whole stage attempt with `TxnAbortReason::Throttled`,
                // which `run_transact`'s own cancellation-reason mapping
                // (site 3, `dynamo.rs`) turns into a `ThrottlingError`
                // `CancellationReasons` entry at this action's own index.
                if let Some(write_limit) = self.throttle_defaults.write_units()
                    && let Some(tablet) =
                        crate::topology::tablet_for_key(meta.tablets_for_table(table), &key)
                {
                    let share =
                        write_limit as f64 / meta.tablets_for_table(table).count().max(1) as f64;
                    let cost = 2.0
                        * match &p.op {
                            crate::KindWriteOp::Put(item) => {
                                capacity::write_units(capacity::item_size(item))
                            }
                            crate::KindWriteOp::Delete | crate::KindWriteOp::Update { .. } => 1.0,
                        };
                    if !self
                        .throttle
                        .check_write(tablet, share, cost, self.env.now())
                    {
                        if let Some(data) = self.data.as_ref() {
                            data.raftkv_metrics.incr(Metric::ThrottledWrites);
                        }
                        return Err(TxnAbortReason::Throttled {
                            table: table.to_owned(),
                            key,
                        });
                    }
                }
                let stage_marker = dynamo::item_stage_marker_change_log(&p.pk, p.sk.as_ref());
                let op = dynamo::kind_write_op_to_eval_op(p.op);
                writes.push(TxnWrite::pending_eval(
                    key,
                    Some(stage_marker),
                    crate::PendingTxnWrite {
                        schema: schema.clone(),
                        pk: p.pk,
                        sk: p.sk,
                        op,
                        condition: p.condition,
                        ttl_expired: false,
                    },
                ));
            }
        }
        match anchor {
            None => {
                let (txn_id, record_key, outcome) = leader
                    .txn_stage(table, writes, participant_spans, conditions)
                    .await
                    .ok_or_else(|| {
                        TxnAbortReason::Other(
                            "CP group leader moved during anchor stage; retry".into(),
                        )
                    })?;
                let ts = txn_id.ts;
                Ok((txn_id, record_key, table.to_owned(), ts, outcome))
            }
            Some((txn_id, record_key, record_table)) => {
                let (ts, outcome) = leader
                    .txn_stage_participant(
                        txn_id.clone(),
                        record_key.clone(),
                        record_table.clone(),
                        writes,
                        conditions,
                    )
                    .await
                    .ok_or_else(|| {
                        TxnAbortReason::Other(
                            "CP group leader moved during participant stage; retry".into(),
                        )
                    })?;
                Ok((txn_id, record_key, record_table, ts, outcome))
            }
        }
    }

    /// **Stage** `writes` on `table`'s tablet leader — the anchor
    /// (`anchor: None`, mints a fresh `TxnId`/record key) or a participant
    /// (`anchor: Some((txn_id, record_key, record_table))`, referencing an
    /// already-known anchor record). Routes exactly like every other CP op
    /// (serve locally, or forward one hop via [`ClientRequest::TxnPrepare`]).
    /// Returns `(txn_id, record_key, record_table, stage_ts, outcome)` — for
    /// the anchor case `stage_ts == txn_id.ts` by construction
    /// (`RaftKvNode::txn_stage_anchor` mints the record's own
    /// commit-attempt timestamp as its stage ts). `Err` here means the
    /// stage entry never even *applied* (not leader, or it timed out) —
    /// `outcome` is what the caller checks to learn whether the entry that
    /// did apply actually staged (see [`ClientResponse::TxnPrepared`]'s
    /// doc). `conditions` is ADR 0018 §2's apply-time write-key conditions
    /// amendment (own-key byte-level OCC — empty for a plain transaction).
    ///
    /// **`participant_spans`** (ADR 0018 §2/PR5, task #18 fix): every
    /// *other* participant's `(table, span)` pairs, meaningful only for the
    /// anchor case (`anchor: None`) — merged into the freshly-created
    /// record's `intent_spans` alongside the anchor's own writes, so
    /// in-doubt recovery's `all_staged` check (`ClientCtx::txn_recover`)
    /// can actually verify every participant, not just the anchor. Ignored
    /// for a participant's own stage (`anchor: Some(..)`), which never
    /// creates a record to populate.
    pub(crate) async fn txn_prepare(
        &self,
        table: &str,
        anchor: Option<(TxnId, Vec<u8>, String)>,
        writes: Vec<TxnWrite>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        participant_spans: Vec<(String, KeyRange)>,
        pending_kind_writes: Vec<PendingKindWrite>,
    ) -> Result<(TxnId, Vec<u8>, String, HlcTimestamp, StageOutcome), TxnAbortReason> {
        let Some(first) = writes.first().map(|w| w.key.clone()).or_else(|| {
            pending_kind_writes
                .first()
                .map(|p| dynamo::item_key(&p.pk, p.sk.as_ref()))
        }) else {
            return Err(TxnAbortReason::Other(
                "txn prepare: writes must be non-empty".into(),
            ));
        };
        match self.cp_route(table, &first).await {
            CpRoute::Local(leader) => {
                self.txn_stage_local(
                    &leader,
                    table,
                    anchor,
                    writes,
                    conditions,
                    participant_spans,
                    pending_kind_writes,
                )
                .await
            }
            CpRoute::Forward(addr, hinted) => {
                let request = ClientRequest::TxnPrepare {
                    table: table.to_owned(),
                    anchor,
                    writes,
                    conditions,
                    participant_spans,
                    pending_kind_writes,
                };
                match self.cp_forward(table, &first, addr, hinted, request).await {
                    ClientResponse::TxnPrepared {
                        txn_id,
                        record_key,
                        record_table,
                        ts,
                        outcome,
                    } => Ok((txn_id, record_key, record_table, ts, outcome)),
                    // ADR 0018's 2026-08-24 `CancellationReasons` amendment
                    // (issue #374 C2b): recover the typed reason a remote
                    // `txn_stage_local` minted, `TxnAbortReason::encode`d
                    // into this same `ClientResponse::Error` string —
                    // `decode` degrades a peer's plain (pre-amendment, or
                    // genuinely unmarked) error to `Other` automatically.
                    ClientResponse::Error(e) => Err(TxnAbortReason::decode(&e)),
                    other => Err(TxnAbortReason::Other(format!(
                        "unexpected reply to forwarded TxnPrepare: {other:?}"
                    ))),
                }
            }
            // `"; retry"`-suffixed (the house retryability convention,
            // `Self::read_should_retry`/`TxnAbortReason::is_ambiguous`): no
            // leader being reachable RIGHT NOW is a transient routing/
            // election-window fact, not evidence the transaction did or
            // will not commit — the same reasoning `cp_read`'s own
            // `CpRoute::None` arm documents, applied here so a
            // `ClientRequestToken`'s idempotency record is never recorded
            // `CANCELLED` off this alone (ADR 0018's issue #298 "deep shape
            // A" amendment).
            CpRoute::None => Err(TxnAbortReason::Other(
                "no CP group leader reachable for txn prepare; retry".into(),
            )),
        }
    }

    /// [`txn_prepare`](Self::txn_prepare), verified: a stage attempt
    /// returning `Ok(..)` only means its *entry applied* — since ADR 0018
    /// §2/PR6 (task #16), it can still have no-op'd internally if any
    /// target key already held another transaction's unresolved intent
    /// (the apply-time writer-push-intents guard `KvCommand::TxnStage`'s
    /// doc describes, closing the chained-stale-intent durability hole a
    /// corpus depth run found). Without checking the returned
    /// `StageOutcome`, a blocked stage would look identical to a genuine
    /// one at the propose layer, and the transaction would go on to commit
    /// **without that key's write ever having happened** — a new, worse
    /// atomicity violation than the one this whole fix exists to close.
    ///
    /// **Since the ADR 0018 §2 apply-time write-key conditions amendment**:
    /// branches directly on `txn_prepare`'s own returned `StageOutcome`
    /// instead of a separate post-hoc `ClientCtx::txn_verify` round trip
    /// (the apply path already knows definitively whether — and why — this
    /// exact stage no-op'd, so a second read to re-derive the same fact was
    /// redundant once the apply arm started reporting it). `Staged` returns
    /// success; `IntentBlocked` first tries [`push_resolution_if_decided`]
    /// (ADR 0018 §2 issue #298 residual fix — see that method's own doc)
    /// and then retries the whole stage after a short backoff — bounded
    /// (`TXN_STAGE_PUSH_ATTEMPTS`), mirroring the bounded retry a *read*
    /// already does against a foreign pending intent, giving a genuinely
    /// still-live blocking transaction room to clear on its own otherwise
    /// (its own coordinator finishing, or `txn_resolver_loop`'s passive
    /// per-second sweep pushing it once past `RECOVERY_GRACE`);
    /// `ConditionFailed`/`Fenced` are both **final** — retrying an
    /// identical stage changes nothing, so these return a client-facing
    /// error immediately, never looping.
    ///
    /// **Issue #412: a stage attempt that never even applied also gets this
    /// same bounded retry, when its own failure is retryable-shaped.**
    /// `txn_prepare` can fail outright — a leader-moved race in
    /// `dynamo::eval_kind_txn_write`'s leader-side old-image read
    /// (`txn_stage_local`'s stage-time kind-write evaluation), or the
    /// identical race in `txn_stage`/`txn_stage_participant` returning
    /// `None` (the propose itself never got accepted) — carrying the house
    /// `"; retry"` shape (`dynamo::leader_read_failure`'s doc). Pre-fix,
    /// that `Err` escaped this loop immediately via `?`, on the very first
    /// attempt, and surfaced through `cp_txn`/`dynamo::run_transact` as a
    /// terminal `TransactionCanceledException` for a condition the very
    /// next attempt would routinely clear — the same class of bug
    /// `dynamo::kind_write_item_at_leader`'s ordinary (non-transactional)
    /// twin never had, since `ClientCtx::cp_kind_write_item`'s own retry
    /// loop already re-resolves routing on this exact shape. Retrying here
    /// is safe: a `txn_prepare` that failed this way never reached its own
    /// propose (the read/evaluate happens strictly before
    /// `leader.txn_stage`/`txn_stage_participant`), so nothing was proposed
    /// to double up on, and re-invoking `txn_prepare` re-resolves
    /// `cp_route` fresh — the identical "safe to retry, re-route every
    /// attempt" discipline the ordinary write path already has. Only a
    /// retryable-shaped `Other` is caught here — `ConditionFailed`/
    /// `TransactionConflict` and a non-retryable `Other` (no CP group
    /// leader reachable, a malformed request) still propagate immediately,
    /// unchanged.
    ///
    /// **`push_resolution_if_decided`** (ADR 0018 §2 issue #298 residual
    /// fix, confirmed live under the un-pinned `SplitMode::InPlace` proof
    /// soak): the write-side sibling of the foreign-intent READ path's
    /// `confirm_or_push`/`resolve_intent_given_status`. A blocker found at
    /// stage time may already be DECIDED — most commonly this exact
    /// coordinator's own immediately-prior attempt (a fresh `TxnId` retry,
    /// `TxnAbortReason::is_safe_to_retry_fresh`, racing its own
    /// already-aborted first try's still-live intent, because that
    /// attempt's own `resolve_all` resolve landed as a silent no-op:
    /// `KvCommand::TxnResolve`'s `fence` check can reject a resolve whose
    /// routing went stale between `cp_route` and the entry's actual apply —
    /// e.g. the target tablet split in between — and unlike `TxnStage`,
    /// `TxnResolve` has no outcome channel to report that, so the resolve's
    /// own proposer sees `Some(ts)` "success" regardless; the resolve
    /// outcome-channel gap itself is named as a separate, deferred issue in
    /// `docs/adr/0018-cross-tablet-transactions.md`'s matching amendment).
    /// Querying the blocker's own decision and, if it already decided,
    /// actively pushing its resolution with FRESH routing — this call's own
    /// fresh `cp_route` (inside `txn_resolve_participant`) is what correctly
    /// reaches whatever tablet the key belongs to NOW, sidestepping the
    /// stale-fence race the ORIGINAL resolve hit — converges well inside
    /// [`txn_prepare_pushing`](Self::txn_prepare_pushing)'s own short retry
    /// budget instead of exhausting it into a spurious `TransactionConflict`.
    /// A still-`Pending`/unconfirmable blocker is left alone (never pushed
    /// via `txn_recover` — that risks aborting a genuinely live coordinator
    /// before `RECOVERY_GRACE`); the caller's own existing backoff-and-retry
    /// is exactly the right, unchanged behavior for GENUINE still-in-flight
    /// cross-transaction contention. Regression:
    /// `issue_298_conflict_tests::a_fresh_stage_pushes_a_decided_blockers_
    /// resolution_instead_of_conflicting`.
    pub(crate) async fn push_resolution_if_decided(
        &self,
        table: &str,
        blocked_key: &[u8],
        blocker: TxnId,
        blocker_record_table: String,
        blocker_record_key: Vec<u8>,
        attempt: u32,
    ) {
        let decided_outcome = match self
            .txn_status(&blocker_record_table, &blocker_record_key)
            .await
        {
            Ok(TxnDecisionStatus::Committed { commit_ts }) => {
                Some(TxnOutcome::Committed { commit_ts })
            }
            Ok(TxnDecisionStatus::Aborted) => Some(TxnOutcome::Aborted),
            Ok(TxnDecisionStatus::Pending) | Err(_) => None,
        };
        if let Some(outcome) = decided_outcome {
            tracing::debug!(
                table,
                ?blocked_key,
                blocking_txn = ?blocker,
                ?outcome,
                attempt,
                "txn prepare: blocker already decided — pushing its resolution before \
                 retrying the stage"
            );
            self.txn_resolve_participant_retrying(
                table,
                blocker,
                blocker_record_key,
                vec![blocked_key.to_vec()],
                outcome,
            )
            .await;
        }
    }

    pub(crate) async fn txn_prepare_pushing(
        &self,
        table: &str,
        anchor: Option<(TxnId, Vec<u8>, String)>,
        writes: Vec<TxnWrite>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        participant_spans: Vec<(String, KeyRange)>,
        pending_kind_writes: Vec<PendingKindWrite>,
    ) -> Result<(TxnId, Vec<u8>, String, HlcTimestamp), TxnAbortReason> {
        // ADR 0018's 2026-08-24 `CancellationReasons` amendment (issue #374
        // C2b): the last-seen `IntentBlocked` key, so exhausting every retry
        // attempt can still name the specific key that never cleared —
        // `TransactionConflict`, never `ConditionFailed` (a lost race, not a
        // permanent condition failure).
        let mut last_blocked: Option<Vec<u8>> = None;
        // Issue #412: the last-seen retryable-shaped `Other` message from a
        // stage attempt that never even applied, so exhausting every retry
        // attempt can still report what kept recurring instead of the
        // generic "did not converge" text.
        let mut last_retryable: Option<String> = None;
        for attempt in 0..TXN_STAGE_PUSH_ATTEMPTS {
            let (txn_id, record_key, record_table, ts, outcome) = match self
                .txn_prepare(
                    table,
                    anchor.clone(),
                    writes.clone(),
                    conditions.clone(),
                    participant_spans.clone(),
                    pending_kind_writes.clone(),
                )
                .await
            {
                Ok(v) => v,
                Err(TxnAbortReason::Other(msg)) if msg.ends_with("; retry") => {
                    tracing::debug!(
                        table,
                        attempt,
                        %msg,
                        "txn prepare: stage attempt itself failed with a retryable routing/\
                         leadership race; retrying"
                    );
                    last_retryable = Some(msg);
                    if attempt + 1 < TXN_STAGE_PUSH_ATTEMPTS {
                        self.env.sleep(TXN_STAGE_PUSH_BACKOFF).await;
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            match outcome {
                StageOutcome::Staged => return Ok((txn_id, record_key, record_table, ts)),
                StageOutcome::IntentBlocked {
                    key,
                    txn_id: blocker,
                    record_key: blocker_record_key,
                    record_table: blocker_record_table,
                } => {
                    tracing::debug!(
                        table,
                        ?key,
                        blocking_txn = ?blocker,
                        attempt,
                        "txn prepare: stage blocked by another transaction's unresolved intent; \
                         retrying"
                    );
                    self.push_resolution_if_decided(
                        table,
                        &key,
                        blocker,
                        blocker_record_table,
                        blocker_record_key,
                        attempt,
                    )
                    .await;
                    last_blocked = Some(key);
                }
                StageOutcome::ConditionFailed { key } => {
                    return Err(TxnAbortReason::ConditionFailed {
                        table: table.to_owned(),
                        key,
                    });
                }
                // ADR 0054 step 4a: a pending write's own apply-time
                // evaluation was rejected — a validation-shaped failure
                // (`condition.evaluate`/`apply_update` returning `Err`,
                // never a false condition, which is `ConditionFailed`
                // above). Final, like `ConditionFailed` — never retried —
                // and, for the identical reason that site gives (no old
                // image in hand, and this path must not add a read just to
                // populate one), `key`/`code` are folded into the message
                // rather than carried as a typed `TxnAbortReason` field;
                // `dynamo.rs::run_transact`'s own `TxnAbortReason::Other(_)
                // => None` arm already falls back to the aggregate-only
                // `TransactionCanceledException` shape for this, matching
                // the pre-4a fidelity a leader-side `ConditionError`/
                // `UpdateError` already had (it too collapsed to `Other`
                // via `WireError`, never a typed per-action reason).
                StageOutcome::Rejected { key, code, message } => {
                    return Err(TxnAbortReason::Other(format!(
                        "txn prepare: stage on table `{table}` rejected pending write {key:?} \
                         at apply ({code}): {message}"
                    )));
                }
                StageOutcome::Fenced => {
                    return Err(TxnAbortReason::Other(format!(
                        "txn prepare: stage on table `{table}` was rejected (a stale route, an \
                         already-sealed/out-of-fence range, or a concurrent in-doubt-recovery \
                         decision); retry"
                    )));
                }
            }
            if attempt + 1 < TXN_STAGE_PUSH_ATTEMPTS {
                self.env.sleep(TXN_STAGE_PUSH_BACKOFF).await;
            }
        }
        match (last_blocked, last_retryable) {
            (Some(key), _) => Err(TxnAbortReason::TransactionConflict {
                table: table.to_owned(),
                key,
            }),
            // Every attempt failed at `txn_prepare` itself with a
            // retryable-shaped `Other` (issue #412) and none ever reached
            // `IntentBlocked` — report the last such failure **verbatim**
            // rather than wrapping it in exhaustion prose.
            //
            // **The wrap used to silently reclassify an already-safe
            // message** (issue #298 residual, confirmed live 2026-08-27):
            // every `msg` reaching this arm already carries the house `";
            // retry"` suffix (it is only ever caught by the loop above's own
            // `msg.ends_with("; retry")` guard) — for a message shaped like
            // `TxnAbortReason::is_safe_to_retry_fresh`'s own
            // `"txn prepare: leader-side evaluation failed:"` allowlist
            // entry, `msg` was *already* both `is_ambiguous()` **and**
            // `is_safe_to_retry_fresh()` before reaching this arm. A prior
            // version of this arm nested `msg` inside a new sentence
            // (`"txn prepare: stage on table \`{table}\` did not converge
            // after N attempts (last transient failure: {msg})"`), which
            // broke classification **twice over**: the nesting parenthesis
            // moved `"; retry"` before the closing `")"` (failing
            // `is_ambiguous`'s suffix check outright), and even once that
            // was patched to re-append `"; retry"` at the very end, the new
            // sentence's own leading text meant the whole message no longer
            // **starts with** `"txn prepare: leader-side evaluation
            // failed:"` (failing `is_safe_to_retry_fresh`'s prefix check
            // specifically) — silently downgrading a message that was
            // PROVABLY safe to retry with a fresh `TxnId` (this stage never
            // even reached its own propose) into one `run_transact` could
            // only leave stuck `PENDING` for the full ADR 0051 TTL window
            // (600s) — far past any real client's retry budget (this soak's
            // own `RETRYABLE_BLIP_DEADLINE`, 90s, included). Passing `msg`
            // through unchanged means this arm can never accidentally
            // downgrade a classification the underlying reason already
            // earned — exhausting `TXN_STAGE_PUSH_ATTEMPTS` on an identical
            // recurring reason doesn't change how safe that reason is to
            // retry, so there is nothing to re-derive here. See
            // `docs/engineering-lessons.md`'s matching issue #298 entry for
            // the full incident (a live capture under the un-pinned
            // `SplitMode::InPlace` soak) and the general "a wrapping
            // `format!` around a classified message is itself a
            // classification bug" lesson.
            (None, Some(msg)) => Err(TxnAbortReason::Other(msg)),
            // Every `TXN_STAGE_PUSH_ATTEMPTS` attempt returning `Ok` with an
            // outcome other than `Staged`/`IntentBlocked`/`ConditionFailed`/
            // `Rejected`/`Fenced` is unreachable (`StageOutcome` is
            // exhaustively matched above) — kept as a typed fallback rather
            // than an
            // `unreachable!()` so a future `StageOutcome` variant fails soft
            // here instead of panicking a live node.
            (None, None) => Err(TxnAbortReason::Other(format!(
                "txn prepare: stage on table `{table}` did not converge after \
                 {TXN_STAGE_PUSH_ATTEMPTS} attempts"
            ))),
        }
    }

    /// **Commit or abort** `txn_id`'s record at `record_key` on `table`'s
    /// (the anchor's own) tablet leader — the wire-routed counterpart of
    /// [`RaftKvNode::txn_commit_at_least`] (`commit: true`, floored at
    /// `min_commit_ts`) / [`RaftKvNode::txn_abort`] (`commit: false`).
    ///
    /// **Deliberately resolves nothing** (ADR 0018 §2/PR5 — a change from
    /// the PR4 shape, which bundled the anchor's own keys' resolve into
    /// this call): resolving every participant, the anchor's own keys
    /// included, is now the caller's uniform job (`cp_txn`'s `resolve_all`),
    /// so a record's `intent_spans` — and hence what a recovery pusher
    /// verifies/resolves — never has to special-case "the anchor's keys are
    /// resolved differently from everyone else's."
    ///
    /// **Returns the record's ACTUAL, applied decision** (ADR 0018 §2/PR5
    /// decision-semantics amendment), never just "the ts my own proposal
    /// landed at": recovery makes duelling deciders legal, so this
    /// `commit`/`abort` proposal can lose to a concurrent recovery decision
    /// on the very same record (the anchor's own Raft log position is the
    /// sole arbiter — see `apply_and_compact`'s `TxnCommit`/`TxnAbort`
    /// arms). The caller MUST act on the returned outcome, never assume the
    /// decision it asked for is the one that happened.
    ///
    /// `orphan_created_ts: Some(created_ts)` (ADR 0018 §2/PR5's
    /// orphan-record fix) overrides `commit`/`min_commit_ts` entirely: this
    /// call is a recovery pusher that found **no record at all** for
    /// `txn_id` (see [`txn_recover`](Self::txn_recover)'s doc) and must
    /// synthesize an `Aborted` tombstone (`RaftKvNode::txn_abort_orphan`)
    /// rather than proposing against a record that doesn't exist.
    pub(crate) async fn txn_decide_anchor(
        &self,
        table: &str,
        txn_id: TxnId,
        record_key: Vec<u8>,
        commit: bool,
        min_commit_ts: HlcTimestamp,
        orphan_created_ts: Option<HlcTimestamp>,
    ) -> Result<TxnOutcome, String> {
        match self.cp_route(table, &record_key).await {
            CpRoute::Local(leader) => {
                decide::frozen_refusal(leader.is_frozen())?;
                if let Some(created_ts) = orphan_created_ts {
                    leader
                        .txn_abort_orphan(txn_id.clone(), record_key.clone(), created_ts)
                        .await
                        .ok_or("CP group leader moved during orphan abort; retry")?;
                } else if commit {
                    leader
                        .txn_commit_at_least(txn_id.clone(), record_key.clone(), min_commit_ts)
                        .await
                        .ok_or("CP group leader moved during anchor commit; retry")?;
                } else {
                    leader
                        .txn_abort(txn_id.clone(), record_key.clone())
                        .await
                        .ok_or("CP group leader moved during anchor abort; retry")?;
                }
                match leader.txn_status_local(&record_key).await {
                    Some(TxnDecisionStatus::Committed { commit_ts }) => {
                        Ok(TxnOutcome::Committed { commit_ts })
                    }
                    Some(TxnDecisionStatus::Aborted) => Ok(TxnOutcome::Aborted),
                    Some(TxnDecisionStatus::Pending) => Err(
                        "txn decide: record still Pending immediately after its own decide \
                         applied — protocol bug"
                            .into(),
                    ),
                    None => Err("CP group leader moved after decide; retry".into()),
                }
            }
            CpRoute::Forward(addr, hinted) => {
                let request = ClientRequest::TxnDecide {
                    table: table.to_owned(),
                    txn_id,
                    record_key: record_key.clone(),
                    commit,
                    min_commit_ts,
                    orphan_created_ts,
                };
                match self
                    .cp_forward(table, &record_key, addr, hinted, request)
                    .await
                {
                    ClientResponse::TxnDecided { outcome } => Ok(outcome),
                    ClientResponse::Error(e) => Err(e),
                    other => Err(format!(
                        "unexpected reply to forwarded TxnDecide: {other:?}"
                    )),
                }
            }
            CpRoute::None => Err("no CP group leader reachable for txn decide".into()),
        }
    }

    /// Retry [`txn_decide_anchor`](Self::txn_decide_anchor) — with the SAME
    /// `txn_id`, never a fresh one — while its own attempt fails outright
    /// (a `"; retry"`-suffixed error, never even reaching a decision),
    /// bounded by [`CLIENT_TIMEOUT`]. Mirrors [`cp_kind_write_item`](Self::cp_kind_write_item)'s
    /// issue #288 freeze-refusal retry shape: `txn_decide_anchor`'s own
    /// `cp_route` call already re-resolves routing fresh every attempt
    /// (essential — after a cutover the record routes to a child tablet,
    /// not the frozen parent), so this wrapper only needs to supply the
    /// backoff-and-loop.
    ///
    /// **Load-bearing, not a style choice** (ADR 0018 §2 issue #298
    /// residual fix): [`cp_txn`](Self::cp_txn) only ever calls this from a
    /// point where it already knows whether every participant staged. When
    /// they all did (the ordinary commit path — the abort paths only ever
    /// reach here because *some* participant did NOT stage), a decide-step
    /// failure that is safe to retry with a FRESH `TxnId` per
    /// `TxnAbortReason::is_safe_to_retry_fresh`'s own allowlist (most
    /// commonly `FROZEN_REFUSAL`, shared byte-for-byte between a stage-time
    /// and a decide-time freeze) is, in fact, NOT safe here: every
    /// participant's intent is already durably staged, so
    /// `ClientCtx::txn_recover`'s own independent `all_staged`-driven
    /// decision can legitimately COMMIT this exact (original) `txn_id` at
    /// any moment — a fresh retry racing that is precisely the
    /// double-materialize hazard this whole amendment exists to close
    /// (confirmed live: `multi_split_soak_streamed_gsi_table_under_mixed_
    /// load`'s own `delivered=146/144` signature, both `x…a`/`x…b` keys of
    /// one transactional pair delivered twice — once via this exact race).
    /// Retrying the SAME decision instead is always safe: a repeat
    /// `TxnCommit`/`TxnAbort` propose for an already-decided record is a
    /// logged no-op (`animus-cp-data`'s own first-applied-wins doctrine),
    /// and it never abandons already-staged work the way minting a fresh
    /// `TxnId` would. See `docs/adr/0018-cross-tablet-transactions.md`'s
    /// matching amendment for the full incident.
    async fn txn_decide_anchor_retrying(
        &self,
        table: &str,
        txn_id: TxnId,
        record_key: Vec<u8>,
        commit: bool,
        min_commit_ts: HlcTimestamp,
    ) -> Result<TxnOutcome, String> {
        let deadline = self.env.now().saturating_add(CLIENT_TIMEOUT);
        loop {
            match self
                .txn_decide_anchor(
                    table,
                    txn_id.clone(),
                    record_key.clone(),
                    commit,
                    min_commit_ts,
                    None,
                )
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(e) if decide::read_should_retry(&e) && self.env.now() < deadline => {
                    tracing::debug!(
                        table,
                        ?txn_id,
                        commit,
                        %e,
                        "txn decide: anchor decide attempt itself failed with a retryable \
                         routing/leadership/freeze race; retrying the SAME decision (never a \
                         fresh TxnId — every participant may already be staged)"
                    );
                    self.env.sleep(TXN_STAGE_PUSH_BACKOFF).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// **Resolve** `keys` on `table`'s tablet leader per the already-decided
    /// `outcome` — the wire-routed counterpart of [`RaftKvNode::txn_resolve`],
    /// used for every participant (the anchor's own keys included, via the
    /// same routing as any other CP op) once the coordinator has a final
    /// decision. Routed by `keys[0]`, never `record_key` (see
    /// [`ClientRequest::TxnResolve`]'s doc).
    ///
    /// **A single, one-shot attempt at whatever `cp_route` resolves right
    /// now** (ADR 0018 §2 write-loss amendment §3/§6) — `Ok(ResolveOutcome::
    /// Fenced)` means this attempt's routing was stale (a concurrent split
    /// moved the target key's range between `cp_route` and the entry's
    /// actual apply) and the resolve did **not** take effect; the caller
    /// must re-route (a fresh `cp_route` call) and retry, never treat this
    /// as done. [`txn_resolve_participant_retrying`](Self::
    /// txn_resolve_participant_retrying) is the bounded-retry wrapper every
    /// production caller should use instead of calling this directly.
    pub(crate) async fn txn_resolve_participant(
        &self,
        table: &str,
        txn_id: TxnId,
        record_key: Vec<u8>,
        keys: Vec<Vec<u8>>,
        outcome: TxnOutcome,
    ) -> Result<ResolveOutcome, String> {
        let Some(first) = keys.first().cloned() else {
            return Ok(ResolveOutcome::Resolved); // nothing to resolve
        };
        match self.cp_route(table, &first).await {
            CpRoute::Local(leader) => {
                // ADR 0050 rung 5 (fork F7): a resolve landing on a frozen
                // parent is refused retryably — post-cutover the identical
                // resolve re-routes to the child, which holds the copied
                // intent + record and materializes at its own position.
                decide::frozen_refusal(leader.is_frozen())?;
                match leader.txn_resolve(txn_id, record_key, keys, outcome).await {
                    Some((_, outcome)) => Ok(outcome),
                    None => Err("CP group leader moved during resolve; retry".into()),
                }
            }
            CpRoute::Forward(addr, hinted) => {
                let request = ClientRequest::TxnResolve {
                    table: table.to_owned(),
                    txn_id,
                    record_key,
                    keys,
                    outcome,
                };
                match self.cp_forward(table, &first, addr, hinted, request).await {
                    ClientResponse::TxnResolved { outcome } => Ok(outcome),
                    ClientResponse::Error(e) => Err(e),
                    other => Err(format!(
                        "unexpected reply to forwarded TxnResolve: {other:?}"
                    )),
                }
            }
            // Nowhere to route this to right now — an ordinary retryable
            // gap, not a confirmed resolve; never silently treated as
            // `Resolved` (this used to be `Ok(())`, indistinguishable from
            // success — the exact ambiguity this amendment closes).
            CpRoute::None => Err("no CP group leader reachable for resolve; retry".into()),
        }
    }

    /// **Bounded-retry wrapper** around
    /// [`txn_resolve_participant`](Self::txn_resolve_participant) — the
    /// actual fix for the ADR 0018 §2 write-loss amendment's §4 "acked
    /// write lost, no error anywhere" residual: a `Fenced` outcome means a
    /// concurrent split moved the target key's range out from under the
    /// PREVIOUS attempt's routing decision, so this loops re-resolving
    /// `cp_route` **fresh** each attempt (via a brand-new
    /// `txn_resolve_participant` call, never reusing a stale route) instead
    /// of ever swallowing the no-op as done. A transient routing/leadership
    /// `Err` gets the identical bounded retry — the two failure shapes are
    /// both "try again with fresh metadata," just for different reasons.
    /// Best-effort and fire-and-forget, like every existing caller of the
    /// one-shot primitive: a resolve that still hasn't landed once this
    /// exhausts its attempts is left for `txn_resolver_loop`'s next tick or
    /// a reader's own on-demand foreign-intent push — this wrapper only
    /// narrows the window, it is not the sole safety net.
    pub(crate) async fn txn_resolve_participant_retrying(
        &self,
        table: &str,
        txn_id: TxnId,
        record_key: Vec<u8>,
        keys: Vec<Vec<u8>>,
        outcome: TxnOutcome,
    ) {
        for attempt in 0..TXN_RESOLVE_FENCED_RETRY_ATTEMPTS {
            let result = self
                .txn_resolve_participant(
                    table,
                    txn_id.clone(),
                    record_key.clone(),
                    keys.clone(),
                    outcome.clone(),
                )
                .await;
            match result {
                Ok(ResolveOutcome::Resolved | ResolveOutcome::OutcomeMismatch) => return,
                Ok(ResolveOutcome::Fenced) => {
                    tracing::debug!(
                        table,
                        ?txn_id,
                        attempt,
                        "txn resolve: fence-miss (a concurrent split likely moved this key's \
                         range) — re-routing with fresh metadata and retrying"
                    );
                }
                Err(e) => {
                    tracing::debug!(
                        table,
                        ?txn_id,
                        attempt,
                        %e,
                        "txn resolve: attempt failed with a retryable routing/leadership race; \
                         retrying"
                    );
                }
            }
            if attempt + 1 < TXN_RESOLVE_FENCED_RETRY_ATTEMPTS {
                self.env.sleep(TXN_STAGE_PUSH_BACKOFF).await;
            }
        }
    }

    /// **Cross-tablet status query** for `txn_id`'s record at `record_key`
    /// (`record_table`'s own tablet) — the wire-routed counterpart of
    /// [`RaftKvNode::txn_status_local`], used by [`cp_get_local`](Self::cp_get_local)'s
    /// foreign-intent path.
    pub(crate) async fn txn_status(
        &self,
        record_table: &str,
        record_key: &[u8],
    ) -> Result<TxnDecisionStatus, String> {
        match self.cp_route(record_table, record_key).await {
            CpRoute::Local(leader) => leader
                .txn_status_local(record_key)
                .await
                .ok_or_else(|| "CP group leader moved, or no record yet; retry".to_string()),
            CpRoute::Forward(addr, hinted) => {
                let request = ClientRequest::TxnStatus {
                    table: record_table.to_owned(),
                    record_key: record_key.to_vec(),
                };
                match self
                    .cp_forward(record_table, record_key, addr, hinted, request)
                    .await
                {
                    ClientResponse::TxnStatusReply { status } => Ok(status),
                    ClientResponse::Error(e) => Err(e),
                    other => Err(format!(
                        "unexpected reply to forwarded TxnStatus: {other:?}"
                    )),
                }
            }
            CpRoute::None => Err("no CP group leader reachable for txn status".into()),
        }
    }

    /// **Cross-tablet recovery view** for `txn_id`'s record at `record_key`
    /// (`record_table`'s own tablet) — the recovery-view dual of
    /// [`txn_status`](Self::txn_status): also returns `intent_spans`/
    /// `created_ts`, everything [`txn_recover`](Self::txn_recover) needs.
    ///
    /// **`Result<Option<..>, String>`, not `Result<.., String>` (issue #298
    /// shape B fix)**: `Ok(None)` means the answering leader's own read
    /// barrier **definitively confirmed no record exists** at this key —
    /// this is a real, trustworthy fact the orphan-record recovery path may
    /// safely act on. `Err(_)` means the query could not be served at all
    /// (routing failure, or the local leader's own barrier failing — see
    /// `RaftKvNode::txn_record_view`'s doc) — an inconclusive "I don't know,"
    /// never evidence of absence. Collapsing these two into one `Err`/`None`
    /// bucket (the pre-fix shape) let `txn_recover`'s orphan branch treat a
    /// merely-unreachable-right-now record identically to a genuinely
    /// nonexistent one, incorrectly synthesizing an abort tombstone for a
    /// transaction whose record (and live coordinator) was fine all along —
    /// the exact sibling of this same fix's `all_staged`/`txn_verify` half.
    pub(crate) async fn txn_record_view(
        &self,
        record_table: &str,
        record_key: &[u8],
    ) -> Result<Option<animus_cp_data::TxnRecordView>, String> {
        match self.cp_route(record_table, record_key).await {
            CpRoute::Local(leader) => leader
                .txn_record_view(record_key)
                .await
                .ok_or_else(|| "CP group leader moved during txn record view; retry".to_string()),
            CpRoute::Forward(addr, hinted) => {
                let request = ClientRequest::TxnRecordView {
                    table: record_table.to_owned(),
                    record_key: record_key.to_vec(),
                };
                match self
                    .cp_forward(record_table, record_key, addr, hinted, request)
                    .await
                {
                    ClientResponse::TxnRecordViewReply { view } => Ok(view),
                    ClientResponse::Error(e) => Err(e),
                    other => Err(format!(
                        "unexpected reply to forwarded TxnRecordView: {other:?}"
                    )),
                }
            }
            CpRoute::None => Err("no CP group leader reachable for txn record view".into()),
        }
    }

    /// **Cross-tablet staged-intent check**: does `table`'s tablet leader
    /// still hold a live intent for `txn_id` anywhere in `span`? Routed by
    /// `span.start` (an exact key — every span a record carries is the
    /// point-span shape `txn::immediate_successor` builds).
    async fn txn_verify(
        &self,
        table: &str,
        span: &KeyRange,
        txn_id: &TxnId,
    ) -> Result<bool, String> {
        match self.cp_route(table, &span.start).await {
            CpRoute::Local(leader) => leader
                .txn_verify_staged(span, txn_id)
                .await
                .ok_or_else(|| "CP group leader moved during txn verify; retry".to_string()),
            CpRoute::Forward(addr, hinted) => {
                let request = ClientRequest::TxnVerify {
                    table: table.to_owned(),
                    span: span.clone(),
                    txn_id: txn_id.clone(),
                };
                match self
                    .cp_forward(table, &span.start, addr, hinted, request)
                    .await
                {
                    ClientResponse::TxnVerifyReply { staged } => Ok(staged),
                    ClientResponse::Error(e) => Err(e),
                    other => Err(format!(
                        "unexpected reply to forwarded TxnVerify: {other:?}"
                    )),
                }
            }
            CpRoute::None => Err("no CP group leader reachable for txn verify".into()),
        }
    }

    /// Resolve every `(table, span)` in `intent_spans` per `status`
    /// (best-effort, fire-and-forget on any individual routing failure —
    /// see [`txn_resolve_participant`](Self::txn_resolve_participant)'s own
    /// doc): groups spans by **`(table, tablet)`** (a span's own exact key,
    /// `span.start`, is the key to resolve — every span this crate ever
    /// builds is a single-key point-span) and issues one
    /// `txn_resolve_participant` call per **tablet**. A no-op if `status` is
    /// still `Pending` (nothing to resolve yet).
    ///
    /// **ADR 0018 §2 write-loss amendment (Bug 3): grouping by table name
    /// alone used to be the bug.** `intent_spans` only ever names a
    /// `(table, span)` — a table, not a tablet — because a span is recorded
    /// at STAGE time from the writer's own key alone (`ClientCtx::cp_txn`'s
    /// `participant_spans`), never from a specific tablet id. A table with
    /// more than one tablet (any split table) can have two participants'
    /// keys share one table name but live on two different Raft groups.
    /// Grouping by table name alone used to bundle both into one
    /// `txn_resolve_participant` call; that call's own `cp_route(table,
    /// &first)` picks a single leader from the *first* key alone, so the
    /// rest of the bundle silently rode along to the wrong tablet. Because
    /// `KvCommand::TxnResolve` used to carry no fence at all, the wrong
    /// tablet applied the write anyway — onto the *same physical key*
    /// (ADR 0028: a table's tablets share one `StorageScope` prefix), MVCC-
    /// stamped with the wrong tablet's own clock. The right tablet's own
    /// clock never learns of that foreign version and can never mint above
    /// it again: every future write to that key silently loses the per-key
    /// LWW race, forever. Re-resolving each key's own **current** tablet
    /// here (immediately before grouping, via the same [`tablet_for`]
    /// [`ClientCtx::cp_txn`] itself uses at stage time) closes this at the
    /// source; [`KvCommand::TxnResolve`]'s own apply-time fence (added in
    /// the same amendment, mirroring `TxnStage`'s) is the structural
    /// seatbelt for every other caller (present or future) that might make
    /// the identical mistake. A key whose tablet can't be resolved right
    /// now (a genuinely transient routing gap) is skipped, not failed
    /// whole-batch — this whole call is best-effort fire-and-forget by
    /// design (a later resolver-loop tick, or the live coordinator's own
    /// resolve, picks up anything left over).
    ///
    /// ADR 0018 §2/PR6 torn-resolve audit: `status` must always be a
    /// **post-decision re-read** (`txn_status_local`/`txn_record_view`,
    /// or `TxnTracker::unresolved_decided`'s own tracked outcome — itself
    /// only ever inserted at the moment this group's own apply flips
    /// `Pending -> Committed`/`Aborted`, ADR 0018 §2/PR5), never a
    /// decider's own candidate/proposed ts. Every caller in this crate
    /// (`cp_txn`'s `resolve_all`, `txn_recover` below,
    /// `txn_resolver_loop`) already satisfies this; verified by this
    /// audit, not merely assumed.
    pub(crate) async fn recovery_resolve(
        &self,
        txn_id: TxnId,
        record_key: Vec<u8>,
        intent_spans: &[(String, KeyRange)],
        status: &TxnDecisionStatus,
    ) {
        let outcome = match status {
            TxnDecisionStatus::Committed { commit_ts } => TxnOutcome::Committed {
                commit_ts: *commit_ts,
            },
            TxnDecisionStatus::Aborted => TxnOutcome::Aborted,
            TxnDecisionStatus::Pending => return,
        };
        let mut by_table_tablet: BTreeMap<(String, TabletId), Vec<Vec<u8>>> = BTreeMap::new();
        for (table, span) in intent_spans {
            let key = span.start.clone();
            // Re-resolve NOW, not at stage time (`intent_spans` carries no
            // tablet id at all — see this method's own doc) — a genuinely
            // unroutable key (table/tablet not currently resolvable) is
            // skipped, not fatal to the rest of this best-effort resolve.
            let Some(tablet) = self.tablet_for(table, &key) else {
                continue;
            };
            by_table_tablet
                .entry((table.clone(), tablet))
                .or_default()
                .push(key);
        }
        for ((table, _tablet), keys) in by_table_tablet {
            self.txn_resolve_participant_retrying(
                &table,
                txn_id.clone(),
                record_key.clone(),
                keys,
                outcome.clone(),
            )
            .await;
        }
    }

    /// **Push a transaction record to a decision** (ADR 0018 §2/PR5's
    /// "recovery" mechanism — the CockroachDB "no blocking on a dead
    /// coordinator" property the Decision section's Recovery bullet
    /// promises): any actor holding a foreign-or-local `Pending` intent past
    /// [`animus_cp_data::RECOVERY_GRACE`] may call this to drive the
    /// transaction to a decision. Callable both from a reader that just hit
    /// a stale `Pending` intent (the read-path push) and from
    /// `txn_resolver_loop`'s own periodic sweep.
    ///
    /// **Protocol** (see the ADR's PR5 amendment for the full safety
    /// argument):
    /// 1. Read the record ([`txn_record_view`](Self::txn_record_view)). If
    ///    already decided, resolve every participant and return the
    ///    decision — no need to re-decide.
    /// 2. **If no record exists at all** (ADR 0018 §2/PR5's orphan-record
    ///    fix — a real, already-acknowledged possibility: PR4's prepare
    ///    phase is concurrent, so a participant's own stage can succeed and
    ///    be discovered by a reader while the *anchor's* `TxnStage` — which
    ///    would create this transaction's record — never lands at all,
    ///    e.g. a fence/seal miss the coordinator's propose outcome alone
    ///    can't distinguish from a genuine stage, PR4's own documented gap,
    ///    now applying to the anchor's own stage too): there is no
    ///    `created_ts` to grace-gate against. `intent_ts_hint` (typically
    ///    the orphaned intent's own applied timestamp,
    ///    [`animus_cp_data::IntentInfo::version`]) is the pusher's only
    ///    trustworthy substitute; with none supplied, decline
    ///    conservatively (never wrongly abort something we can't even
    ///    time-bound). Past grace on that substitute, this can ONLY ever
    ///    decide **abort** — an absent record means there is no candidate
    ///    participant list to verify "all staged" against, so committing
    ///    would be unsound; aborting is always safe (see
    ///    [`RaftKvNode::txn_abort_orphan`]'s doc). The synthesized
    ///    tombstone also closes a related hazard: a **late-arriving**
    ///    genuine anchor `TxnStage` for this same `txn_id` finds it and
    ///    no-ops instead of resurrecting a `Pending` record
    ///    (`KvCommand::TxnStage`'s own resurrection guard).
    /// 3. If `Pending` and not yet past grace, decline (`Pending`) — a live
    ///    coordinator may still be working on it.
    /// 4. If `Pending` and stale: verify every `(table, span)` in
    ///    `intent_spans` ([`txn_verify`](Self::txn_verify)). All staged →
    ///    propose `TxnCommit`; any missing (or any verify query itself
    ///    failing — conservatively treated as "not confirmed staged") →
    ///    propose `TxnAbort`.
    /// 5. Either proposal may **lose** to a concurrent decision (a
    ///    still-live coordinator, or a duelling recoverer) — re-read the
    ///    record's actual status and act on THAT, never on what was
    ///    proposed (see `txn_decide_anchor`'s doc for the identical
    ///    argument on the coordinator side).
    /// 6. Resolve every participant per the final, actual decision.
    ///
    /// **Grace is liveness-only**: whether this call even attempts step 3
    /// affects only *when* a decision might be pushed, never *what* it
    /// decides once pushed — a recovery commit requires every span
    /// independently verified staged, exactly the coordinator's own commit
    /// precondition, so a recovery commit and a coordinator's own commit
    /// are the SAME decision; a recovery abort can only ever race a
    /// still-live coordinator's late prepare, in which case the
    /// coordinator's own subsequent commit attempt simply loses (step 4's
    /// mechanism) and the client correctly sees an abort.
    pub(crate) async fn txn_recover(
        &self,
        record_table: &str,
        record_key: &[u8],
        txn_id: &TxnId,
        intent_ts_hint: Option<HlcTimestamp>,
    ) -> Result<TxnDecisionStatus, String> {
        let view = match self.txn_record_view(record_table, record_key).await {
            Ok(Some(view)) => view,
            // ADR 0018 §2/PR5 correctness amendment (issue #298 shape B, the
            // `txn_record_view` sibling of the `txn_verify` fix below): an
            // `Err` here means the query itself could not be served (a
            // routing failure, or this replica's own read barrier failing —
            // e.g. mid-fork/cutover, exactly what a high split cadence
            // produces routinely) — it is UNKNOWN whether a record exists,
            // never evidence that it doesn't. Only `Ok(None)` — the
            // answering leader's own barrier-confirmed, definitive "no
            // record at this key" — may ever feed the orphan-abort logic
            // below. Treating an `Err` identically used to let a transient
            // routing hiccup synthesize an abort tombstone for a
            // transaction whose record was fine and simply unreachable by
            // *this* particular query attempt — permanently losing an
            // already-staged (or already-committed) write.
            Err(_) => return Ok(TxnDecisionStatus::Pending),
            Ok(None) => {
                // Step 1b: no record at all. Without a substitute clock we
                // cannot tell "genuinely stale" from "the anchor's stage is
                // simply still in flight" — decline rather than guess.
                let Some(hint_ts) = intent_ts_hint else {
                    return Ok(TxnDecisionStatus::Pending);
                };
                let now_ms = match self.cp_route(record_table, record_key).await {
                    CpRoute::Local(leader) => leader.env().now().0 / 1_000_000,
                    // `Nanos` has no `elapsed()` (`tokio::time::Instant::
                    // elapsed` is a tokio-only convenience) — two
                    // back-to-back `now()` reads and a saturating diff
                    // reproduce the identical near-zero duration the
                    // original `tokio::time::Instant::now().elapsed()`
                    // always measured here (the gap between minting and
                    // reading its own instant, not any real wait).
                    _ => {
                        let t = self.env.now();
                        t.duration_since(self.env.now()).as_millis() as u64
                    }
                };
                if now_ms < hint_ts.wall_ms + animus_cp_data::RECOVERY_GRACE.as_millis() as u64 {
                    return Ok(TxnDecisionStatus::Pending);
                }
                // Always an abort — see this method's own doc for why an
                // absent record can never safely commit.
                let proposed = self
                    .txn_decide_anchor(
                        record_table,
                        txn_id.clone(),
                        record_key.to_vec(),
                        false,
                        HlcTimestamp::zero(),
                        Some(hint_ts),
                    )
                    .await?;
                let decided_status = outcome_to_status(&proposed);
                // Re-read for whatever `intent_spans` now exist (typically
                // empty for a fresh tombstone — this pusher only ever knew
                // about the one intent that triggered it, not the whole
                // transaction's participant set, since no record existed
                // to learn that from). A failure here is harmless: the
                // caller that triggered this push (e.g.
                // `cp_get_local_resolving`) still finishes its own read
                // off the returned status regardless of whether this
                // fan-out resolve runs.
                if let Ok(Some(final_view)) = self.txn_record_view(record_table, record_key).await {
                    self.recovery_resolve(
                        txn_id.clone(),
                        record_key.to_vec(),
                        &final_view.intent_spans,
                        &decided_status,
                    )
                    .await;
                }
                self.record_recovery_metric(&proposed);
                return Ok(decided_status);
            }
        };
        if !matches!(view.status, TxnDecisionStatus::Pending) {
            self.recovery_resolve(
                txn_id.clone(),
                record_key.to_vec(),
                &view.intent_spans,
                &view.status,
            )
            .await;
            return Ok(view.status);
        }

        // Grace check (liveness-only — see this method's own doc): compare
        // against any reachable env's wall clock, since the pusher may be a
        // different node than the one that minted the record. `cp_route`
        // always resolves *some* local or forwarded leader for `record_key`
        // itself, so re-route here rather than plumb a fresh `Env` handle
        // through just for a clock read.
        let now_ms = match self.cp_route(record_table, record_key).await {
            CpRoute::Local(leader) => leader.env().now().0 / 1_000_000,
            // A forwarded caller has no local env to read; approximate with
            // this node's own — the grace window is generous (seconds) and
            // liveness-only, so modest cross-node clock skew here is
            // harmless (it can only shift *when* a push is attempted).
            // `Nanos` has no `elapsed()` (`tokio::time::Instant::elapsed` is
            // a tokio-only convenience) — two back-to-back `now()` reads and
            // a saturating diff reproduce the identical near-zero duration
            // the original `tokio::time::Instant::now().elapsed()` always
            // measured here.
            _ => {
                let t = self.env.now();
                t.duration_since(self.env.now()).as_millis() as u64
            }
        };
        if now_ms < view.created_ts.wall_ms + animus_cp_data::RECOVERY_GRACE.as_millis() as u64 {
            return Ok(TxnDecisionStatus::Pending);
        }

        // ADR 0018 §2/PR5 correctness amendment (issue #298 shape B): a
        // `txn_verify` `Err` is a transient routing failure (most commonly
        // "no CP group leader reachable" while a participant's tablet is
        // mid-fork/cutover) — it means "could not verify," never "verified
        // absent." Folding it into the same bucket as a genuine `Ok(false)`
        // used to let recovery decide **Abort** for a transaction whose
        // coordinator (`cp_txn`) is concurrently committing (or has already
        // committed) from its own, unaffected view — losing an already-acked
        // write. Only a span that is either affirmatively verified staged
        // (`Ok(true)`) or affirmatively verified NOT staged (`Ok(false)`) may
        // ever feed a decision; any `Err` makes the whole push inconclusive
        // and this call **declines** (returns `Pending`, proposing nothing)
        // instead of guessing — the identical "never fabricate a fact this
        // call didn't actually observe" discipline `recovery_resolve`'s own
        // doc already holds callers to for `intent_spans`.
        let mut all_staged = true;
        let mut inconclusive = false;
        for (table, span) in &view.intent_spans {
            match self.txn_verify(table, span, txn_id).await {
                Ok(true) => {}
                Ok(false) => all_staged = false,
                Err(_) => inconclusive = true,
            }
        }
        if inconclusive {
            // Metered (never logged at more than debug — an in-flight split
            // routinely produces a handful of these, not worth `warn!`
            // volume on its own) so an operator can see how often recovery
            // is going inconclusive; `txn_resolver_loop`'s own grace tracker
            // is what escalates a **stuck** case (this call declining on
            // every tick for the same txn_id well past `RECOVERY_GRACE`) to
            // a `warn!` + a distinct metric.
            if let Some(data) = self.data.as_ref() {
                data.raftkv_metrics
                    .incr(Metric::CpTxnRecoveryVerifyInconclusive);
            }
            tracing::debug!(
                ?txn_id,
                record_table,
                ?record_key,
                "txn_recover: at least one participant's txn_verify errored (transient routing \
                 failure, e.g. mid-fork/cutover) — declining rather than risking a wrong Abort \
                 against a possibly still-live coordinator; retried next sweep"
            );
            return Ok(TxnDecisionStatus::Pending);
        }

        let candidate = view.created_ts;
        let proposed = self
            .txn_decide_anchor(
                record_table,
                txn_id.clone(),
                record_key.to_vec(),
                all_staged,
                candidate,
                None,
            )
            .await?;

        let decided_status = outcome_to_status(&proposed);
        self.recovery_resolve(
            txn_id.clone(),
            record_key.to_vec(),
            &view.intent_spans,
            &decided_status,
        )
        .await;

        self.record_recovery_metric(&proposed);
        Ok(decided_status)
    }

    /// Records the `CpTxnRecoveredCommitted`/`CpTxnRecoveredAborted` metric
    /// for a just-completed recovery decision — shared by both
    /// [`txn_recover`](Self::txn_recover) branches (the ordinary decided-
    /// record path and the orphan-record path).
    fn record_recovery_metric(&self, proposed: &TxnOutcome) {
        if let Some(data) = self.data.as_ref() {
            match proposed {
                TxnOutcome::Committed { .. } => {
                    data.raftkv_metrics.incr(Metric::CpTxnRecoveredCommitted);
                }
                TxnOutcome::Aborted => {
                    data.raftkv_metrics.incr(Metric::CpTxnRecoveredAborted);
                }
            }
        }
    }

    /// **Multi-participant transaction** (ADR 0018 §2/PR4): atomically write
    /// every `(table, key, Option<value>)` in `writes` across however many
    /// tablets they span. `preconditions` — `(table, key, expected)`,
    /// `expected: None` meaning "must be absent" — are checked once before
    /// staging and **re-checked right before the commit decision**; a
    /// precondition that no longer matches aborts the whole transaction with
    /// a retryable conflict error instead of committing.
    ///
    /// **A deliberate simplification versus the ADR's precise design** (see
    /// the PR4 amendment): the ADR describes evaluating preconditions at a
    /// specific read timestamp `R` and refreshing via an HLC-timestamped
    /// re-read only if the final `commit_ts` exceeds `R`. Exposing a read's
    /// serve timestamp back to a wire caller (so it could later be compared
    /// against the eventual `commit_ts`) is not yet wired on the client
    /// protocol — only `read_at` (an explicit, caller-chosen `ts`) is, not
    /// "tell me the `ts` an ordinary linearizable read happened to serve
    /// at". This re-checks by **value** (an ordinary linearizable read,
    /// twice) instead, bounding the same race (a conflicting write landing
    /// between prepare and commit) without that extra wire primitive —
    /// correct for the stated goal, but not byte-for-byte the ADR's
    /// mechanism. Flagged here and in the ADR amendment as a follow-up.
    ///
    /// **Flow** (ADR 0018 §3, the PR4 amendment, and the PR5 amendment
    /// lifting its one deliberate deviation): group `writes` by owning
    /// tablet; the first write's tablet is the **anchor** (stages first,
    /// synchronously — it mints the `TxnId`/record key every participant
    /// needs, and its record's `intent_spans` name **every** participant,
    /// ADR 0018 §2/PR5). Every other participant then stages
    /// **concurrently** (`futures::future::join_all`). `staged` tracks
    /// every participant that actually needs resolving, the anchor's own
    /// keys included (PR5: `txn_decide_anchor` no longer resolves anything
    /// inline). Any prepare failure — or a failed pre-commit precondition
    /// re-check — proposes an abort on the anchor; on success, `commit_ts`
    /// is the anchor's own `txn_commit_at_least` result, floored at the max
    /// of every participant's acked stage ts — the single Raft commit on
    /// the anchor's record IS the atomic commit point.
    ///
    /// **Every decide attempt reports the record's ACTUAL outcome, not what
    /// was asked for** (ADR 0018 §2/PR5 decision-semantics amendment): with
    /// recovery, a duelling decider is legal — an abort attempt can lose to
    /// a concurrent recovery *commit* (every participant genuinely staged,
    /// from recovery's independent point of view), and a commit attempt can
    /// lose to a concurrent recovery *abort*. This method always branches
    /// on what actually happened, never on which decision it proposed.
    ///
    /// **Resolve is asynchronous, post-ack, on the successful-commit path**
    /// (ADR 0018 §2/PR5 — the PR4 amendment's own flagged deviation, now
    /// lifted): once the anchor's commit is durable, this returns
    /// immediately and spawns a best-effort resolve of every participant
    /// (anchor's own keys included) in the background — safe to leave
    /// un-awaited now that `txn_resolver_loop` exists as the safety net
    /// that eventually finishes any resolve this spawn doesn't get to (a
    /// crash, a transient forward failure). The abort paths still resolve
    /// synchronously before returning — there is no successful ack to speed
    /// up on an error return, so the extra safety margin costs nothing.
    ///
    /// **ADR 0046 D1 amendment (re-scoped under ADR 0049)**: for a
    /// transaction touching at least one **images-carrying** table (an
    /// index or a stream — `dynamo::txn_resolve_awaited`), the async-spawn
    /// above is instead an **awaited, bounded** resolve
    /// (`TXN_RESOLVE_ALL_AWAIT_BUDGET`, parallelized across participants
    /// via `resolve_all_parallel`) — LSI rows and the GSI/stream change
    /// record only appear at resolve (materialize-at-resolve, A1), so an
    /// ack-then-async-resolve window would leave a committed write readable
    /// on the base table but transiently absent from its index/stream. A
    /// timeout still acks (delayed, never denied). Every other transaction
    /// — including a marker-table one, which since ADR 0049 also stages
    /// `pending` kind writes but has no index/stream consumer to protect —
    /// keeps the fire-and-forget spawn and the **sequential** `resolve_all`
    /// (parallelizing it universally measurably destabilized the torn-pair
    /// hard-gate test under concurrent load, twice now: once during the D1
    /// delivery, and again when ADR 0049's constant-true gate briefly
    /// re-universalized it by implication; see `resolve_all_parallel`'s own
    /// doc and `dynamo::txn_resolve_awaited`'s for the full account).
    ///
    /// **`write_conditions`** (ADR 0018 §2 apply-time write-key conditions
    /// amendment) — `(table, key, expected)` own-key byte-level OCC
    /// conditions checked at *apply* time on the key's own tablet, upgrading
    /// a write action's own condition from same-node-only protection (this
    /// amendment predates ADR 0054; the `rmw_lock` it refers to is long
    /// since deleted, step 4b) to full cross-node correctness: `key` MUST be
    /// one of `writes`' own keys (an `Err` otherwise) — a condition on a key
    /// this transaction does not write belongs in `preconditions` instead
    /// (see [`TxnWriteCondition`]'s doc for why mixing them up is exactly
    /// the self-referential-stall bug the PR7 amendment documented).
    /// Split one (table, tablet) group's [`TxnTableWrite`]s into the
    /// already-concrete writes `RaftKvNode::txn_stage_anchor`/
    /// `txn_stage_participant` can take directly, and the pending
    /// kind-write-path ones [`ClientCtx::txn_stage_local`] must still
    /// evaluate at the leader (ADR 0046 U3, PR2) — see [`TxnTableWrite`]'s
    /// doc for why exactly one of `value`/`pending` is ever `Some`.
    fn split_group(
        group: Vec<TxnTableWrite>,
    ) -> Result<(Vec<TxnWrite>, Vec<PendingKindWrite>), String> {
        let mut writes = Vec::new();
        let mut pending = Vec::new();
        for w in group {
            match (w.value, w.pending) {
                // A plain-value transactional write only ever comes from the
                // raw client protocol now (`ClientRequest::Txn` — the Dynamo
                // edge's `run_transact` always builds `pending` kind-write
                // specs under ADR 0049's constant-true gate), and it too must
                // leave the ADR 0049 §3 stage marker: a raw write staged
                // during an ADR 0050 split build would otherwise be invisible
                // to the build's change-log tail until resolve — which can
                // land after the parent is gone. Prefix = the write's own
                // full key bytes (a raw write has no pk/sk decomposition —
                // the finest per-key dirty hint, leading with the key's own
                // token so the apply-time token validation holds), `base_sk`
                // empty.
                (Some(value), None) => {
                    let marker = crate::dynamo::stage_marker_change_log(&w.key, Vec::new());
                    let mut write = TxnWrite::plain(w.key, Some(value));
                    write.stage_marker = Some(marker);
                    writes.push(write);
                }
                (None, Some(p)) => pending.push(p),
                (None, None) => {
                    return Err(format!(
                        "cp_txn: write to table `{}` key {:?} has neither a value nor a \
                         pending kind-write spec",
                        w.table, w.key
                    ));
                }
                (Some(_), Some(_)) => {
                    return Err(format!(
                        "cp_txn: write to table `{}` key {:?} has both a value and a pending \
                         kind-write spec (exactly one is expected)",
                        w.table, w.key
                    ));
                }
            }
        }
        Ok((writes, pending))
    }

    pub(crate) async fn cp_txn(
        &self,
        writes: Vec<TxnTableWrite>,
        preconditions: Vec<TxnPrecondition>,
        write_conditions: Vec<TxnWriteCondition>,
    ) -> Result<HlcTimestamp, TxnAbortReason> {
        if writes.is_empty() {
            return Err(TxnAbortReason::Other(
                "cp_txn: writes must be non-empty".into(),
            ));
        }
        // **Load-bearing validation, not a redundant belt-and-suspenders
        // check**: `RaftKvNode::txn_stage` (the anchor's own stage) hard-
        // `assert!`s its anchor key is at least `TOKEN_BYTES` long (ADR
        // 0022) — a sound invariant when only trusted internal callers
        // (a test, or the Dynamo edge, which always builds ADR-0022-shaped
        // keys) ever reached it. This is the **first** wire-facing caller
        // that can hand it an arbitrary client-supplied key — a short key
        // would panic this whole node process (a real DoS vector), not
        // fail gracefully. Validate every write's key up front (not just
        // the anchor's — a future reordering of `writes` should not
        // resurface this) and return a client-facing error instead of ever
        // reaching that assert.
        if let Some(w) = writes.iter().find(|w| w.key.len() < TOKEN_BYTES) {
            return Err(TxnAbortReason::Other(format!(
                "txn key {:?} of table `{}` must be at least {TOKEN_BYTES} bytes long \
                 (ADR 0022) for a multi-participant transaction",
                w.key, w.table
            )));
        }

        // Auto-provision every distinct table's first tablet on demand, like
        // `cp_write`.
        let mut seen_tables: BTreeSet<String> = BTreeSet::new();
        for w in &writes {
            if seen_tables.insert(w.table.clone())
                && !self.effective_metadata().has_table_tablet(&w.table)
            {
                self.provision_tablet(&w.table)
                    .await
                    .map_err(TxnAbortReason::Other)?;
            }
        }

        // Precondition check #1 (pre-stage). A mismatch here is a
        // `ConditionCheck` action's own cross-key OCC (`preconditions`, never
        // a write's own key — see `TxnWriteCondition`'s doc) — not one of
        // this amendment's two typed reasons (ADR 0018's 2026-08-24
        // `CancellationReasons` amendment, issue #374 C2b left this path
        // aggregate-only; `dynamo.rs::run_transact`'s own coordinator-side
        // preflight already flags a `ConditionCheck` failure by index before
        // `cp_txn` is ever called, so this re-check only fires on a genuine
        // race the preflight couldn't have seen).
        let observed = self
            .check_preconditions(&preconditions)
            .await
            .map_err(TxnAbortReason::Other)?;

        // Own-key condition lookup, consumed (via `remove`) as `writes` is
        // grouped below — whatever's left over named a key that isn't one
        // of `writes`' own, a caller error (see `write_conditions`'s doc).
        let mut condition_map: BTreeMap<(String, Vec<u8>), Option<Vec<u8>>> = BTreeMap::new();
        for (table, key, expected) in write_conditions {
            condition_map.insert((table, key), expected);
        }

        // ADR 0046 D1 (re-scoped under ADR 0049): whether this transaction
        // must AWAIT its post-commit resolve — only when a pending write
        // targets a table whose change records carry images (an index or a
        // stream; the consumer-visibility rationale D1 actually rests on).
        // Since ADR 0049's constant-true write-path gate, `pending.is_some()`
        // alone is true for EVERY transaction, and keying this branch on it
        // silently universalized the awaited `resolve_all_parallel`
        // configuration that `resolve_all_parallel`'s own comment records as
        // reproduced-red on the torn-pair hard-gate test — which duly went
        // intermittently red again. See `dynamo::txn_resolve_awaited`'s doc.
        let awaits_resolve = {
            let meta = self.effective_metadata();
            dynamo::txn_resolve_awaited(&meta, &writes)
        };

        // Group by (table, tablet), preserving first-seen order — `order[0]`
        // is the anchor. `condition_groups` mirrors `groups`' keying, only
        // populated for a (table, tablet) that owns at least one
        // conditioned key. Kept as the un-split `TxnTableWrite` (ADR 0046
        // U3, PR2) here — a group can mix plain (already-known) writes and
        // pending kind-write-path ones; [`split_group`] separates them right
        // before each group is actually staged.
        let mut order: Vec<(String, TabletId)> = Vec::new();
        let mut groups: BTreeMap<(String, TabletId), Vec<TxnTableWrite>> = BTreeMap::new();
        let mut condition_groups: BTreeMap<(String, TabletId), StageConditions> = BTreeMap::new();
        for w in writes {
            let tablet = self.tablet_for(&w.table, &w.key).ok_or_else(|| {
                TxnAbortReason::Other(format!("no tablet owns a txn key of table `{}`", w.table))
            })?;
            if let Some(expected) = condition_map.remove(&(w.table.clone(), w.key.clone())) {
                condition_groups
                    .entry((w.table.clone(), tablet))
                    .or_default()
                    .push((w.key.clone(), expected));
            }
            let gk = (w.table.clone(), tablet);
            if let std::collections::btree_map::Entry::Vacant(e) = groups.entry(gk.clone()) {
                e.insert(Vec::new());
                order.push(gk.clone());
            }
            groups.get_mut(&gk).expect("just inserted").push(w);
        }
        if let Some(((table, key), _)) = condition_map.into_iter().next() {
            return Err(TxnAbortReason::Other(format!(
                "cp_txn: a write-key condition named {table}/{key:?}, which is not one of this \
                 transaction's own write keys — use `preconditions` for a condition on a key \
                 this transaction does not write"
            )));
        }

        let anchor_gk = order[0].clone();
        let anchor_group = groups.remove(&anchor_gk).expect("anchor group present");
        let anchor_conditions = condition_groups.remove(&anchor_gk).unwrap_or_default();
        let (anchor_table, _anchor_tablet) = anchor_gk;
        let anchor_keys: Vec<Vec<u8>> = anchor_group.iter().map(|w| w.key.clone()).collect();
        let (anchor_writes, anchor_pending) =
            Self::split_group(anchor_group).map_err(TxnAbortReason::Other)?;

        // ADR 0018 §2/PR5 (task #18 fix): the anchor's record must name
        // every OTHER participant's `(table, span)` pairs up front, not
        // just its own — `groups` (with the anchor's own entry already
        // removed above) holds exactly that. Without this, in-doubt
        // recovery's `all_staged` check (`ClientCtx::txn_recover`) only
        // ever verifies the anchor's own keys against `intent_spans`,
        // trivially reporting "all staged" even when a real participant
        // never staged at all — a genuine cross-tablet atomicity
        // violation on the recovery path (see `docs/adr/
        // 0018-cross-tablet-transactions.md`'s corrective note on this).
        let participant_spans: Vec<(String, KeyRange)> = groups
            .iter()
            .flat_map(|((table, _tablet), group)| {
                let table = table.clone();
                group.iter().map(move |w| {
                    let mut end = w.key.clone();
                    end.push(0);
                    (table.clone(), KeyRange::new(w.key.clone(), Some(end)))
                })
            })
            .collect();

        let (txn_id, record_key, record_table, anchor_ts) = self
            .txn_prepare_pushing(
                &anchor_table,
                None,
                anchor_writes,
                anchor_conditions,
                participant_spans,
                anchor_pending,
            )
            .await?;

        // Every other participant stages concurrently.
        let participant_gks: Vec<(String, TabletId)> = order.into_iter().skip(1).collect();
        let participant_futs = participant_gks.iter().map(|gk| {
            let table = gk.0.clone();
            let group = groups.get(gk).expect("group present").clone();
            let conditions = condition_groups.get(gk).cloned().unwrap_or_default();
            let keys: Vec<Vec<u8>> = group.iter().map(|w| w.key.clone()).collect();
            let txn_id = txn_id.clone();
            let record_key = record_key.clone();
            let record_table = record_table.clone();
            async move {
                let (writes, pending) = match Self::split_group(group) {
                    Ok(split) => split,
                    Err(e) => return (table, keys, Err(TxnAbortReason::Other(e))),
                };
                let result = self
                    .txn_prepare_pushing(
                        &table,
                        Some((txn_id, record_key, record_table)),
                        writes,
                        conditions,
                        Vec::new(), // unused: a participant's own stage creates no record.
                        pending,
                    )
                    .await;
                (table, keys, result)
            }
        });
        let participant_results = futures::future::join_all(participant_futs).await;

        // ADR 0018 §2/PR5: `staged` now tracks *every* participant this
        // transaction actually touches, the anchor's own keys included —
        // `txn_decide_anchor` no longer resolves anything inline (recovery
        // needs the record's `intent_spans` to already list every
        // participant uniformly, and the resolve fan-out below treats them
        // identically too).
        let mut candidate = anchor_ts;
        let mut staged: Vec<(String, Vec<Vec<u8>>)> = vec![(anchor_table.clone(), anchor_keys)];
        let mut first_err: Option<TxnAbortReason> = None;
        for (table, keys, result) in participant_results {
            match result {
                Ok((_, _, _, ts)) => {
                    candidate = candidate.max(ts);
                    staged.push((table, keys));
                }
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }

        // Resolve every staged participant (best-effort, fire-and-forget —
        // a resolve failure never blocks a decision already durable on the
        // anchor; the resolver loop, ADR 0018 §2/PR5, is the safety net
        // that eventually finishes it).
        //
        // ADR 0018 §2/PR6 torn-resolve audit: every call site below passes
        // an `outcome` sourced from `txn_decide_anchor`'s own `Ok(..)`
        // return, which is itself always a **post-decision re-read**
        // (`txn_status_local`, inside `txn_decide_anchor`) — never the
        // caller's own proposed/candidate ts. This is load-bearing: once a
        // same-outcome-different-ts duplicate commit is a legal no-op
        // (ADR 0018 §2/PR6) rather than an assert, resolving with a
        // losing decider's own candidate instead of the actual, winning
        // decision would be exactly the torn-resolve hazard that
        // amendment's own review flagged.
        let resolve_all = |outcome: TxnOutcome, staged: Vec<(String, Vec<Vec<u8>>)>| {
            let this = self.clone();
            let txn_id = txn_id.clone();
            let record_key = record_key.clone();
            async move {
                for (table, keys) in staged {
                    this.txn_resolve_participant_retrying(
                        &table,
                        txn_id.clone(),
                        record_key.clone(),
                        keys,
                        outcome.clone(),
                    )
                    .await;
                }
            }
        };
        // ADR 0046 D1: a **parallel** sibling of `resolve_all` above, used
        // only by the awaited-bounded branch further down (a transaction
        // touching a kind-write-path table) — fanning out to every
        // participant's own tablet leader concurrently instead of one at a
        // time is what makes a short fixed budget plausible at all once
        // there's more than one participant. Deliberately **not** used for
        // the ordinary fire-and-forget spawn path above: that path's
        // resolves already run fully in the background with no latency
        // budget to protect, and switching it to `join_all` measurably
        // destabilized a pre-existing, already-timing-sensitive regression
        // test (`dynamo_txn.rs`'s
        // `transact_get_items_never_observes_a_torn_pair_under_concurrent_writes`,
        // a tight concurrent-writer loop where a resolve's own wall-clock
        // latency doubles as the next transaction's own staging retry
        // budget) — reproduced red with the parallel version applied
        // universally, green again scoped like this. Not fully root-caused
        // (plausibly increased concurrent Raft/network load momentarily
        // slowing an individual resolve under this test's specific tight
        // loop, not a correctness bug — every resolve still completes,
        // `txn_resolver_loop` is the safety net either way), but the
        // sequential default is the proven-stable one, so parallelism stays
        // opt-in to where D1 actually needs it.
        //
        // ADR 0049 postscript, proving the scoping is load-bearing: when the
        // constant-true write-path gate made every transaction stage
        // `pending` kind writes, the awaited branch below (then keyed on
        // "any pending") re-universalized this parallel path by implication
        // — and this same test went intermittently red again (a
        // budget-expired ack racing the writer's next same-key stage into
        // `TXN_STAGE_PUSH_ATTEMPTS` exhaustion). The branch is now keyed on
        // `dynamo::txn_resolve_awaited` (images-carrying tables only), which
        // restores the exact pre-ADR-0049 behavior for every marker-only
        // transaction.
        let resolve_all_parallel = |outcome: TxnOutcome, staged: Vec<(String, Vec<Vec<u8>>)>| {
            let this = self.clone();
            let txn_id = txn_id.clone();
            let record_key = record_key.clone();
            async move {
                let futs = staged.into_iter().map(|(table, keys)| {
                    let this = this.clone();
                    let txn_id = txn_id.clone();
                    let record_key = record_key.clone();
                    let outcome = outcome.clone();
                    async move {
                        this.txn_resolve_participant_retrying(
                            &table, txn_id, record_key, keys, outcome,
                        )
                        .await;
                    }
                });
                futures::future::join_all(futs).await;
            }
        };

        if let Some(reason) = first_err {
            // ADR 0018 §2/PR5 decision-semantics amendment: this abort
            // attempt can itself lose to a concurrent recovery *commit* on
            // the same record (every participant genuinely staged after
            // all, from recovery's independent point of view) — report the
            // record's **actual** outcome, never assume the abort we asked
            // for is what happened.
            match self
                .txn_decide_anchor_retrying(
                    &anchor_table,
                    txn_id.clone(),
                    record_key.clone(),
                    false,
                    candidate,
                )
                .await
            {
                Ok(TxnOutcome::Aborted) => {
                    resolve_all(TxnOutcome::Aborted, staged).await;
                    // Propagate the participant's own typed reason verbatim
                    // (ADR 0018's 2026-08-24 `CancellationReasons`
                    // amendment) — it already names the responsible action;
                    // wrapping it in another `Other` string here would erase
                    // the `ConditionFailed`/`TransactionConflict` distinction
                    // `dynamo.rs::run_transact` needs to flag the right index.
                    Err(reason)
                }
                Ok(TxnOutcome::Committed { commit_ts }) => {
                    resolve_all(TxnOutcome::Committed { commit_ts }, staged).await;
                    Ok(commit_ts)
                }
                Err(e) => Err(TxnAbortReason::Other(format!(
                    "transaction aborted: {reason} (and abort itself failed after retrying: {e}); \
                     retry"
                ))),
            }
        } else if !preconditions.is_empty()
            && self
                .check_preconditions(&preconditions)
                .await
                .map_err(TxnAbortReason::Other)?
                != observed
        {
            // Precondition check #2 (pre-commit refresh — see this method's own
            // doc for why this is a value re-check, not the ADR's ts-based one).
            match self
                .txn_decide_anchor_retrying(
                    &anchor_table,
                    txn_id.clone(),
                    record_key.clone(),
                    false,
                    candidate,
                )
                .await
            {
                Ok(TxnOutcome::Aborted) => {
                    resolve_all(TxnOutcome::Aborted, staged).await;
                    Err(TxnAbortReason::Other(
                        "a precondition changed between prepare and commit; retry".into(),
                    ))
                }
                Ok(TxnOutcome::Committed { commit_ts }) => {
                    resolve_all(TxnOutcome::Committed { commit_ts }, staged).await;
                    Ok(commit_ts)
                }
                Err(e) => Err(TxnAbortReason::Other(format!(
                    "transaction aborted: a precondition changed between prepare and commit \
                     (and abort itself failed after retrying: {e}); retry"
                ))),
            }
        } else {
            match self
                .txn_decide_anchor_retrying(
                    &anchor_table,
                    txn_id.clone(),
                    record_key.clone(),
                    true,
                    candidate,
                )
                .await
                .map_err(|e| {
                    TxnAbortReason::Other(format!(
                        "txn decide: anchor commit failed after every participant staged, even \
                         after retrying the same decision ({e}); retry"
                    ))
                })? {
                TxnOutcome::Committed { commit_ts } => {
                    // ADR 0018 §2/PR5: the deviation PR4 flagged, lifted —
                    // the anchor's commit is already durable and IS the
                    // atomic commit point (the client can be told "done"
                    // right now); every participant's resolve (anchor's own
                    // keys included) is best-effort and can safely happen
                    // after the ack, since a crash here leaves nothing
                    // ambiguous — `txn_resolver_loop` finishes it. This is
                    // strictly safer than the interim synchronous shape,
                    // not merely faster: it no longer holds the client
                    // response hostage to every participant's own
                    // liveness/latency.
                    //
                    // ADR 0046 D1: for a transaction touching any
                    // images-carrying table (index/stream), LSI rows and
                    // the stream/GSI change record only appear at resolve
                    // (materialize-at-resolve, A1) — an ack-then-async-
                    // resolve window would leave a committed write readable
                    // on the base table but transiently absent from its
                    // index/stream. Await `resolve_all` under a short
                    // bounded budget first; a timeout still acks (delayed,
                    // never denied — `txn_resolver_loop` remains the safety
                    // net for whatever the bound didn't cover). Every other
                    // transaction — marker-only tables included, since ADR
                    // 0049 — keeps the original fire-and-forget sequential
                    // spawn unchanged (see `dynamo::txn_resolve_awaited`).
                    if awaits_resolve {
                        // Race against the budget instead of `tokio::time::
                        // timeout` (no `Env` equivalent) — `Box::pin` the
                        // resolve future since an `async move` block capturing
                        // locals across `.await` points is not `Unpin` in
                        // general and `futures::future::select` requires both
                        // arms to be. Whichever resolves first is discarded
                        // either way (the pre-existing code discarded the
                        // `Result<(), Elapsed>` too), so this preserves the
                        // exact "await `resolve_all_parallel`, but never past
                        // `TXN_RESOLVE_ALL_AWAIT_BUDGET`" semantics.
                        let budget = Box::pin(resolve_all_parallel(
                            TxnOutcome::Committed { commit_ts },
                            staged,
                        ));
                        let _ = futures::future::select(
                            budget,
                            self.env.sleep(TXN_RESOLVE_ALL_AWAIT_BUDGET),
                        )
                        .await;
                    } else {
                        // `env.spawn_task` (ADR 0003) instead of raw
                        // `tokio::spawn` — under `ProdEnv` this is the same
                        // `tokio::spawn` underneath, so the detached,
                        // fire-and-forget lifetime is unchanged: this call
                        // still returns immediately, and the resolve runs to
                        // completion (or is dropped on process exit) with no
                        // handle kept here either way.
                        self.env
                            .spawn_task(resolve_all(TxnOutcome::Committed { commit_ts }, staged));
                    }
                    Ok(commit_ts)
                }
                // The anchor's own commit lost to a concurrent recovery
                // abort (a duelling decider, ADR 0018 §2/PR5) — report the
                // abort honestly rather than a false success.
                TxnOutcome::Aborted => {
                    resolve_all(TxnOutcome::Aborted, staged).await;
                    Err(TxnAbortReason::Other(
                        "transaction aborted: lost to a concurrent in-doubt-recovery decision"
                            .into(),
                    ))
                }
            }
        }
    }

    /// Read every `(table, key)` in `preconditions` (an ordinary
    /// linearizable read) and compare to its `expected` value (`None` =
    /// "must be absent"); `Err` on the first mismatch (a genuine, immediate
    /// precondition failure — not a routing error, so never retried).
    /// Returns the observed `(table, key, actual)` triples so
    /// [`cp_txn`](Self::cp_txn) can compare them again later (the pre-commit
    /// refresh check).
    async fn check_preconditions(
        &self,
        preconditions: &[TxnPrecondition],
    ) -> Result<Vec<TxnPrecondition>, String> {
        let mut observed = Vec::with_capacity(preconditions.len());
        for (table, key, expected) in preconditions {
            // A transaction precondition is an OCC check the commit
            // decision rests on: always linearizable (ADR 0055), never the
            // cheap path, whatever the client's own reads asked for.
            let actual = self
                .cp_read(table, key.clone(), ReadConsistency::Strong)
                .await?;
            if &actual != expected {
                return Err(format!(
                    "transaction precondition failed for {table}/{key:?}: expected {expected:?}, \
                     found {actual:?}"
                ));
            }
            observed.push((table.clone(), key.clone(), actual));
        }
        Ok(observed)
    }
}
