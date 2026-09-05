//! ClientCtx's write-path cluster (ADR 0061 rung C5 step 2): kind-scoped
//! item writes (`cp_kind_write_item`, `cp_kind_write_raw*`) and raw batch
//! propose/local-confirm plumbing (`poll_probe`, `cp_batch_local`,
//! `cp_put_local`/`cp_delete_local`). Moved verbatim out of `lib.rs`'s
//! `impl<E: Env> ClientCtx<E>` blocks -- no logic changes. (Split-child row
//! seeding — `seed_rows_local`/`seed_child_rows` — was deleted in the
//! copy-split-deletion endgame's Layer B1; `KvCommand::SeedBatch`/
//! `CpGroup::propose_seed_batch` stay, still used by restore.)

use std::time::Duration;

use animus_control::{Metadata, ProposeResult};
use animus_cp_data::{KindBatchOutcome, KindEvalOp};
use animus_dynamo::{AttributeValue, ConditionExpression, Item};
use animus_env::{Env, Nanos};
use animus_node::host::RelayClient;

use crate::{
    CLIENT_TIMEOUT, CP_CONFIRM_POLL_INIT, CP_CONFIRM_POLL_MAX, ClientCtx, ClientRequest,
    ClientResponse, CpGroup, CpRoute, KindBatchSignal, KindWriteOp, KvPair, ProbeIdentity,
    ProbeWait, SCHEMA_POLL_INTERVAL, classify_kind_batch_outcome, decide, dynamo,
};

/// The message [`ClientCtx::cp_kind_eval_local`] returns for a confirmed-
/// but-payload-lost `KindEval` (ADR 0054 mechanism 3's own documented
/// residual: the leader-local result slot ages out or this node's own
/// registration never got recorded) when there is nothing safe to
/// substitute — deliberately the SAME string [`ClientCtx::cp_kind_local`]'s
/// `poll_probe`-driven confirm returns for an ordinary confirm timeout
/// (`ProbeWait::TimedOut`), not a new one: both are the identical
/// wire-visible shape — "the write may have happened, but this node cannot
/// honestly report what changed" — and the caller-facing behavior (a
/// terminal `InternalServerError`, no retry — this string does not carry
/// the house `"; retry"` suffix) must match. See
/// [`ClientCtx::cp_kind_eval_local`]'s own doc for the two cases that reach
/// this vs. the safe best-effort re-read.
const KIND_EVAL_CONFIRM_AMBIGUOUS: &str = "CP kind write did not commit in time";

/// The confirmed, authoritative result of a [`KvCommand::KindEval`]
/// (`animus_cp_data`) once [`ClientCtx::cp_kind_eval_local`]'s propose has
/// resolved — the ADR 0054 step 3 analogue of the old `KindWriteOutcome`
/// pair, but built from the APPLY-side decision rather than a leader-side
/// evaluation. `dynamo::kind_write_item_at_leader` maps this straight into
/// `dynamo::KindWriteOutcome`/a `WireError`; nothing else should construct
/// or match on it.
///
/// [`KvCommand::KindEval`]: animus_cp_data::KvCommand::KindEval
pub(crate) enum KindEvalApplied {
    /// The write applied. `new: None` for a `Delete` op; `old: None` if the
    /// item did not previously exist.
    Ok {
        old: Option<Item>,
        new: Option<Item>,
    },
    /// The write's own `ConditionExpression` evaluated to `false` against
    /// apply's fresh read — a genuine `ConditionalCheckFailedException`.
    /// (Apply's identical outcome variant also covers an unresolved
    /// foreign transaction intent on this key — see this crate's own
    /// `KindEvalSeatbeltMismatch` doc and `kind_write_item_at_leader`'s
    /// comment on the mapping call site for why that rare race is not, and
    /// cannot cheaply be, told apart from a genuine condition failure at
    /// this layer.)
    ConditionFailed,
    /// The evaluator itself rejected the write — a domain violation in the
    /// client's own condition, or a malformed/oversized update — copied
    /// verbatim from `animus_cp_data::KindBatchOutcome::Rejected`.
    Rejected { code: String, message: String },
}

impl<E: Env, R: RelayClient> ClientCtx<E, R> {
    /// **The evaluate-at-leader write primitive (ADR 0046 U3)** —
    /// `PutItem`/`DeleteItem`/`UpdateItem`'s entry point on an indexed or
    /// streamed table, replacing the edge-evaluated
    /// `index_aware_write`/`ClientCtx::cp_kind_write` pairing those three
    /// call sites (plus `BatchWriteItem`'s indexed branch) used to go
    /// through. Resolves the item's own base key (recomputed from `pk`/`sk`,
    /// the single source of truth — never trusted from a caller-supplied
    /// key), then either serves **locally** (zero hops — this node hosts
    /// the leader, so [`dynamo::kind_write_item_at_leader`] runs in-process)
    /// or **forwards** [`ClientRequest::KindWriteItem`] one hop to the
    /// leader's node, inheriting `cp_forward`'s hinted-retry/backoff/
    /// election-wait exactly like every other CP write. See
    /// [`ClientRequest::KindWriteItem`]'s doc for why this closes the
    /// cross-node LSI/change-record orphan race `index_aware_write`'s
    /// design had.
    ///
    /// **Retries the retryable freeze refusal (issue #288).** A tablet mid
    /// split-cutover freeze (`FROZEN_REFUSAL`, ADR 0050 rung 5) refuses
    /// every mutating propose with a `"; retry"`-suffixed error *before*
    /// ever proposing — from `kind_write_item_at_leader`'s own pre-propose
    /// check when local, or the forwarded leader's identical check when not
    /// — so it's cheap and safe to retry. Mirrors [`cp_read`](Self::cp_read)'s
    /// deadline-bounded loop: bounded by [`CLIENT_TIMEOUT`], re-resolving
    /// `cp_route` every attempt (essential — after cutover the key routes to
    /// a child tablet, not the frozen parent), retrying only while
    /// [`decide::read_should_retry`] matches the error.
    /// Before this fix a client writing during a split's freeze window got a
    /// terminal error instead of the write succeeding once the child
    /// activates a moment later. The retry loop lives *outside*
    /// `kind_write_item_at_leader`'s own `rmw_lock` scope (issue #285 narrowed
    /// that lock to read+evaluate only), so retrying here — including the
    /// sleep between attempts — never pins the lock across the wait.
    pub(crate) async fn cp_kind_write_item(
        &self,
        meta: &Metadata,
        table: &str,
        pk: &animus_dynamo::AttributeValue,
        sk: Option<&animus_dynamo::AttributeValue>,
        op: KindWriteOp,
        condition: Option<&animus_dynamo::ConditionExpression>,
    ) -> Result<dynamo::KindWriteOutcome, animus_dynamo::wire::WireError> {
        // Auto-provision the table's tablet on first write (ADR 0023), as
        // `cp_kind_write` does — an indexed/streamed table's first item write
        // can race its own `CreateTable`'s tablet provisioning. Stays
        // outside the retry loop below: provisioning is itself idempotent,
        // so re-checking it every retry pass would just be a wasted
        // metadata read once the tablet exists.
        if !self.effective_metadata().has_table_tablet(table) {
            self.provision_tablet(table)
                .await
                .map_err(|e| dynamo::internal(&e))?;
        }
        let base_key = dynamo::item_key(pk, sk);
        // Whether this write may be safely re-applied; see the retry decision
        // at the bottom of the loop.
        let idempotent = dynamo::kind_write_is_idempotent(&op);
        let deadline = self.env.now().saturating_add(CLIENT_TIMEOUT);
        loop {
            let err = match self.cp_route(table, &base_key).await {
                CpRoute::Local(leader) => {
                    match dynamo::kind_write_item_at_leader(
                        self,
                        &leader,
                        meta,
                        table,
                        pk,
                        sk,
                        op.clone(),
                        condition,
                        // Ordinary client write — never the TTL reaper's own
                        // service identity (ADR 0051 §7; the reaper never
                        // calls through this routed helper — see
                        // `ttl_reaper.rs`).
                        false,
                    )
                    .await
                    {
                        Ok(outcome) => return Ok(outcome),
                        Err(e) => e,
                    }
                }
                CpRoute::Forward(addr, hinted) => {
                    let request = ClientRequest::KindWriteItem {
                        table: table.to_owned(),
                        pk: pk.clone(),
                        sk: sk.cloned(),
                        op: op.clone(),
                        condition: condition.cloned(),
                    };
                    match self
                        .cp_forward(table, &base_key, addr, hinted, request)
                        .await
                    {
                        ClientResponse::KindWriteOk {
                            old,
                            new,
                            collection_bytes,
                        } => {
                            return Ok(dynamo::KindWriteOutcome::Ok {
                                old,
                                new,
                                collection_bytes,
                            });
                        }
                        ClientResponse::ConditionFailed => {
                            return Ok(dynamo::KindWriteOutcome::ConditionFailed);
                        }
                        // The far side may carry a typed error's own code
                        // in the string (`dynamo::encode_relayed_error`);
                        // an unmarked string decodes to `internal`, the
                        // pre-marker behavior.
                        ClientResponse::Error(e) => dynamo::decode_relayed_error(&e),
                        other => {
                            return Err(dynamo::internal(&format!(
                                "unexpected reply to forwarded kind write item: {other:?}"
                            )));
                        }
                    }
                }
                CpRoute::None => dynamo::internal("no CP group leader reachable"),
            };
            // **At-most-once for a non-idempotent write.** This loop re-enters
            // `kind_write_item_at_leader`, which re-reads the old image and
            // re-applies the actions — a fresh read-modify-write, not a replay
            // of the original proposal. For every idempotent op (Put, Delete,
            // SET, REMOVE, a set union or difference) that converges to the
            // same state and the retry is free. A numeric `ADD` does not
            // converge, and a retryable error is not proof the write missed:
            // a failed OCC seatbelt applies as a silent no-op that the
            // confirm-poll reports exactly like a fence miss, so a write that
            // landed can still come back retryable. Retrying then counts twice.
            //
            // DynamoDB's guarantee is at-most-once **per request**, not
            // exactly-once: a *client* that retries an `ADD` which actually
            // applied does double-count there too. So the fix is not an
            // idempotency token — it is simply that the service must not
            // re-apply on its own. A non-idempotent write therefore gets one
            // attempt, and any transient failure is surfaced for the caller to
            // decide about, exactly as DynamoDB would.
            if !idempotent || !decide::read_should_retry(&err.message) || self.env.now() >= deadline
            {
                return Err(err);
            }
            self.env.sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// As [`cp_kind_write`](Self::cp_kind_write), but for a batch with **no
    /// base-kind write** — a GSI reconciliation's footprint/cursor-row
    /// update, or the trim janitor's change-record deletions (ADR 0042 §7/§8)
    /// — none of which touch a client-visible row.
    ///
    /// Confirmation therefore cannot probe a base row; it instead confirms
    /// the batch's **last** write actually landed (`local_get_kind`
    /// returning exactly what was asked for — `Some(value)` for a put,
    /// `None` for a tombstone) rather than stopping at `Accepted`, which only
    /// means "appended to the leader's log". A fenced-out entry commits as a
    /// no-op, so acking on acceptance alone would report an effect that
    /// never landed — and since the whole batch is **one** atomic Raft entry
    /// (`KvCommand::KindBatch`'s own whole-or-nothing apply gate), any single
    /// write's landed effect proves every other write in the same entry
    /// landed too; the last one is picked so a caller that orders its own
    /// "this batch is durable" signal last (the GSI drain's cursor-row bump,
    /// the trim janitor's final deletion) gets it confirmed, not merely an
    /// earlier entry in the same batch.
    ///
    /// **Retries the retryable freeze refusal (issue #288)** — the fast/
    /// marker-write arm's own share of the gap: this primitive backs plain
    /// (unindexed, unstreamed) Dynamo writes and the
    /// raw client protocol, none of which used to retry `FROZEN_REFUSAL`
    /// either. Same shape as [`cp_kind_write_item`](Self::cp_kind_write_item)'s
    /// identical fix: a deadline-bounded loop mirroring
    /// [`cp_read`](Self::cp_read), re-resolving `cp_route` every attempt so a
    /// post-cutover retry lands on the child tablet, retrying only while
    /// [`decide::read_should_retry`] matches the error.
    pub(crate) async fn cp_kind_write_raw(
        &self,
        table: &str,
        writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
        change_log: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), String> {
        self.cp_kind_write_raw_bounded(table, writes, change_log, CLIENT_TIMEOUT)
            .await
    }

    /// [`cp_kind_write_raw`](Self::cp_kind_write_raw)'s single-attempt
    /// sibling: identical write, but a `FROZEN_REFUSAL`-shaped failure
    /// returns immediately instead of retrying for `CLIENT_TIMEOUT` (issue
    /// #298). The one caller today is `reconcile_partition`'s GSI row
    /// write, invoked from the frozen-endgame acceleration loop
    /// (`FROZEN_ENDGAME_GSI_DRAIN_MAX_PASSES`) — see
    /// `index_drain::is_retryable_elsewhere`'s doc for why that loop must
    /// not spend up to `CLIENT_TIMEOUT` *per pass* blocked on a write whose
    /// own target tablet can, under a cascade, be mid-split and needing
    /// this SAME node's own `change_consumer_loop` to reach its turn before
    /// it ever un-freezes — multiplying a per-write retry budget by a
    /// per-pass loop count is exactly the shape that turned a few co-hosted
    /// splits into a multi-minute self-inflicted stall. A single fast
    /// failure here costs a fraction of a millisecond, so the loop's own
    /// pass count (or `change_consumer_loop`'s next ordinary 200ms tick)
    /// is the only retry budget actually spent.
    pub(crate) async fn cp_kind_write_raw_once(
        &self,
        table: &str,
        writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
        change_log: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), String> {
        self.cp_kind_write_raw_bounded(table, writes, change_log, Duration::ZERO)
            .await
    }

    async fn cp_kind_write_raw_bounded(
        &self,
        table: &str,
        writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
        change_log: Vec<(Vec<u8>, Vec<u8>)>,
        timeout: Duration,
    ) -> Result<(), String> {
        let Some(first) = writes.first().map(|(_, k, _)| k.clone()) else {
            return Ok(());
        };
        let deadline = self.env.now().saturating_add(timeout);
        loop {
            let err = match self.cp_route(table, &first).await {
                CpRoute::Local(leader) => {
                    match Self::cp_kind_raw_local(&leader, writes.clone(), change_log.clone()).await
                    {
                        Ok(()) => return Ok(()),
                        Err(e) => e,
                    }
                }
                CpRoute::Forward(addr, hinted) => {
                    let request = ClientRequest::KindWrite {
                        table: table.to_owned(),
                        writes: writes.clone(),
                        change_log: change_log.clone(),
                    };
                    match decide::ok_or_err(
                        self.cp_forward(table, &first, addr, hinted, request).await,
                        "forwarded CP kind write",
                    ) {
                        Ok(()) => return Ok(()),
                        Err(e) => e,
                    }
                }
                CpRoute::None => "no CP group leader reachable".to_string(),
            };
            if !decide::read_should_retry(&err) || self.env.now() >= deadline {
                return Err(err);
            }
            self.env.sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// The **known-leader** local half of [`cp_kind_write_raw`](Self::
    /// cp_kind_write_raw): fence pre-check, propose, then confirm on the
    /// batch's **last** write — `Some(value)` for a put, `None` for a
    /// tombstone (see `cp_kind_write_raw`'s doc for why the last write
    /// proves the whole entry). The ONE confirm implementation for a raw
    /// kind batch, shared by `cp_kind_write_raw`'s `Local` arm and
    /// `cp_serve_forwarded`'s `KindWrite` arm — they diverged once
    /// (the serve arm used [`cp_kind_local`](Self::cp_kind_local), whose
    /// confirm *requires* a `Some`-valued base write), so a raw batch whose
    /// base write is a tombstone erred iff the connected node did not lead
    /// the tablet (leader-placement-bimodal).
    pub(crate) async fn cp_kind_raw_local(
        leader: &CpGroup<E>,
        writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
        change_log: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), String> {
        // ADR 0050 rung 5: a frozen split parent refuses USER data (base/
        // LSI writes) but not consumer bookkeeping (cursor/footprint-only
        // batches — the GSI drain's own writes), which must keep flowing so
        // the drain can finish the frozen log and release the cutover veto
        // (the apply-time gate makes the identical distinction).
        if writes.iter().any(|(kind, _, _)| {
            *kind == animus_cp_data::KIND_BASE || *kind == animus_cp_data::KIND_LSI
        }) {
            decide::frozen_refusal(leader.is_frozen())?;
        }
        let fence = leader.scope_range();
        for (_, key, _) in &writes {
            if !fence.contains(key) {
                return Err("kind write outside this group's live range; retry".into());
            }
        }
        let Some((probe_kind, probe_key, probe_value)) = writes
            .last()
            .map(|(kind, key, value)| (*kind, key.clone(), value.clone()))
        else {
            return Ok(()); // empty batch is a no-op
        };
        let accepted_index = match leader.put_kind_batch_conditioned(writes, change_log, Vec::new())
        {
            ProposeResult::Accepted { index, .. } => index,
            other => return Err(format!("kind write not accepted: {other:?}")),
        };
        let deadline = leader.env().now().saturating_add(CLIENT_TIMEOUT);
        // The same exponential confirm back-off `cp_put_local` uses — NOT the
        // drain's old flat 10ms sleep. This is a client hot path since ADR
        // 0049 routed every plain Dynamo/raw-protocol write through it,
        // and a flat 10ms floor put one whole tick under nearly every
        // sequential write (measured on the ADR 0049 §5 bench: ~13.6 ms/op
        // vs the pre-train ~4.7 — the poll cadence, not the marker bytes).
        let mut poll = CP_CONFIRM_POLL_INIT;
        while leader.env().now() < deadline {
            if leader.local_get_kind(probe_kind, &probe_key).await == probe_value {
                return Ok(());
            }
            if decide::confirm_wait_is_futile(
                leader.engine_applied_index(),
                leader.is_leader(),
                accepted_index,
            ) {
                // Close the probe-vs-apply race: the entry may have applied
                // between the probe above and the futility read.
                if leader.local_get_kind(probe_kind, &probe_key).await == probe_value {
                    return Ok(());
                }
                return Err(
                    "kind batch superseded before its effect appeared (leadership churn \
                     or an apply-time no-op); retry"
                        .into(),
                );
            }
            leader.env().sleep(poll).await;
            poll = (poll * 2).min(CP_CONFIRM_POLL_MAX);
        }
        Err("kind batch did not apply in time".into())
    }

    /// Propose a `KindBatch` on a **known-leader** local handle and confirm it.
    ///
    /// Confirmation probes the batch's **base-kind** write, the one row a
    /// client can observe: `poll_probe` reads through the group's base scope,
    /// so an LSI/footprint/change-log write is not observable to it. Every
    /// caller includes a base write (a put's item, or a delete's tombstone
    /// *value*), so there is always a probe; a batch with none is refused
    /// rather than acked unconfirmed — a fenced-out entry commits as a no-op,
    /// so acking without a probe would falsely report a write that never
    /// happened (the hazard `cp_batch_local`'s doc spells out).
    ///
    /// **`conditions` (ADR 0046 U3, `pub(crate)` since [`dynamo::
    /// kind_write_item_at_leader`] calls this from outside `impl ClientCtx`)**:
    /// threaded straight through to `put_kind_batch_fenced`'s own
    /// `KvCommand::KindBatch.conditions` field — see that field's doc. Every
    /// pre-existing caller here passes an empty `Vec` (zero behavior
    /// change); `kind_write_item_at_leader` is the one caller that supplies
    /// its own-key OCC seatbelt. A failed condition no-ops the whole batch
    /// silently, indistinguishable from a fence miss, so it surfaces through
    /// this same function's existing `"CP kind write did not commit in
    /// time"` timeout — deliberately no new outcome channel.
    ///
    /// **`identity` (issue #469)**: threaded straight through to
    /// [`poll_probe`](Self::poll_probe)'s value-equality fallback gate — see
    /// [`ProbeIdentity`]'s doc for why an idempotent write may confirm on
    /// value equality alone but a non-idempotent one (a numeric `ADD`) may
    /// not. The caller already knows this (`dynamo::
    /// kind_write_is_idempotent`, computed once per write); `poll_probe`
    /// never recomputes it.
    ///
    /// **No production caller as of ADR 0054 step 3.** `kind_write_item_at_
    /// leader` — this function's one production caller — now proposes
    /// `KvCommand::KindEval` via [`cp_kind_eval_local`](Self::
    /// cp_kind_eval_local) instead, so the client's own `condition` is
    /// evaluated fresh at apply rather than carried here as a byte-level
    /// `conditions` seatbelt. This function stays for its own regression
    /// coverage (`confirm_futility_tests::
    /// a_condition_failed_kind_batch_fails_fast_with_a_retryable_error`,
    /// issue #268 — the confirm-futility fast-fail on a raw `KindBatch`,
    /// independent of ADR 0054) until step 4 deletes `rmw_lock`/the
    /// `KindBatch.conditions` seatbelt outright and this function along
    /// with them. The `cfg_attr` mirrors `local_scan_kind_bounded`'s own
    /// precise, not blanket, dead-code allowance: only the non-`cfg(test)`
    /// build (the `tests/` binaries and the release lib) sees it — `cargo
    /// test -p animusd --lib`, which actually calls this, sees no
    /// allowance at all.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn cp_kind_local(
        leader: &CpGroup<E>,
        writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
        change_log: Vec<(Vec<u8>, Vec<u8>)>,
        conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        identity: ProbeIdentity,
    ) -> Result<(), String> {
        let probe = writes
            .iter()
            .find(|(kind, _, v)| *kind == animus_cp_data::KIND_BASE && v.is_some())
            .map(|(_, k, v)| (k.clone(), v.clone().expect("filtered to Some")));
        let Some((probe_key, probe_val)) = probe else {
            return Err("a kind batch must carry a base-kind write to confirm on".into());
        };
        decide::frozen_refusal(leader.is_frozen())?;
        // Pre-propose range check, the same reasoning as `cp_batch_propose`:
        // a fenced-out entry applies as a no-op, and the probe below would then
        // just time out with a generic error instead of a clean routing error.
        let fence = leader.scope_range();
        for (_, key, _) in &writes {
            if !fence.contains(key) {
                return Err("kind write outside this group's live range; retry".into());
            }
        }
        let (accepted_index, accepted_term) =
            match leader.put_kind_batch_conditioned(writes, change_log, conditions) {
                ProposeResult::Accepted { index, term } => (index, term),
                other => return Err(format!("kind write not accepted: {other:?}")),
            };
        let deadline = leader.env().now().saturating_add(CLIENT_TIMEOUT);
        match Self::poll_probe(
            leader,
            accepted_index,
            accepted_term,
            &probe_key,
            &probe_val,
            identity,
            deadline,
        )
        .await
        {
            ProbeWait::Confirmed => Ok(()),
            // A failed own-key `conditions` entry lands here too: the entry
            // applies as a silent no-op (see `KindBatch.conditions`' doc in
            // `animus-cp-data`), so "superseded" is the caller's cue to
            // re-read and re-evaluate — the ordinary OCC retry round.
            ProbeWait::Superseded => Err(
                "CP kind write superseded before its effect appeared (leadership churn, an \
                 apply-time no-op, or a failed write condition); retry"
                    .into(),
            ),
            ProbeWait::TimedOut => Err("CP kind write did not commit in time".into()),
        }
    }

    /// Propose a self-contained **evaluated write** (ADR 0054 step 3 —
    /// `KvCommand::KindEval`) and confirm it —
    /// [`cp_kind_local`](Self::cp_kind_local)'s sibling for the cut-over
    /// single-item write path. `dynamo::kind_write_item_at_leader` is the
    /// one caller: it builds `schema`/`op`/`condition` from the client's
    /// request and this node's own `Metadata` read, then hands the write
    /// to apply rather than computing its own diff (the ADR 0054 fix).
    ///
    /// **Reuses `poll_probe`'s term-checked confirm channel**
    /// (`classify_kind_batch_outcome`, the same identity-checked
    /// `(index, term)` proof `cp_kind_local`'s own confirm uses) but
    /// **not** its value-equality fallback: apply computes the written
    /// bytes itself, so there is no leader-known `probe_val` to compare
    /// against here — the confirm question is always "what did MY entry's
    /// outcome say", never "does the key already hold bytes I predicted."
    /// `NoOp` is read back into the specific `KindBatchOutcome` variant
    /// (`ConditionFailed`/`Sealed`/`Rejected`) to decide the caller-visible
    /// result — see [`kind_eval_noop`](Self::kind_eval_noop)'s own doc for
    /// each mapping and why `ConditionFailed` and `Sealed` diverge (one is
    /// definitive, one asks for a re-route).
    ///
    /// `base_key` is the item's own base-kind key
    /// (`dynamo::item_key(pk, sk)`) — the caller already has it; recomputing
    /// it here from a moved `pk`/`sk` would need borrowing both twice for
    /// no benefit, since `RaftKvNode::propose_kind_eval` derives the
    /// identical key internally from its own `pk`/`sk` arguments anyway
    /// (this function's `base_key` is used only for the pre-propose range
    /// check below, mirroring `cp_kind_local`'s own tripwire).
    #[allow(clippy::too_many_arguments)] // one item write's full identity, mirrors cp_kind_local's sibling shape
    pub(crate) async fn cp_kind_eval_local(
        leader: &CpGroup<E>,
        schema: animus_item::WriteSchema,
        pk: AttributeValue,
        sk: Option<AttributeValue>,
        op: KindEvalOp,
        condition: Option<ConditionExpression>,
        ttl_expired: bool,
        base_key: &[u8],
        identity: ProbeIdentity,
    ) -> Result<KindEvalApplied, String> {
        decide::frozen_refusal(leader.is_frozen())?;
        // Pre-propose range check — the identical routing-bug tripwire
        // `cp_kind_local`/`cp_batch_propose` already carry (ADR 0050 Train
        // B rung 7: ranges are immutable now, so this is belt-and-
        // suspenders against a stale `cp_route` resolution, never a fence).
        let fence = leader.scope_range();
        if !fence.contains(base_key) {
            return Err("kind write outside this group's live range; retry".into());
        }
        let (accepted_index, accepted_term) =
            match leader.propose_kind_eval(schema, pk, sk, op, condition, ttl_expired) {
                ProposeResult::Accepted { index, term } => (index, term),
                other => return Err(format!("kind eval not accepted: {other:?}")),
            };
        let deadline = leader.env().now().saturating_add(CLIENT_TIMEOUT);
        loop {
            let effects_readable = leader.engine_applied_index() >= accepted_index;
            let outcome = leader.kind_batch_outcome(accepted_index);
            match classify_kind_batch_outcome(outcome.clone(), accepted_term, effects_readable) {
                KindBatchSignal::Confirm => {
                    return Self::kind_eval_confirmed(
                        leader,
                        accepted_index,
                        accepted_term,
                        base_key,
                        identity,
                    )
                    .await;
                }
                KindBatchSignal::NoOp => return Self::kind_eval_noop(outcome),
                KindBatchSignal::Inconclusive => {}
            }
            if decide::confirm_wait_is_futile(
                leader.engine_applied_index(),
                leader.is_leader(),
                accepted_index,
            ) {
                // Close the probe-vs-apply race exactly like `poll_probe`
                // does: re-check once more before giving up.
                let outcome = leader.kind_batch_outcome(accepted_index);
                let effects_readable = leader.engine_applied_index() >= accepted_index;
                match classify_kind_batch_outcome(outcome.clone(), accepted_term, effects_readable)
                {
                    KindBatchSignal::Confirm => {
                        return Self::kind_eval_confirmed(
                            leader,
                            accepted_index,
                            accepted_term,
                            base_key,
                            identity,
                        )
                        .await;
                    }
                    KindBatchSignal::NoOp => return Self::kind_eval_noop(outcome),
                    KindBatchSignal::Inconclusive => {}
                }
                return Err(
                    "CP kind write superseded before its effect appeared (leadership churn or \
                     an apply-time no-op); retry"
                        .into(),
                );
            }
            if leader.env().now() >= deadline {
                return Err(KIND_EVAL_CONFIRM_AMBIGUOUS.into());
            }
            leader.env().sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// [`cp_kind_eval_local`](Self::cp_kind_eval_local)'s `Confirm` arm:
    /// this proposer's own `(index, term)` entry provably applied. Reads
    /// back the leader-local `old`/`new` payload
    /// (`RaftKvNode::take_kind_eval_result`, ADR 0054 mechanism 3) — `Some`
    /// on the ordinary path (this same node proposed and is still the one
    /// polling), `None` only in the documented residual: the payload map's
    /// bounded retention aged the slot out from under an unusually slow
    /// confirm loop, or (impossible in practice given `propose_kind_eval`'s
    /// own core-lock-held registration, but not provable never to regress)
    /// this node's registration never landed.
    ///
    /// A lost payload for an **idempotent** write (`ProbeIdentity::
    /// ValueProves` — Put/Delete/SET/REMOVE/a set union or difference) is
    /// recoverable: any entry that landed these exact bytes is a
    /// legitimate success no matter whose, so a plain re-read of the
    /// CURRENT committed value stands in for `new` (the pre-image is
    /// genuinely gone, and is never guessed at as `old` — a caller that
    /// asked for it gets `None`, not a wrong answer). A lost payload for a
    /// **non-idempotent** write (`RequiresOwnEntry` — a numeric `ADD`) is
    /// NOT recoverable this way: the current value could already reflect a
    /// later write, and reporting it as this entry's own `new` would be a
    /// silent lie about which write produced it — so this returns the same
    /// ambiguous, non-retried error [`cp_kind_local`](Self::cp_kind_local)
    /// already returns for its own unconfirmable case
    /// ([`KIND_EVAL_CONFIRM_AMBIGUOUS`]), never a new one. Either way the
    /// write **did** apply — this is a reporting gap, not a correctness
    /// one; see ADR 0054's own "Not removed, deliberately: Ambiguity"
    /// consequence for why the ADR accepts this rather than inventing a
    /// speculative recovery.
    async fn kind_eval_confirmed(
        leader: &CpGroup<E>,
        index: u64,
        term: u64,
        base_key: &[u8],
        identity: ProbeIdentity,
    ) -> Result<KindEvalApplied, String> {
        match leader.take_kind_eval_result(index, term) {
            Some(result) => Ok(KindEvalApplied::Ok {
                old: result.old,
                new: result.new,
            }),
            None if identity == ProbeIdentity::ValueProves => {
                let new = leader.local_get(base_key).await.and_then(|bytes| {
                    animus_dynamo::wire::decode_stored_item(&bytes)
                        .ok()
                        .flatten()
                });
                Ok(KindEvalApplied::Ok { old: None, new })
            }
            None => Err(KIND_EVAL_CONFIRM_AMBIGUOUS.into()),
        }
    }

    /// [`cp_kind_eval_local`](Self::cp_kind_eval_local)'s `NoOp` arm:
    /// nothing landed at this index, so read back which of the three
    /// no-op-shaped `KindBatchOutcome` variants it was.
    ///
    /// - `ConditionFailed` → `Ok(KindEvalApplied::ConditionFailed)`, a
    ///   DEFINITIVE result (never retried here) — the client's own
    ///   condition genuinely evaluated false against apply's fresh read.
    ///   (This is also where an unresolved foreign transaction intent on
    ///   this key lands, per apply's own documented foreign-intent
    ///   discipline — an accepted, rare imprecision; see this function's
    ///   own caller, `dynamo::kind_write_item_at_leader`, for the full
    ///   account of why that is not disambiguated here.)
    /// - `Rejected { code, message }` → `Ok(KindEvalApplied::Rejected)`,
    ///   also definitive — a domain violation in the condition or a
    ///   malformed/oversized update, copied verbatim.
    /// - `Sealed` → `Err(..; retry)` — the tablet mid-split-cutover froze
    ///   this write; unlike the two above, this is NOT a decision about
    ///   the write's own content, so the caller must re-route (via
    ///   `cp_kind_write_item`'s own retry loop) rather than treat it as a
    ///   terminal answer, mirroring `cp_kind_local`'s identical `Sealed`
    ///   handling one layer up (folded into its own generic `Superseded`
    ///   message there, since that path never distinguishes the three).
    /// - `Applied`/`None` never reach here — `classify_kind_batch_outcome`
    ///   only ever returns `NoOp` for one of the three variants above.
    fn kind_eval_noop(outcome: Option<(u64, KindBatchOutcome)>) -> Result<KindEvalApplied, String> {
        match outcome.map(|(_, o)| o) {
            Some(KindBatchOutcome::ConditionFailed { .. }) => Ok(KindEvalApplied::ConditionFailed),
            Some(KindBatchOutcome::Rejected { code, message, .. }) => {
                Ok(KindEvalApplied::Rejected { code, message })
            }
            Some(KindBatchOutcome::Sealed { .. }) => Err(
                "CP kind write superseded before its effect appeared (tablet frozen for split \
                 cutover); retry"
                    .into(),
            ),
            other => unreachable!(
                "classify_kind_batch_outcome only returns NoOp for ConditionFailed/Sealed/\
                 Rejected, got {other:?}"
            ),
        }
    }

    /// Propose a `Batch` on a **known-leader** local handle, returning the probe
    /// `(key, value)` to confirm on success — the batch analog of `put`, split out
    /// from confirmation so a caller can poll for confirmation more than once
    /// without proposing more than once. `Err` means the batch was **never**
    /// accepted anywhere (the leader moved) — a fresh retry is free. `Ok` means it
    /// was appended to the leader's log; the caller must still confirm via
    /// [`poll_probe`] before treating it as durable, and must not call this again
    /// for the same data while a poll is still pending (see
    /// [`ClientCtx::cp_batch_write_patient`]'s doc for why re-proposing an
    /// already-accepted-but-unconfirmed batch is actively harmful).
    ///
    /// **Pre-propose range check (ADR 0028 write fences).** `cp_route` can
    /// resolve `Local` off a stale `Metadata` view during a split's crossover
    /// window (this node still thinks it hosts the leader for a wider range
    /// than the tablet's group has actually narrowed to). Proposing anyway
    /// and relying solely on the *embedded* fence to no-op the entry at apply
    /// time is not enough here: `cp_batch_local`'s confirm loop
    /// ([`poll_probe`]) waits for the **last key's value to read back**, and a
    /// fenced-out batch never writes anything — so the loop just times out
    /// with a generic "did not commit" error rather than a clean routing
    /// error, and (see `cp_put_local`'s doc for the sharper version of this
    /// hazard) a confirm mechanism keyed on a coarser signal than value
    /// equality (e.g. an engine-applied index, which a no-op still advances)
    /// could go further and **falsely ack** a write that never happened. So
    /// every key is checked against the leader's own live
    /// [`RaftKvNode::scope_range`] *before* proposing: on a miss, this
    /// returns `Err` **without proposing**, in the same shape as the
    /// `NotLeader` case below, so `cp_batch_write`/`cp_batch_write_patient`'s
    /// caller sees an ordinary routing failure and retries (re-resolving
    /// `cp_route`, which reaches the correct child once this node's own view
    /// of the split has caught up). The embedded `fence` (stamped from this
    /// same read) still rides the proposed entry regardless, covering the
    /// residual race between this check and the entry's actual apply — see
    /// [`RaftKvNode::scope_range`]'s doc for why that sliver can't be closed
    /// for free; an out-of-range write landing in that sliver is *dropped*
    /// (a safe no-op), never mis-applied, so the residual risk is a
    /// mis-timed error, not silent corruption.
    fn cp_batch_propose(
        leader: &CpGroup<E>,
        group: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Option<(u64, u64, KvPair)>, String> {
        decide::frozen_refusal(leader.is_frozen())?;
        let probe = group.last().cloned();
        let fence = leader.scope_range();
        if let Some((bad_key, _)) = group.iter().find(|(k, _)| !fence.contains(k)) {
            return Err(format!(
                "key {bad_key:?} outside tablet's current range (stale routing, likely a split crossover); retry"
            ));
        }
        match leader.put_batch(group) {
            ProposeResult::Accepted { index, term } => Ok(probe.map(|p| (index, term, p))),
            ProposeResult::NotLeader { .. } => Err("CP group leader moved; retry".into()),
        }
    }

    /// Poll `leader`'s local engine for `probe_key` to reflect `probe_val` until
    /// `deadline` — the durable-before-ack confirm wait shared by every CP write
    /// path (mirrors [`cp_put_local`](Self::cp_put_local)). Ends early, with
    /// [`ProbeWait::Superseded`], once [`decide::confirm_wait_is_futile`]
    /// says the accepted entry's effect can no longer appear.
    ///
    /// `accepted_term` is the term [`ProposeResult::Accepted`] carried
    /// alongside `accepted_index` — see the identity-check note below.
    ///
    /// `identity` gates the value-equality fallback below (issue #469): see
    /// [`ProbeIdentity`]'s doc for the full false-ack hazard it closes.
    async fn poll_probe(
        leader: &CpGroup<E>,
        accepted_index: u64,
        accepted_term: u64,
        probe_key: &[u8],
        probe_val: &[u8],
        identity: ProbeIdentity,
        deadline: Nanos,
    ) -> ProbeWait {
        loop {
            // Ask the entry what it did, in preference to reading the value
            // back. Value equality cannot tell "my entry no-op'd" from "my
            // entry applied and a concurrent write then overwrote it" — the
            // second is a success, and reporting it as a failure made a
            // contended key fail spuriously (measured: ten concurrent
            // `PutItem`s to one key, six "superseded" errors). The outcome is
            // recorded per Raft index at apply time and is identical on every
            // replica.
            // **Both halves are required.** The outcome says whether the entry
            // did anything; `engine_applied_index` says whether its effects are
            // merged and readable. The outcome is recorded as the entry is
            // processed, *before* its writes are flushed into the engine, so
            // acking on it alone would ack a write that is not yet visible —
            // the durable-before-visible rule, and precisely the false-ack
            // hazard `cp_put_local`'s doc warns about. Value equality used to
            // imply both at once; splitting them means saying so explicitly.
            //
            // A no-op needs no such wait: it wrote nothing, so there is
            // nothing to become readable and its outcome is final immediately.
            //
            // **`classify_kind_batch_outcome` additionally requires `term ==
            // accepted_term` before treating `Applied` as a confirm (a
            // false-ack found in review of PR #334's KindBatch apply-time
            // outcome channel).** The outcome map is keyed by Raft log index
            // alone, and an *accepted* (appended-locally) entry is not yet a
            // *committed* one — if this node loses leadership before commit,
            // log-matching truncates the accepted entry and a completely
            // different command can commit and apply at the identical index,
            // recording `Applied` there for *its* content, not ours. Index
            // alone cannot tell "my entry applied" from "a different entry
            // now occupies my old index" — only the pair (index, term) can,
            // by Raft's log-matching property (see `ProposeResult::
            // Accepted`'s doc). A term mismatch is classified `Inconclusive`
            // exactly like `None`: not proof of failure either (the value
            // probe below still confirms if the reoccupying entry's content
            // happens to be identical), just not proof of success. See
            // `kind_batch_signal_tests` for the identity check exercised in
            // isolation.
            let effects_readable = leader.engine_applied_index() >= accepted_index;
            match classify_kind_batch_outcome(
                leader.kind_batch_outcome(accepted_index),
                accepted_term,
                effects_readable,
            ) {
                KindBatchSignal::Confirm => return ProbeWait::Confirmed,
                // The caller's OCC round (re-read, re-evaluate) or a re-route.
                KindBatchSignal::NoOp => return ProbeWait::Superseded,
                // Fall through to the value probe, which is the pre-existing
                // behaviour.
                KindBatchSignal::Inconclusive => {}
            }
            // **Value equality proves visibility, not authorship — sound
            // only when the write is idempotent (issue #469).** This
            // fallback answers "does the key already hold the bytes I
            // proposed?", which is a different question from "did MY entry
            // put them there?" For Put/Delete/SET/REMOVE/a set union or
            // difference the two coincide: any entry landing these exact
            // bytes is a legitimate success, whoever's entry it was. For a
            // numeric `ADD` they do not: `kind_write_item_at_leader` reads
            // `old` under a lock it releases *before* proposing (issue
            // #285), so two concurrent evaluators of the same key can read
            // the identical stale `old`, compute byte-identical `new` (a
            // pure function of `(cur, delta)`), and each propose an entry
            // carrying those same bytes with an own-key OCC seatbelt keyed
            // to that same stale `old`. The first such entry to apply wins
            // the seatbelt and writes the bytes; every later one legitimately
            // `ConditionFailed`s and no-ops — but in the window after the
            // winner's bytes are visible and before *this* entry's own
            // outcome is recorded, value equality already matches, even
            // though this entry never applied. Confirming there acks an
            // increment that never landed. So for a non-idempotent write
            // (`identity == RequiresOwnEntry`) this fallback must not run at
            // all — not even to read the key — and the loop instead keeps
            // polling `classify_kind_batch_outcome` for this entry's own
            // (index, term) until it resolves or the deadline ends the wait
            // in `TimedOut`, never a value guess.
            if identity == ProbeIdentity::ValueProves
                && leader.local_get(probe_key).await.as_deref() == Some(probe_val)
            {
                return ProbeWait::Confirmed;
            }
            if decide::confirm_wait_is_futile(
                leader.engine_applied_index(),
                leader.is_leader(),
                accepted_index,
            ) {
                // Close the probe-vs-apply race before giving up: re-check the
                // outcome first, then the value. `confirm_wait_is_futile` can
                // have returned `true` via its `!is_leader()` clause alone,
                // with `engine_applied_index()` still behind `accepted_index`
                // — so `effects_readable` must be recomputed here, not
                // assumed `true` from the fact that we're in this branch.
                if classify_kind_batch_outcome(
                    leader.kind_batch_outcome(accepted_index),
                    accepted_term,
                    leader.engine_applied_index() >= accepted_index,
                ) == KindBatchSignal::Confirm
                {
                    return ProbeWait::Confirmed;
                }
                // Same idempotency gate as the fallback above — a
                // non-idempotent write must not consult the value here
                // either, for the identical reason: a different,
                // concurrently-committed entry's bytes prove nothing about
                // whether THIS entry (about to be judged futile) applied.
                if identity == ProbeIdentity::ValueProves
                    && leader.local_get(probe_key).await.as_deref() == Some(probe_val)
                {
                    return ProbeWait::Confirmed;
                }
                return ProbeWait::Superseded;
            }
            if leader.env().now() >= deadline {
                return ProbeWait::TimedOut;
            }
            leader.env().sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Propose a `Batch` on a **known-leader** local handle and wait until it is
    /// committed + durable + applied — durable-before-ack. The whole batch is one
    /// Raft entry, so confirming the **last** key reflects our value on the leader's
    /// local engine means the entry committed + applied and the whole batch is
    /// durable (the leader applies only after a quorum commit + WAL fsync, as in
    /// [`cp_put_local`](Self::cp_put_local); a per-batch quorum barrier would not
    /// scale under load).
    ///
    /// Confirms with [`ProbeIdentity::ValueProves`] (issue #469): this
    /// primitive is the raw `Batch` command, which only ever carries plain
    /// key/value puts — there is no `ADD`-shaped write at this layer — so
    /// every write it can possibly confirm is structurally idempotent, and
    /// the value-equality fallback is always sound here.
    pub(crate) async fn cp_batch_local(
        leader: &CpGroup<E>,
        group: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), String> {
        let Some((accepted_index, accepted_term, (probe_key, probe_val))) =
            Self::cp_batch_propose(leader, group)?
        else {
            return Ok(());
        };
        let deadline = leader.env().now().saturating_add(CLIENT_TIMEOUT);
        match Self::poll_probe(
            leader,
            accepted_index,
            accepted_term,
            &probe_key,
            &probe_val,
            ProbeIdentity::ValueProves,
            deadline,
        )
        .await
        {
            ProbeWait::Confirmed => Ok(()),
            ProbeWait::Superseded => Err(
                "CP batch write superseded before its effect appeared (leadership churn or an \
                 apply-time no-op); retry"
                    .into(),
            ),
            ProbeWait::TimedOut => Err("CP batch write did not commit in time".into()),
        }
    }

    /// Propose a CP write on a **known-leader** local handle and wait until it is
    /// committed + durable + applied before returning — durable-before-ack.
    ///
    /// We confirm via a **local** read on the leader, not a linearizable ReadIndex
    /// barrier: the leader applies an entry only after a quorum commit + WAL fsync
    /// (durable-before-visible in `animus-cp-data`), so the leader's local read
    /// reflecting our value means it is durable. A per-write quorum barrier would
    /// not scale under concurrent load. (If we lose leadership before commit, the
    /// entry may be truncated and never appear locally — the confirm loop then
    /// ends early via [`decide::confirm_wait_is_futile`]
    /// with a retryable error rather than polling out the whole
    /// [`CLIENT_TIMEOUT`]: the write did not confirm, and the caller's retry
    /// re-resolves routing.)
    ///
    /// **Pre-propose range check (ADR 0028 write fences).** `cp_route` can hand
    /// us a `Local` leader off a stale `Metadata` view during a split's
    /// crossover window — this node still believes it hosts the leader for a
    /// range wider than the tablet's group has actually narrowed to (e.g. this
    /// key now belongs to a just-minted sibling on the same shared engine).
    /// Stamping the leader's own `fence` on the proposed entry (below) is
    /// necessary but **not sufficient on its own**: a fenced-out entry still
    /// commits and applies as a no-op, and *if* a confirm mechanism ever keyed
    /// success on a coarser signal than exact value equality (e.g. "has this
    /// index applied yet" — a no-op still advances that watermark) it would
    /// **falsely ack** a write that never actually landed anywhere. This confirm
    /// loop polls value equality (success is never keyed on the coarser
    /// applied-index signal — [`decide::confirm_wait_is_futile`]
    /// only ever ends a wait *early with an error*),
    /// which degrades that hazard to "returns a retryable error" rather than a
    /// false ack — but that is a property of *this* poll, not a defense to rely on, so the
    /// explicit pre-check below is the actual guard: reject an out-of-range key
    /// **before proposing at all**, in the same `Err` shape as the `NotLeader`
    /// case, so the caller (`cp_write`) sees an ordinary routing failure and its
    /// own retry re-resolves `cp_route` (reaching the correct child once this
    /// node's view of the split has caught up), instead of a write that silently
    /// shadowed/corrupted the child's data. The embedded `fence` (stamped from
    /// the *same* `scope_range()` read used for the check) still rides the
    /// entry regardless, to cover the residual race between this check and the
    /// entry's actual apply (the scope can narrow further in between) — see
    /// [`RaftKvNode::scope_range`]'s doc for why that sliver isn't free to
    /// close; a write landing in it is *dropped* (a safe no-op that this loop
    /// times out on), never mis-applied.
    pub(crate) async fn cp_put_local(
        leader: &CpGroup<E>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), String> {
        decide::frozen_refusal(leader.is_frozen())?;
        let fence = leader.scope_range();
        if !fence.contains(&key) {
            return Err(
                "key outside tablet's current range (stale routing, likely a split crossover); retry".into(),
            );
        }
        match leader.put(key.clone(), value.clone()) {
            ProposeResult::Accepted { index, .. } => {
                let deadline = leader.env().now().saturating_add(CLIENT_TIMEOUT);
                let mut poll = CP_CONFIRM_POLL_INIT;
                loop {
                    if leader.local_get(&key).await.as_deref() == Some(value.as_slice()) {
                        return Ok(());
                    }
                    if decide::confirm_wait_is_futile(
                        leader.engine_applied_index(),
                        leader.is_leader(),
                        index,
                    ) {
                        // Close the probe-vs-apply race before giving up.
                        if leader.local_get(&key).await.as_deref() == Some(value.as_slice()) {
                            return Ok(());
                        }
                        return Err(
                            "CP write superseded before its effect appeared (leadership churn \
                             or an apply-time no-op); retry"
                                .into(),
                        );
                    }
                    if leader.env().now() >= deadline {
                        return Err("CP write did not commit in time".into());
                    }
                    leader.env().sleep(poll).await;
                    poll = (poll * 2).min(CP_CONFIRM_POLL_MAX);
                }
            }
            ProposeResult::NotLeader { .. } => Err("CP group leader moved; retry".into()),
        }
    }

    /// Propose a CP delete on a **known-leader** local handle and wait until the
    /// key reads absent locally (committed + durable + applied tombstone) —
    /// durable-before-ack. Local read, not a barrier, as in
    /// [`cp_put_local`](Self::cp_put_local) — and the **same pre-propose range
    /// check** against the leader's live `scope_range()` before proposing, for
    /// the same reason: a stale-routed delete for a key that now belongs to a
    /// split sibling must not be silently accepted as a fenced-out no-op (which
    /// would otherwise leave the sibling's real value untouched but let the
    /// caller believe the delete succeeded once the parent's own read of that
    /// physical key coincidentally reads absent — see `cp_put_local`'s doc for
    /// the full hazard and why the pre-check, not just the embedded fence, is
    /// the actual guard).
    pub(crate) async fn cp_delete_local(leader: &CpGroup<E>, key: Vec<u8>) -> Result<(), String> {
        decide::frozen_refusal(leader.is_frozen())?;
        let fence = leader.scope_range();
        if !fence.contains(&key) {
            return Err(
                "key outside tablet's current range (stale routing, likely a split crossover); retry".into(),
            );
        }
        match leader.delete(key.clone()) {
            ProposeResult::Accepted { index, .. } => {
                let deadline = leader.env().now().saturating_add(CLIENT_TIMEOUT);
                let mut poll = CP_CONFIRM_POLL_INIT;
                loop {
                    if leader.local_get(&key).await.is_none() {
                        return Ok(());
                    }
                    if decide::confirm_wait_is_futile(
                        leader.engine_applied_index(),
                        leader.is_leader(),
                        index,
                    ) {
                        // Close the probe-vs-apply race before giving up.
                        if leader.local_get(&key).await.is_none() {
                            return Ok(());
                        }
                        return Err(
                            "CP delete superseded before its effect appeared (leadership churn \
                             or an apply-time no-op); retry"
                                .into(),
                        );
                    }
                    if leader.env().now() >= deadline {
                        return Err("CP delete did not commit in time".into());
                    }
                    leader.env().sleep(poll).await;
                    poll = (poll * 2).min(CP_CONFIRM_POLL_MAX);
                }
            }
            ProposeResult::NotLeader { .. } => Err("CP group leader moved; retry".into()),
        }
    }
}

/// Regression for issue #469: a lost-acked-write via `poll_probe`'s
/// value-equality fallback. `poll_probe` is private to this module (no
/// `pub(crate)`) — this lives as a child module here, not in `lib.rs` or an
/// external `tests/*.rs` file, specifically so it can call `poll_probe`
/// directly rather than only its `cp_kind_local`/`cp_batch_local` wrappers,
/// which is what lets this test pin the exact accepted-but-unapplied window
/// the bug lives in (see `poll_probe`'s own doc for the mechanism). Mirrors
/// `lib.rs`'s `simenv_client_ctx_tests` harness style (a single-voter
/// `RaftKvNode<SimEnv, MemoryEngine>`, no sockets, no `ProdEnv`) — see that
/// module's doc for why constructing these types here widens nothing.
///
/// **The scenario.** Entry A is an ordinary unconditioned `KindBatch` that
/// writes `(key, value)` and is allowed to fully commit + apply. Entry B
/// proposes the exact SAME `(key, value)` bytes but carries an own-key OCC
/// seatbelt engineered to legitimately fail once entry B itself applies (a
/// guard key with an expected value that can never match) — the same shape
/// a concurrent numeric `ADD` evaluator produces from a stale read (issue
/// #469's own account: two evaluators reading identical stale `old` under
/// `kind_write_item_at_leader`'s released-before-propose lock compute
/// byte-identical `new`). With entry B accepted but not yet applied,
/// `poll_probe` is driven directly for entry B's own `(index, term)`. The
/// pre-fix code's ungated value-equality fallback matches entry A's already-
/// visible bytes at the very first poll and falsely reports `Confirmed` —
/// before entry B has had any chance to apply and legitimately
/// `ConditionFailed`. The fix (`ProbeIdentity::RequiresOwnEntry`) never
/// consults the value at all for a non-idempotent write, so the wait
/// continues until entry B's own outcome resolves — `Superseded`, never a
/// false `Confirmed`.
#[cfg(test)]
mod poll_probe_identity_tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use animus_cp_data::{KIND_BASE, RaftKvNode};
    use animus_env::{Clock, EnvExt, nid};
    use animus_sim::{SimEnv, Simulator};
    use animus_storage::MemoryEngine;

    use super::*;

    #[derive(Clone, Copy, Debug, Default)]
    struct NeverRelay;

    #[async_trait::async_trait]
    impl RelayClient for NeverRelay {
        async fn relay(
            &self,
            addr: String,
            _request: &ClientRequest,
            _timeout: Duration,
        ) -> ClientResponse {
            ClientResponse::Error(format!(
                "NeverRelay: this single-node harness never relays (addr={addr})"
            ))
        }
    }

    /// A one-voter `RaftKvNode<SimEnv, MemoryEngine>` wrapped as a
    /// `CpGroup<SimEnv>` — no control plane, no schema catalog: `poll_probe`
    /// needs only a leader handle, so this harness skips everything
    /// `simenv_client_ctx_tests`' fuller fixture sets up for a real
    /// `ClientCtx` write/read round trip.
    fn single_voter_group(seed: u64) -> (Simulator, CpGroup<SimEnv>) {
        let sim = Simulator::new(seed);
        let kv: RaftKvNode<SimEnv, MemoryEngine> =
            RaftKvNode::start(sim.env(nid(0)), vec![nid(0)], MemoryEngine::new());
        (sim, CpGroup::Mem(kv))
    }

    /// Spawn `fut` on `env` and drive `sim` until it resolves — the
    /// harness's own converged-or-timeout idiom (root `CLAUDE.md`'s
    /// "Testing" rule), mirroring `simenv_client_ctx_tests::
    /// spawn_and_capture`.
    fn spawn_and_capture<T, F>(sim: &mut Simulator, env: &SimEnv, fut: F) -> Option<T>
    where
        T: Send + 'static,
        F: std::future::Future<Output = T> + Send + 'static,
    {
        let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
        let out = slot.clone();
        env.spawn_task(async move {
            let result = fut.await;
            *out.lock().expect("result slot poisoned") = Some(result);
        });
        sim.run_for(Duration::from_secs(2));
        slot.lock().expect("result slot poisoned").take()
    }

    #[test]
    fn a_non_idempotent_probe_never_confirms_on_a_different_entrys_identical_bytes() {
        run(0x0469_0001);
    }

    #[test]
    fn a_non_idempotent_probe_never_confirms_on_a_different_entrys_identical_bytes_seed2() {
        run(0x0469_0002);
    }

    /// Replay proof (repo convention): `ANIMUS_SEED=<seed> cargo test -p
    /// animusd --lib replays_issue_469_from_an_explicit_env_seed` reruns
    /// this exact scenario from a printed seed.
    #[test]
    fn replays_issue_469_from_an_explicit_env_seed() {
        let seed = std::env::var("ANIMUS_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0x0469_0003);
        run(seed);
    }

    fn run(seed: u64) {
        let (mut sim, leader) = single_voter_group(seed);
        sim.run_for(Duration::from_millis(500));
        assert!(
            leader.is_leader(),
            "the sole voter must be its own leader (seed={seed})"
        );

        let key = b"item".to_vec();
        let value = b"same-bytes-both-entries-propose".to_vec();

        // Entry A: an ordinary unconditioned put, allowed to fully commit
        // and apply before entry B is ever proposed.
        let (a_index, _a_term) = match leader.put_kind_batch_conditioned(
            vec![(KIND_BASE, key.clone(), Some(value.clone()))],
            Vec::new(),
            Vec::new(),
        ) {
            ProposeResult::Accepted { index, term } => (index, term),
            other => panic!("entry A not accepted (seed={seed}): {other:?}"),
        };
        sim.run_for(Duration::from_millis(500));
        assert!(
            leader.engine_applied_index() >= a_index,
            "entry A must apply before entry B is proposed (seed={seed})"
        );
        assert_eq!(
            futures::executor::block_on(leader.local_get(&key)),
            Some(value.clone()),
            "entry A's write must be visible before entry B is proposed \
             (seed={seed})"
        );

        // Entry B: proposes the exact SAME (key, value) bytes, guarded by an
        // own-key OCC seatbelt that can never hold (the guard key is never
        // written) — engineered to legitimately `ConditionFailed` once
        // entry B itself applies. This is the concurrent-`ADD`-evaluator
        // shape from issue #469's own account, reproduced directly rather
        // than through a real race: both entries propose byte-identical
        // `new` bytes, and only one of them may legitimately win.
        //
        // **Entry B's own propose lives INSIDE the spawned task, as the
        // first thing its future does — not before spawning it.** This is
        // load-bearing, not a style choice: `SimEnv` is single-threaded
        // cooperative (root `CLAUDE.md`), so a task only yields the
        // scheduler at an `.await` that actually returns `Pending`. Entry B
        // does not exist as a Raft entry until this future's first poll
        // proposes it, so the apply-drain loop has nothing to race ahead
        // of; proposing it and immediately awaiting `poll_probe` in the
        // same poll means the very first thing this task's future does is
        // observe entry B accepted-but-unapplied (asserted below) — no
        // other task, in particular the apply loop, gets a turn in
        // between. Proposing B before spawning (an earlier draft of this
        // test did exactly that) let the already-running apply loop drain
        // and record entry B's outcome during the *next* `sim.run_for`
        // before `poll_probe`'s own task ever got its first poll — closing
        // the very window this test exists to pin, and the pre-fix code
        // then genuinely never got a chance to consult the stale value
        // (silently passing for the wrong reason).
        let guard_key = b"guard-never-written".to_vec();
        let deadline = leader.env().now().saturating_add(Duration::from_secs(5));
        let env = leader.env().clone();
        let sanity_unapplied_at_probe_start: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));
        let result = spawn_and_capture(&mut sim, &env, {
            let leader = leader.clone();
            let key = key.clone();
            let value = value.clone();
            let sanity = sanity_unapplied_at_probe_start.clone();
            async move {
                let (b_index, b_term) = match leader.put_kind_batch_conditioned(
                    vec![(KIND_BASE, key.clone(), Some(value.clone()))],
                    Vec::new(),
                    vec![(guard_key, Some(b"can-never-match".to_vec()))],
                ) {
                    ProposeResult::Accepted { index, term } => (index, term),
                    other => panic!("entry B not accepted: {other:?}"),
                };
                *sanity.lock().expect("sanity slot poisoned") =
                    Some(leader.kind_batch_outcome(b_index).is_none());
                ClientCtx::<SimEnv, NeverRelay>::poll_probe(
                    &leader,
                    b_index,
                    b_term,
                    &key,
                    &value,
                    ProbeIdentity::RequiresOwnEntry,
                    deadline,
                )
                .await
            }
        });

        assert_eq!(
            sanity_unapplied_at_probe_start
                .lock()
                .expect("sanity slot poisoned")
                .take(),
            Some(true),
            "entry B must have no recorded outcome at the instant its own \
             confirm-poll begins, or this run doesn't exercise the \
             accepted-but-unapplied window this test exists to pin \
             (seed={seed})"
        );

        assert_ne!(
            result,
            Some(ProbeWait::Confirmed),
            "a non-idempotent write's confirm-poll must never treat a \
             DIFFERENT, already-applied entry's identical bytes as proof \
             THIS entry applied (seed={seed}): {result:?}"
        );
        // Not just "not Confirmed" — the scenario is engineered to resolve
        // cleanly once entry B's own outcome is recorded (`ConditionFailed`
        // -> `NoOp` -> `Superseded`), proving the fixed loop actually made
        // progress rather than degrading to a `TimedOut` guess.
        assert_eq!(
            result,
            Some(ProbeWait::Superseded),
            "entry B's own ConditionFailed outcome should resolve the wait \
             once applied (seed={seed}): {result:?}"
        );
    }
}
