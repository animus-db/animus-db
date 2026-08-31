//! ClientCtx's schema/catalog-provisioning cluster (ADR 0061 rung C5 step 2):
//! schema DDL proposals, tablet provisioning/serveability wait, node
//! registration, table/tablet drop, split trigger, force-seal, stream growth
//! and backfill-cursor clearing. Moved verbatim out of `lib.rs`'s
//! `impl<E: Env> ClientCtx<E>` blocks -- no logic changes.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_env::{Env, Metric, NodeId};
use animus_node::control_handle::ControlHandle;
use animus_node::host::RelayClient;
use animus_tablet::{KeyRange, TabletId, TabletState};

use crate::ReadConsistency;
use crate::{
    CLIENT_TIMEOUT, ClientCtx, ClientRequest, ClientResponse, CpRoute, MAX_REPLICATION_FACTOR,
    MetaCommand, NodeAddrs, NodeStatus, PlacementPolicy, ProposeResult, RegisterOutcome,
    SCHEMA_COMMIT_TIMEOUT, SCHEMA_POLL_INTERVAL, SCHEMA_PROPOSE_PATIENCE,
    SPLIT_KEY_NOT_TOKEN_VIABLE, STREAM_GROW_MID_SPLIT, STREAM_GROW_NO_SPLIT_POINT, SplitMode,
    WATCH_METADATA_SERVER_TIMEOUT, decide, index_drain, median_split_key, split_child_placement,
    topology,
};

impl<E: Env, R: RelayClient> ClientCtx<E, R> {
    /// Serve a long-poll [`ClientRequest::WatchMetadata`] (ADR 0035 PR5 for
    /// the long-poll mechanism itself; ADR 0038 PR5 for the incremental
    /// reply shape below): park on this node's own
    /// [`ControlHandle::metadata_watch`] for up to
    /// [`WATCH_METADATA_SERVER_TIMEOUT`], then reply — either because the
    /// watch genuinely advanced past `last_seen`, or because the bound
    /// elapsed with nothing new (a normal outcome, not an error; the caller
    /// just retries with the same `last_seen`, exactly like a `Status` poll
    /// that happened not to observe a change).
    ///
    /// Only a genuine control-group replica (`ControlHandle::Local`) can
    /// serve this. A `Remote` data-only node **rejects** it instead of
    /// degrading: its own `ControlHandle::metadata_watch()` is itself driven
    /// by replies to *this exact request* (see
    /// [`control_handle::RemoteControlClient`]'s doc), so serving it here
    /// would only let a misdirected watch (e.g. a stale `client_route` entry
    /// pointing at a data node instead of a control node) degrade silently to
    /// an effective ~[`WATCH_METADATA_SERVER_TIMEOUT`]-second poll — worse
    /// than the pre-PR5 fixed-interval poll, not better. Rejecting fails the
    /// misdirected watch fast instead.
    ///
    /// **Incremental reply (ADR 0038 PR5)**: once the watch resolves, try
    /// this node's own [`RaftNode::watch_delta_since`] first — if its bounded
    /// delta ring covers `(last_seen, watermark]`, reply with
    /// [`ClientResponse::MetadataDelta`] instead of a full [`ClientResponse::
    /// Status`] clone. Falls back to the full reply whenever the ring
    /// doesn't cover the range (a fresh/lagging/just-recovered replica, or a
    /// caller whose `last_seen` aged out of the window) — the log-tail vs
    /// `InstallSnapshot` fallback shape this plane already has. **Also**
    /// falls back while the ADR 0030 growth-node mirror overlay is active on
    /// this node (`self.remote_metadata` populated): that overlay serves
    /// `effective_metadata()` from a *different* source than this node's own
    /// (on a growth node, permanently inert) local ring, so a delta off that
    /// ring would answer the wrong question.
    pub(crate) async fn watch_metadata(&self, last_seen: u64) -> ClientResponse {
        let ControlHandle::Local(raft) = &self.control else {
            return ClientResponse::Error(
                "this node has no local control-plane watch to serve (ADR 0035 data-only node); \
                 watch a control-plane node instead"
                    .into(),
            );
        };
        let watch = raft.metadata_watch();
        // Race the watch against the server-side timeout (no `Env`
        // equivalent to `tokio::select!` — `animus-cp-data::
        // cluster_segment_store` uses the identical `futures::future::
        // select` shape for its own relay-correlation race). Both arms are
        // `Unpin` (`MetadataChanged` is a plain, non-self-referential
        // struct; `env.sleep` is `async_trait`-boxed), so no `pin_mut!` is
        // needed. Whichever resolves first is discarded either way — this
        // preserves the exact "wait for a change or WATCH_METADATA_SERVER_
        // TIMEOUT, whichever comes first" semantics `tokio::select!` gave.
        let _ = futures::future::select(
            watch.changed(last_seen),
            self.env.sleep(WATCH_METADATA_SERVER_TIMEOUT),
        )
        .await;
        let leader_hint = self.control_leader_hint();
        // Intra-cluster dual (ADR 0047) — the same `self.control.leader()`
        // id, resolved through `intra_addr` instead of `route_addr`. This is
        // the field `remote_metadata_watch_loop`'s own dial candidates read
        // (via `RemoteControlClient::intra_leader_addr_hint`), never the
        // human-facing `leader_hint` above.
        let intra_leader_hint = self.intra_control_leader_hint();
        let control_voters = self.control.config().unwrap_or_default();
        let growth_mirror_active = self
            .remote_metadata
            .lock()
            .expect("remote metadata poisoned")
            .is_some();
        if !growth_mirror_active && let Some(reply) = raft.watch_delta_since(last_seen) {
            return ClientResponse::MetadataDelta {
                writes: reply.writes,
                watermark: reply.watermark,
                leader_hint,
                intra_leader_hint,
                control_voters,
            };
        }
        ClientResponse::Status {
            metadata: self.effective_metadata(),
            leader_hint,
            intra_leader_hint,
            watermark: watch.latest(),
            control_voters,
        }
    }

    /// Propose a **schema-catalog** `command` toward the control-plane leader
    /// (v1 Phase 1 / A2): propose locally if this node is the control leader, else
    /// relay [`ClientRequest::ProposeSchema`] to the leader's node. Best-effort per
    /// call — the caller polls its replicated `Metadata` for the commit and
    /// re-invokes, so a transient relay failure is retried with a re-resolved
    /// leader. The result replicates to every node via Raft.
    ///
    /// Returns whether this call has reason to believe `command` reached *some*
    /// leader's Raft log (a local `Accepted`, or a relay that didn't visibly
    /// fail) — `false` only when nothing was sent anywhere (no leader
    /// known/reachable, or a local propose lost a leadership race).
    /// [`propose_and_await`](Self::propose_and_await) uses this to decide
    /// whether to back off before resubmitting: re-proposing an
    /// already-in-flight command on every poll tick just appends a duplicate
    /// log entry (harmless to apply for an idempotent command like
    /// `SplitTablet` — its `new_id` guard rejects the duplicate — but still
    /// wasted WAL/replication work, worse under exactly the load/latency that
    /// caused the wait in the first place). Same shape as the already-fixed
    /// `cp_batch_write_patient`/`propose_and_confirm_split` retry-amplification
    /// bugs, applied to the schema-proposal path.
    pub(crate) async fn propose_schema(&self, command: &MetaCommand) -> bool {
        if let Some(leader) = self.edge.leader_handle() {
            return matches!(
                leader.propose(command.clone()),
                ProposeResult::Accepted { .. }
            );
        }
        // Prefer the control handle's own **intra** leader-address hint (ADR
        // 0047; ADR 0035 PR4's original `leader_addr_hint` populated directly
        // from `Status` replies for a `Remote` data node) over an
        // `intra_addr` lookup — the hint is strictly fresher for a data-only
        // node, since it rides the very `Status` reply that filled the
        // mirror, whereas `intra_addr` needs this leader's address to have
        // separately synced into the replicated node-address book. This is a
        // machine-to-machine relay, so it uses the intra hint/route, never
        // the human-facing `leader_addr_hint`/`route_addr` (see the root
        // `CLAUDE.md`'s hint-field-conflation lesson). A no-op for `Local`
        // (always `None`).
        if let Some(addr) = self.control.intra_leader_addr_hint() {
            return !matches!(
                self.relay(addr, ClientRequest::ProposeSchema(command.clone()))
                    .await,
                ClientResponse::Error(_)
            );
        }
        if let Some(leader_id) = self.control.leader()
            && let Some(addr) = self.intra_addr(leader_id)
        {
            return !matches!(
                self.relay(addr, ClientRequest::ProposeSchema(command.clone()))
                    .await,
                ClientResponse::Error(_)
            );
        }
        // No locally-known leader. The common cause is a real control-group
        // voter mid-election (rare, brief); the other is a **control-plane-
        // follower-less growth node** (ADR 0030) whose own control `RaftCore`
        // never learns a leader at all, since it never receives real Raft
        // traffic for a group it was never a voter of — for it, this is the
        // *only* path that can ever reach the real cluster (its own local
        // `propose` always fails, and it has no leader hint to relay a single
        // hop to). Broadcast to every other known **intra** address instead:
        // a real control-group member among them resolves the actual leader
        // itself (one more hop — `ProposeSchema`'s handler is a single,
        // bounded relay, never a chain). Returns true on the first address that
        // connects, regardless of what its own `propose_schema` achieves
        // (best-effort, same as every other branch here — the caller confirms
        // via replicated `Metadata`, not this return value).
        for (id, addr) in self.intra_route_snapshot() {
            // Self-skip by id, not by address string: this node's own
            // `intra_route` entry is `advertised_addr(self)`, which a bind
            // address comparison would never match once `advertise_host`
            // (ADR 0060) is set — the id is the one identity that's always
            // comparable regardless of how this node's own address is
            // spelled.
            if Some(&id) == self.admin.node_id.as_ref() {
                continue;
            }
            if !matches!(
                self.relay(addr, ClientRequest::ProposeSchema(command.clone()))
                    .await,
                ClientResponse::Error(_)
            ) {
                return true;
            }
        }
        false
    }

    /// Provision the **first tablet** of `table` (ADR 0023): a fresh cluster has no
    /// data tablet, so `CreateTable` stands one up — a single tablet covering the
    /// whole token ring, scoped to `table`, which splits on demand as it grows. The
    /// replica set is the first `min(N, RF)` `Active` CP members. Relays
    /// `CreateTablet` to the control leader and waits until it appears, then attaches
    /// an RF `SetTabletPolicy` (so the reconciler auto-replaces a `Down` replica) on
    /// the committed tablet id. Idempotent + race-safe: the state machine admits only
    /// one `CreateTablet` per table, so concurrent callers converge on one tablet.
    pub(crate) async fn provision_tablet(&self, table: &str) -> Result<(), String> {
        let deadline = self.env.now().saturating_add(SCHEMA_COMMIT_TIMEOUT);
        // Propose-side patience (the `propose_and_await` discipline — see the
        // retry-amplification entries in `docs/engineering-lessons.md`): the
        // *poll* below stays at `SCHEMA_POLL_INTERVAL` so a commit is observed
        // promptly, but a command believed to have reached a leader's log is
        // not re-proposed until `SCHEMA_PROPOSE_PATIENCE` elapses. This loop
        // used to re-propose on every 50ms poll tick, appending a duplicate
        // control-log entry each time — harmless to apply (`CreateTablet` is
        // first-committer-wins per table) but real WAL/replication/apply work
        // piled onto the control plane under exactly the slow-commit
        // conditions that make this wait long in the first place (measured on
        // a deliberately slowed disk: a six-table concurrent bring-up
        // proposed `CreateTablet` 264 times and `SetTabletPolicy` 240 times
        // for what should be ~6+6 — the self-amplification behind issue
        // #268's 25s seed-put flake on starved CI runners). It cannot simply
        // ride `propose_and_await`: the create arm must re-derive its tablet
        // id and replica set from fresh metadata per proposal (the
        // `trigger_split` stale-allocator lesson), and the needed command
        // switches to `SetTabletPolicy` once the tablet exists — hence an
        // inline pacer, reset on the phase switch so the policy proposal is
        // not held back by the create proposal's own patience window.
        let mut next_propose_at = self.env.now();
        let mut last_proposed_create: Option<bool> = None;
        loop {
            // Fresh, not `metadata_cached()` (ADR 0035 PR4): the "no tablet
            // yet" branch below picks the tablet's *initial* replica set from
            // `meta.members`, and a `Remote` data node's mirror is routinely a
            // poll interval stale (ADR 0035 §5) — `metadata_fresh()` avoids
            // needlessly under-sizing that initial set on a node whose own
            // read is avoidably behind.
            //
            // But freshness of the READ is not enough on its own to make the
            // recorded POLICY correct, and — after this exact race recurred
            // under `cluster_growth.rs`'s heavy three-concurrent-cluster load
            // (see `docs/engineering-lessons.md`) — the policy below is
            // deliberately no longer derived from `t.replicas.len()` at all.
            // **The invariant is: the policy always records the *target* RF
            // (`MAX_REPLICATION_FACTOR`), never whatever the replica set's
            // size happened to be at creation.** `CreateTablet` only ever
            // succeeds once per table (idempotent, first-committer wins) and
            // may legitimately mint a *smaller* initial set if fewer than
            // `MAX_REPLICATION_FACTOR` members are `Active` yet at that
            // instant — even a maximally fresh read can observe a cluster
            // that is still mid-bootstrap, promoting its own members one
            // commit at a time. Recording the *target* rather than the
            // *observation* is what makes that best-effort initial set
            // self-heal: `reconcile_placement`'s existing violation-repair
            // path (the same one that replaces a later-killed replica)
            // proposes a `CasTabletReplicas` growing it to
            // `MAX_REPLICATION_FACTOR` the moment enough candidates are
            // `Active`, with no separate "did the RF ever get set right"
            // mechanism needed. A too-low RF baked from a point-in-time
            // observation, by contrast, is invisible to that machinery
            // forever — `reconcile_placement` only fixes *violations of the
            // recorded policy*, so an under-observed RF just becomes a new,
            // permanently-satisfied target.
            let meta = self.control.metadata_fresh().await;
            if let Some((&tablet, _)) = meta.tablets_for_table(table).next() {
                // The tablet exists; ensure its RF policy is set, then we're done. The
                // caller's op routes through `cp_route`, which itself waits for the
                // group to form/elect (`CLIENT_TIMEOUT`), so provisioning need not
                // block on serveability here.
                if meta.policies.contains_key(&tablet) {
                    return Ok(());
                }
                let now = self.env.now();
                if last_proposed_create != Some(false) || now >= next_propose_at {
                    let sent = self
                        .propose_schema(&MetaCommand::SetTabletPolicy {
                            tablet,
                            policy: Some(PlacementPolicy::simple("cp-rf", MAX_REPLICATION_FACTOR)),
                        })
                        .await;
                    last_proposed_create = Some(false);
                    next_propose_at = now.saturating_add(if sent {
                        SCHEMA_PROPOSE_PATIENCE
                    } else {
                        Duration::ZERO
                    });
                }
            } else {
                // No tablet yet: pick the first min(N, RF) Active CP members and
                // propose its creation toward the control leader.
                let mut replicas: Vec<NodeId> = meta
                    .members
                    .iter()
                    .filter(|(_, m)| m.status == NodeStatus::Active)
                    .map(|(id, _)| id.clone())
                    .collect();
                replicas.truncate(MAX_REPLICATION_FACTOR);
                let now = self.env.now();
                if !replicas.is_empty()
                    && (last_proposed_create != Some(true) || now >= next_propose_at)
                {
                    // The id and replica set are re-derived fresh per
                    // (re)proposal, never captured once outside the loop — a
                    // stale allocator-derived id is the `trigger_split`
                    // collision lesson (`docs/engineering-lessons.md`).
                    let sent = self
                        .propose_schema(&MetaCommand::CreateTablet {
                            tablet: meta.next_free_tablet_id(),
                            table: Some(table.to_owned()),
                            range: KeyRange::whole(),
                            replicas,
                        })
                        .await;
                    last_proposed_create = Some(true);
                    next_propose_at = now.saturating_add(if sent {
                        SCHEMA_PROPOSE_PATIENCE
                    } else {
                        Duration::ZERO
                    });
                }
            }
            if self.env.now() >= deadline {
                return Err("table tablet did not provision in time".into());
            }
            self.env.sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Wait until `table`'s just-provisioned tablet can actually **serve** a
    /// request, by issuing a linearizable probe read through the ordinary
    /// [`cp_read`](Self::cp_read) routing machinery (ReadIndex on the group
    /// leader, local or forwarded) until it succeeds — a converged-or-timeout
    /// poll, never a fixed sleep.
    ///
    /// [`provision_tablet`](Self::provision_tablet) deliberately confirms only
    /// the **metadata** commit; the tablet's Raft group then forms and elects
    /// asynchronously (each replica's tablet-host reconciler, ADR 0031). A
    /// caller that *acks table creation to a client* (the DynamoDB
    /// `CreateTable` edge) must call this before
    /// replying, or the ack races the formation window: the client's
    /// immediately-following first write only lands via the election-wait
    /// machinery (`cp_forward`'s backoff pass / the local
    /// `RouteDecision::Wait`) and, under unlucky timing, can burn much of its
    /// own `CLIENT_TIMEOUT` or fail outright. First-*write* auto-provision
    /// paths (`cp_kind_write_item`, `fast_marker_write`, …) need no such call
    /// — their own op routes through `cp_route`, which already waits.
    ///
    /// The probe key is the empty key: a freshly-provisioned table has one
    /// tablet over the whole ring (`KeyRange::whole()`), whose range contains
    /// every key, so the probe routes to it without minting a token-prefixed
    /// key — and a served read of an absent key still proves the full path
    /// (leader elected, ReadIndex barrier satisfied) that a first write needs.
    /// A ReadIndex success requires the leader to confirm quorum contact, so
    /// "readable" here implies "can commit a write promptly" too.
    ///
    /// On timeout the table + tablet already exist (both commits confirmed
    /// upstream) — the error only means the group did not become serveable
    /// within the budget, exactly the state a retried data op's own routing
    /// wait would then contend with.
    pub(crate) async fn await_table_serveable(&self, table: &str) -> Result<(), String> {
        // One `cp_read` is already internally bounded (`cp_route`'s wait and
        // `cp_forward`'s election backoff are both capped by `CLIENT_TIMEOUT`),
        // but it can surface a non-retryable-shaped transient early (e.g. a
        // forwarding hop's transport error mid-formation) — so wrap it in the
        // house converged-or-timeout retry loop with its own overall deadline.
        let deadline = self.env.now().saturating_add(CLIENT_TIMEOUT);
        loop {
            // Deliberately `Strong` (ADR 0055): this probe exists to prove
            // the group has actually elected and can serve a linearizable
            // read before `CreateTable` acks — an eventual read would pass
            // against a replica that has merely applied something, which is
            // precisely the formation window the probe must not hand the
            // client (ADR 0023's 2026-08-17 amendment).
            let err = match self
                .cp_read(table, Vec::new(), ReadConsistency::Strong)
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => e,
            };
            if self.env.now() >= deadline {
                return Err(format!(
                    "table `{table}` was created but its tablet did not become \
                     serveable in time: {err}"
                ));
            }
            self.env.sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Kick off a split of `tablet` at `split_key` — either the **copy-based**
    /// workflow (ADR 0050: propose `MetaCommand::BeginSplit` — parent to
    /// `Splitting`, still fully serving, two `Building` children minted at
    /// **placement-chosen final homes**, fork F5, [`split_child_placement`])
    /// or the **in-place** workflow (ADR 0058 Train 2: propose
    /// `MetaCommand::BeginSplitInPlace` — parent to `Splitting`, no tablet-map
    /// rows minted, the intent recorded directly on the parent for the CP
    /// data plane's own host reconciler to drive), selected by
    /// [`ClientCtx::split_mode`] — the ONE branch point between the two;
    /// everything else on this call (the idempotent already-`Splitting`
    /// handling, the confirm loop, the child-id allocation, F11 token
    /// alignment) is shared verbatim. Confirms by observing the parent's own
    /// state become `Splitting` (state-based, replacing the old zero-copy
    /// epoch-advance confirm: a rebalance's `CasTabletReplicas` also bumps
    /// the epoch, so an epoch advance alone proves nothing about a split;
    /// observing the state does, and on a stray epoch bump the loop re-arms
    /// its CAS instead of mis-reporting).
    ///
    /// **Asynchronous by design**: success means *the split workflow
    /// started* — a copy-based split's own driver (ADR 0050 stages 2–4)
    /// seeds the children and performs the freeze/cutover; an in-place
    /// split's fork happens entirely inside the CP data plane's own Raft
    /// apply (ADR 0058 Stage 3) and its cutover is driven by
    /// `index_drain.rs`'s `inplace_split_driver_tick`. This call never waits
    /// for either. Calling on a tablet already `Splitting` returns success
    /// immediately ("already in flight" — the caller's intent is
    /// accomplished-in-progress, and kickoff is idempotent) **regardless of
    /// which workflow is running** — a stale-configured caller can never
    /// re-trigger a split that already started under the other mode.
    ///
    /// Routed to the control leader (relayable, [`is_relayable_command`]), so
    /// this works from any node the client happens to be connected to.
    #[tracing::instrument(
        name = "split_tablet",
        skip(self, split_key),
        fields(tablet = tablet.0, new_id = tracing::field::Empty)
    )]
    pub(crate) async fn trigger_split(
        &self,
        tablet: TabletId,
        split_key: Vec<u8>,
    ) -> ClientResponse {
        // `effective_metadata()`, not `self.control.metadata_cached()`
        // directly (ADR 0035 PR5 staleness-audit fix): unlike a plain stale
        // read racing a *concurrent* epoch bump — which the CAS below catches
        // cleanly, since `expected_epoch` would just fail to match at apply
        // time — `metadata_cached()` is *permanently* empty on a
        // control-plane-follower-less growth node (ADR 0030), so the
        // `tablets.get(&tablet)` lookup below would unconditionally miss and
        // this would always return "no such tablet" before ever proposing
        // anything, on every call, regardless of whether the tablet actually
        // exists on the real cluster. The CAS only protects against
        // staleness *after* a read succeeds; it can't rescue a read that
        // never has anything to see.
        let mut initial_epoch = match self.effective_metadata().tablets.get(&tablet) {
            None => return ClientResponse::Error("no such tablet".into()),
            Some(t) if t.state == TabletState::Splitting => {
                // Already mid-workflow: kickoff is idempotent.
                return ClientResponse::PutOk;
            }
            Some(t) if t.state == TabletState::Building => {
                return ClientResponse::Error(
                    "tablet is a Building split child - not splittable".into(),
                );
            }
            Some(t) => t.epoch,
        };
        let deadline = self.env.now().saturating_add(SCHEMA_COMMIT_TIMEOUT);
        let mut next_propose_at = self.env.now();
        loop {
            // Confirmed: the parent's own STATE became `Splitting` — the one
            // transition only a committed `BeginSplit` of this exact tablet
            // performs (ours, or a racing proposer's that landed first —
            // harmless: "this tablet's split workflow is running" is what
            // the caller wanted either way). Deliberately state-based, not
            // the old epoch-advance confirm: a rebalance's
            // `CasTabletReplicas` also bumps a tablet's epoch, so an epoch
            // advance alone can't distinguish "my split landed" from "an
            // unrelated placement move landed"; a stray epoch bump instead
            // RE-ARMS the CAS below so the next propose attempt carries the
            // fresh epoch rather than being rejected forever. (The old
            // confirm's own hazard — two proposers computing one `new_id`
            // from equally-stale reads — still shapes the id choice below:
            // child ids are recomputed fresh from the allocator on every
            // attempt, never once up front.)
            let meta = self.effective_metadata();
            match meta.tablets.get(&tablet) {
                None => return ClientResponse::Error("no such tablet".into()),
                Some(t) if t.state == TabletState::Splitting => return ClientResponse::PutOk,
                Some(t) if t.epoch != initial_epoch => {
                    // An unrelated epoch bump (rebalance/repair CAS): re-arm.
                    initial_epoch = t.epoch;
                }
                Some(_) => {}
            }
            if self.env.now() >= deadline {
                return ClientResponse::Error("split did not begin in time".into());
            }
            if self.env.now() >= next_propose_at {
                // Child ids come from the **monotonic allocator**
                // (`next_free_tablet_id`, ADR 0023 — the same allocator
                // provisioning uses), *not* `max(existing ids) + 1`, which
                // could re-mint a freed id after a `DropTableTablets`.
                // Recomputed fresh on **every** propose attempt (not once
                // up front) — the collision-race fix inherited from the old
                // confirm's rewrite: a later attempt, once this node's own
                // metadata has caught up, sees the allocator floor moved
                // past whatever else was created meanwhile and mints
                // genuinely free ids instead of repeating doomed ones.
                let left_id = meta.next_free_tablet_id();
                let right_id = TabletId(left_id.0 + 1);
                // F11 (ADR 0042 §14, Fork D): this is the ONE choke point
                // every split proposer funnels through — `auto_split_loop`,
                // `POST /admin/tablet/split` (`admin::action_split`), and
                // `ClientRequest::SplitTablet`'s handler all call this
                // method and nothing else, so rounding here (rather than in
                // each caller) structurally can't be forgotten by a future
                // one. See `align_split_key`'s own doc for the rounding +
                // single-token-skip rule (Fork E). `tablet`'s range cannot
                // have changed since `initial_epoch` was captured (the loop
                // only reaches here while the epoch check above still
                // matches), so recomputing this every attempt is
                // equivalent to computing it once — just simpler to read
                // alongside the fresh `new_id` above.
                let (aligned_key, viable) =
                    decide::align_split_key(&meta, tablet, split_key.clone());
                if !viable {
                    self.control
                        .metrics()
                        .incr(Metric::StreamSplitSingleTokenSkipped);
                    return ClientResponse::Error(SPLIT_KEY_NOT_TOKEN_VIABLE.into());
                }
                tracing::Span::current().record("new_id", left_id.0);
                // Fork F5: children are minted at placement-chosen final
                // homes — the one data movement of a copy-based split is the
                // build itself, so the mint must pick the real destinations.
                let children_replicas = match split_child_placement(&meta, tablet) {
                    Ok(sets) => sets,
                    Err(e) => return ClientResponse::Error(e),
                };
                let [left_replicas, right_replicas] = children_replicas;
                // ADR 0058 Train 2 rung 3 residue: `self.split_mode` is the
                // ONE branch point between the two workflows — both
                // commands share the identical `{parent, expected_epoch,
                // split_key, children}` shape (`BeginSplitInPlace`'s own
                // doc), the idempotent already-`Splitting` handling and the
                // confirm loop above are unchanged either way, and neither
                // `auto_split_loop` nor any other caller of `trigger_split`
                // needs to know which one ran.
                let cmd = match self.split_mode {
                    SplitMode::Copy => MetaCommand::BeginSplit {
                        parent: tablet,
                        expected_epoch: initial_epoch,
                        split_key: aligned_key,
                        children: [(left_id, left_replicas), (right_id, right_replicas)],
                    },
                    SplitMode::InPlace => MetaCommand::BeginSplitInPlace {
                        parent: tablet,
                        expected_epoch: initial_epoch,
                        split_key: aligned_key,
                        children: [(left_id, left_replicas), (right_id, right_replicas)],
                    },
                };
                let sent = self.propose_schema(&cmd).await;
                next_propose_at = self.env.now().saturating_add(if sent {
                    SCHEMA_PROPOSE_PATIENCE
                } else {
                    Duration::ZERO
                });
            }
            self.env.sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// **Registration compare-and-swap** (ADR 0040 Decision C): propose
    /// `MetaCommand::RegisterNode` on the control-plane leader (locally if
    /// we are it, else relayed — [`is_relayable_command`] allows this
    /// command) and wait for the claim to commit + replicate here,
    /// structurally identical to [`trigger_split`](Self::trigger_split) —
    /// propose, then poll for the exact effect. Primarily polls
    /// [`metadata_fresh`](Self::metadata_fresh) (a genuine read-your-writes
    /// round trip on `Remote`) rather than `effective_metadata()`/
    /// `metadata_cached()`: a wrong "collision" verdict here has real,
    /// structural consequences for the caller (a minted id re-mints and
    /// retries; a proposed id fails loudly) that a possibly-stale cached
    /// read could get wrong.
    ///
    /// **Falls back to [`effective_metadata`](Self::effective_metadata)
    /// when `metadata_fresh()` hasn't (yet) confirmed anything** (root-cause
    /// fix for a decommission-vs-self-registration race, see
    /// `docs/engineering-lessons.md`): `metadata_fresh()`'s own doc already
    /// documents that a growth/permanently-non-voting node's local
    /// `RaftNode` "stays exactly as stuck" as it always was — its own local
    /// Raft log never independently advances, by ADR 0030 design, so this
    /// confirmation could **never** succeed for exactly the shape of caller
    /// this function itself names as its primary one: `spawn_common_tail`'s
    /// one-shot self-registration, which runs on *every* node shape,
    /// including a growth node. Without the fallback, that self-registration
    /// silently burns the *entire* `SCHEMA_COMMIT_TIMEOUT` re-proposing an
    /// already-successful, already-committed `RegisterNode` on every single
    /// join (never observing its own success), and if an operator drains +
    /// removes that same node while this futile retry loop is still live,
    /// the stale re-propose can land *after* `RemoveMember` clears
    /// `node_addrs`/`members` — indistinguishable, at apply time, from a
    /// genuinely fresh claim (`MetaCommand::RegisterNode`'s own apply arm
    /// has no notion of "this identity was just decommissioned") — silently
    /// resurrecting the just-removed node as a fresh `Down` member, which a
    /// live heartbeat then promotes straight back to `Active`. The fallback
    /// only ever *widens* when this converges (never narrows: `metadata_
    /// fresh()` is still tried first, unchanged, so a genuine voter — for
    /// which the two reads coincide, no mirror overlay ever being active —
    /// sees no behavior change at all) — it makes a growth node's own
    /// self-registration observe its own already-committed success
    /// immediately (one `SCHEMA_POLL_INTERVAL` tick) instead of blindly
    /// re-proposing for a full 10s, closing the race window this caused.
    /// The other caller, `admin_add_control_member`, only ever runs from a
    /// genuine control-group leader — the fallback is inert there too.
    ///
    /// Returns [`RegisterOutcome::Registered`] once `node_addrs[node]`
    /// equals exactly the `addrs` just proposed (whether from this call's
    /// own `Applied`, an idempotent `NoOp` replay of an identical prior
    /// claim, or a concurrent identical registration that landed first —
    /// all indistinguishable on purpose, since only the observable state
    /// matters); [`RegisterOutcome::Collision`] once `node_addrs[node]` is
    /// visibly a **different** entry — a durable fact, not a timing fluke,
    /// so a caller never needs to poll further once it sees this.
    pub(crate) async fn register_node(
        &self,
        node: NodeId,
        addrs: NodeAddrs,
        labels: BTreeMap<String, String>,
    ) -> Result<RegisterOutcome, String> {
        let cmd = MetaCommand::RegisterNode {
            node: node.clone(),
            addrs: addrs.clone(),
            labels,
        };
        match self
            .propose_and_await(cmd, SCHEMA_COMMIT_TIMEOUT, || async {
                if let Some(outcome) = Self::register_outcome_from(
                    &self.metadata_fresh().await.node_addrs,
                    &node,
                    &addrs,
                ) {
                    return Some(outcome);
                }
                Self::register_outcome_from(&self.effective_metadata().node_addrs, &node, &addrs)
            })
            .await
        {
            Ok(outcome) => Ok(outcome),
            Err(()) => Err(format!(
                "node registration for {node} did not commit within {}s \
                 (no control-plane leader reachable?)",
                SCHEMA_COMMIT_TIMEOUT.as_secs()
            )),
        }
    }

    /// Shared verdict for [`register_node`](Self::register_node)'s two reads
    /// (`metadata_fresh()` then, on `None`, the `effective_metadata()`
    /// fallback): `node_addrs`'s entry for `node`, if any, exactly matches
    /// `addrs` (`Registered`), is visibly something else (`Collision`), or
    /// is absent (`None`, not yet observable from *this* source — the caller
    /// tries the next one, or waits for the next poll tick).
    fn register_outcome_from(
        node_addrs: &BTreeMap<NodeId, NodeAddrs>,
        node: &NodeId,
        addrs: &NodeAddrs,
    ) -> Option<RegisterOutcome> {
        match node_addrs.get(node) {
            Some(existing) if existing == addrs => Some(RegisterOutcome::Registered),
            Some(_) => Some(RegisterOutcome::Collision),
            None => None,
        }
    }

    /// Drop `table` **and garbage-collect its data** (ADR 0024), cascading to
    /// every GSI's hidden index table (ADR 0041 §5): remove the schema from the
    /// replicated catalog, then remove every affected table's tablets from the
    /// replicated tablet map — the trigger each hosting node's per-node
    /// tablet-host reconciler (ADR 0031 PR4) converges on by stopping its
    /// local group and deleting its engine + WAL files. This is the real
    /// `DeleteTable` sink (the DynamoDB edge + the admin
    /// dashboard); [`drop_table_schema`](Self::drop_table_schema) alone remains
    /// the schema-only primitive (the admin panel's schema-only drop).
    /// Returns once the schema and every tablet (base **and** hidden index)
    /// have left this node's replicated metadata; the per-node file
    /// reclamation continues asynchronously on every replica.
    ///
    /// **Cascade order is load-bearing for convergence under a crash-and-retry
    /// (ADR 0041 §5's as-built note)**:
    ///
    /// 1. **Enumerate the table's GSIs and drop each hidden table's tablets
    ///    first**, while the definitions are still enumerable — a base
    ///    table's LSIs need no separate step (colocated in the base table's
    ///    own tablets, reclaimed by step 3's `erase_scope`, which walks every
    ///    row kind). The read is [`metadata_fresh`](Self::metadata_fresh), not
    ///    a cached/mirrored view: this is a **permanent** decision (once step
    ///    2 removes the schema, the defs are gone for good), so it must not
    ///    read stale. A crash here leaves the base schema and its defs
    ///    intact, so a retry re-enumerates and finishes any hidden table this
    ///    attempt didn't reach.
    /// 2. **Drop the base schema** (which deletes the GSI/LSI *definitions*
    ///    with it). A crash here leaves a state where step 1's hidden-table
    ///    drops already landed but the base tablets have not — a retry's
    ///    step 1 finds no GSIs left to enumerate (already gone) and proceeds
    ///    straight to step 3.
    /// 3. **Drop the base table's own tablets** (base rows, colocated LSI
    ///    rows, the change log, and footprints — all four `StorageScope`
    ///    kinds sharing one tablet group, reclaimed together by
    ///    `CpGroup::erase_scope` iterating `kind_scopes`). A crash here
    ///    leaves the schema gone but the base tablets present — a retry's
    ///    steps 1/2 are no-ops (idempotent) and it finishes step 3.
    ///
    /// **Belt-and-suspenders second sweep**: the GSI drain provisions a
    /// hidden table's first tablet *lazily*, and can do so **concurrently**
    /// with this drop (a change record drained mid-drop, racing step 1's
    /// enumeration). After step 3, sweep the tablet map itself — not the
    /// now-gone index definitions — for any tablet named `<table>$<index>`
    /// ([`animus_dynamo::split_index_table_name`]) and drop those too. This
    /// is keyed on the tablet map, so it also cleans up any orphan left by a
    /// **pre-fix** drop that never cascaded at all. `drain_tablet`'s own
    /// provisioning and `reconcile_partition`'s writes race this drop
    /// harmlessly — both error paths are logged-and-swallowed by
    /// `change_consumer_loop` (best-effort convergence; the next tick just
    /// retries), and once this table's groups leave `hosted_groups()` (the
    /// reconciler's `Reclaim` teardown), the drain simply stops sweeping
    /// them.
    pub(crate) async fn drop_table(&self, table: String) -> Result<(), String> {
        let indexes = self.metadata_fresh().await.table_indexes(&table).to_vec();
        for idx in indexes
            .iter()
            .filter(|idx| idx.kind == animus_control::schema::IndexKind::Global)
        {
            let index_table = animus_dynamo::index_table_name(&table, &idx.name);
            self.drop_table_tablets(index_table).await?;
        }

        self.drop_table_schema(table.clone()).await?;
        self.drop_table_tablets(table.clone()).await?;

        let orphans: BTreeSet<String> = self
            .effective_metadata()
            .tablets
            .values()
            .filter_map(|t| t.table.as_deref())
            .filter(|name| {
                animus_dynamo::split_index_table_name(name).is_some_and(|(base, _)| base == table)
            })
            .map(str::to_owned)
            .collect();
        for orphan in orphans {
            self.drop_table_tablets(orphan).await?;
        }
        Ok(())
    }

    /// Propose `MetaCommand::DropTableTablets` for `table` and wait until every
    /// tablet scoped to it has left this node's replicated metadata (ADR 0024).
    /// Shared by [`drop_table`](Self::drop_table)'s base-table drop, its
    /// GSI-hidden-table cascade (ADR 0041 §5), and `dynamo.rs`'s single-index
    /// drop cascade (ADR 0045 §5, `drop_index`) — same command, same
    /// commit-wait discipline in all three. `pub(crate)` (not module-private)
    /// for exactly that last caller, a sibling module. `table` need not have
    /// a schema entry: a hidden index table never has one, and
    /// `DropTableTablets`'s apply is keyed purely on the tablet map
    /// (`tablets_for_table`), not the schema catalog.
    pub(crate) async fn drop_table_tablets(&self, table: String) -> Result<(), String> {
        let command = MetaCommand::DropTableTablets {
            table: table.clone(),
        };
        // Always propose at least once — never gate on "no tablets in *this*
        // node's metadata": a lagging replica may not have applied the tablet's
        // creation yet, so local absence cannot prove there is nothing to drop
        // (and `propose_and_await` returns on its first poll in that state).
        // The command is idempotent (`NoOp`) on the leader when there truly is
        // nothing. (A *schema'd* base table is safe either way — the
        // schema-drop wait already forced this replica past the tablet's
        // creation in the log — but a plain-client table, or a hidden index
        // table with no schema wait at all, skips that forcing.)
        self.propose_schema(&command).await;
        // `effective_metadata()`, not `self.control.metadata_cached()`
        // directly (ADR 0035 PR5 staleness-audit fix): the latter is
        // permanently empty on a control-plane-follower-less growth node
        // (ADR 0030), so `tablets_for_table(&table).next().is_none()` was
        // unconditionally `true` there — reporting a false success on the
        // very first poll regardless of whether the drop actually committed,
        // not merely timing out. `effective_metadata()`'s mirror is the
        // right contract here (this poll confirms *absence*, which the
        // cache-tolerant view proves just as soundly as a fresh one once it
        // has synced at all).
        self.propose_and_await(command, SCHEMA_COMMIT_TIMEOUT, || async {
            self.effective_metadata()
                .tablets_for_table(&table)
                .next()
                .is_none()
                .then_some(())
        })
        .await
        .map_err(|()| {
            format!(
                "DROP TABLE `{table}`: tablet GC did not commit within {}s (no control-plane leader reachable?)",
                SCHEMA_COMMIT_TIMEOUT.as_secs()
            )
        })
    }

    /// Propose `MetaCommand::DropTableSchema` and wait for the table to disappear
    /// from the replicated catalog (ADR 0013). Idempotent: dropping an absent
    /// table returns `Ok(())` immediately. Schema-only: does
    /// **not** touch the table's tablets/data (the admin panel's schema-only
    /// drop uses this); a real drop goes through [`drop_table`](Self::drop_table).
    pub(crate) async fn drop_table_schema(&self, table: String) -> Result<(), String> {
        // Fresh, not a cache-tolerant read (ADR 0035 PR1): this is a
        // commit-wait poll, which must observe its own just-proposed
        // command landing in the authoritative state.
        if !self.metadata_fresh().await.has_table_schema(&table) {
            return Ok(());
        }
        let command = MetaCommand::DropTableSchema {
            table: table.clone(),
        };
        self.propose_and_await(command, SCHEMA_COMMIT_TIMEOUT, || async {
            (!self.metadata_fresh().await.has_table_schema(&table)).then_some(())
        })
        .await
        .map_err(|()| {
            format!(
                "DROP TABLE `{table}` did not commit within {}s (no control-plane leader reachable?)",
                SCHEMA_COMMIT_TIMEOUT.as_secs()
            )
        })
    }

    /// Propose `command` on the current leader and poll `committed` until it
    /// reports the change visible in this node's replicated metadata (or time
    /// out). Resubmits the proposal on a leader change or transient failure, but
    /// **not** on every poll tick while a prior attempt is still believed
    /// in-flight — see [`propose_schema`](Self::propose_schema)'s doc; that
    /// backs off for [`SCHEMA_PROPOSE_PATIENCE`] after a proposal we believe
    /// reached a leader's log, only resubmitting immediately when we know it
    /// wasn't sent anywhere. Returns the committed value `committed` observed,
    /// or `Err(())` on timeout.
    ///
    /// `committed` is an **async** closure (ADR 0035 PR4 — [`metadata_fresh`]
    /// is now a genuine network round trip on a `Remote` handle, so every
    /// caller's commit-wait predicate must be able to `.await` it; every
    /// existing call site's predicate — sync in substance for a `Local`
    /// handle — just gained an `async` wrapper with no behavior change).
    pub(crate) async fn propose_and_await<T, Fut>(
        &self,
        command: MetaCommand,
        timeout: Duration,
        committed: impl Fn() -> Fut,
    ) -> Result<T, ()>
    where
        Fut: std::future::Future<Output = Option<T>>,
    {
        let deadline = self.env.now().saturating_add(timeout);
        let mut next_propose_at = self.env.now();
        loop {
            if let Some(value) = committed().await {
                return Ok(value);
            }
            let now = self.env.now();
            if now >= deadline {
                return Err(());
            }
            if now >= next_propose_at {
                let sent = self.propose_schema(&command).await;
                next_propose_at = now.saturating_add(if sent {
                    SCHEMA_PROPOSE_PATIENCE
                } else {
                    Duration::ZERO
                });
            }
            self.env.sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Force one seal pass of `tablet`'s own hot tail (ADR 0042/0043's F12-b
    /// disable-triggered final seal), wherever that tablet's leader actually
    /// runs — the caller (`dynamo.rs`'s disable flow) may be connected to
    /// any node, not necessarily one that leads any of the table's tablets.
    ///
    /// Forwards via [`forward_to_tablet_leader`](Self::forward_to_tablet_leader)
    /// (the hint-chasing shape) — an earlier revision relayed once and
    /// re-resolved `resolve_cp_route` from scratch instead, which never
    /// converges when this node hosts no replica of `tablet` (see the
    /// helper's doc); the outer loop here still re-resolves between chases
    /// as its converged-or-timeout backstop.
    pub(crate) async fn force_seal_tablet(&self, tablet: TabletId) -> Result<(), String> {
        let deadline = self.env.now().saturating_add(SCHEMA_COMMIT_TIMEOUT);
        loop {
            match self.resolve_cp_route(tablet) {
                Some(CpRoute::Local(leader)) => {
                    let table = self
                        .effective_metadata()
                        .tablets
                        .get(&tablet)
                        .and_then(|t| t.table.clone());
                    let Some(table) = table else {
                        return Err("no such tablet".into());
                    };
                    return index_drain::seal_now(self, &table, tablet, &leader)
                        .await
                        .map(|_| ());
                }
                Some(CpRoute::Forward(addr)) => {
                    let request = ClientRequest::ForceSeal { tablet: tablet.0 };
                    match self
                        .forward_to_tablet_leader(Some(tablet), addr, request)
                        .await
                    {
                        ClientResponse::PutOk => return Ok(()),
                        ClientResponse::Error(e) if self.env.now() >= deadline => {
                            return Err(e);
                        }
                        ClientResponse::Error(_) => {} // retry below
                        other => {
                            return Err(format!(
                                "unexpected reply to forwarded force-seal: {other:?}"
                            ));
                        }
                    }
                }
                Some(CpRoute::None) | None => {} // not settled yet, retry
            }
            if self.env.now() >= deadline {
                return Err("force-seal did not reach a tablet leader in time".into());
            }
            self.env.sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Force one PITR seal pass of `tablet`'s own hot tail (ADR 0059 §9,
    /// Train 3's disable-triggered final seal) — the PITR twin of
    /// [`force_seal_tablet`](Self::force_seal_tablet), identical shape and
    /// identical forwarding discipline.
    pub(crate) async fn force_pitr_seal_tablet(&self, tablet: TabletId) -> Result<(), String> {
        let deadline = self.env.now().saturating_add(SCHEMA_COMMIT_TIMEOUT);
        loop {
            match self.resolve_cp_route(tablet) {
                Some(CpRoute::Local(leader)) => {
                    let table = self
                        .effective_metadata()
                        .tablets
                        .get(&tablet)
                        .and_then(|t| t.table.clone());
                    let Some(table) = table else {
                        return Err("no such tablet".into());
                    };
                    return index_drain::pitr_seal_now(self, &table, tablet, &leader)
                        .await
                        .map(|_| ());
                }
                Some(CpRoute::Forward(addr)) => {
                    let request = ClientRequest::ForcePitrSeal { tablet: tablet.0 };
                    match self
                        .forward_to_tablet_leader(Some(tablet), addr, request)
                        .await
                    {
                        ClientResponse::PutOk => return Ok(()),
                        ClientResponse::Error(e) if self.env.now() >= deadline => {
                            return Err(e);
                        }
                        ClientResponse::Error(_) => {} // retry below
                        other => {
                            return Err(format!(
                                "unexpected reply to forwarded PITR force-seal: {other:?}"
                            ));
                        }
                    }
                }
                Some(CpRoute::None) | None => {} // not settled yet, retry
            }
            if self.env.now() >= deadline {
                return Err("PITR force-seal did not reach a tablet leader in time".into());
            }
            self.env.sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// One tablet's own share of growth PR3's manual trigger (`POST
    /// /admin/stream/grow`, [`grow_stream`](Self::grow_stream)'s per-tablet
    /// call): wherever `tablet`'s own CP group leader actually runs,
    /// materialize its live pairs and split at their byte-weighted median
    /// ([`median_split_key`]) via [`trigger_split`](Self::trigger_split) —
    /// which independently applies F11's token-rounding and Fork E's
    /// single-token skip, exactly as every other split proposer does.
    /// Returns the tablet's own [`ClientResponse`] verbatim: `PutOk` for a
    /// genuine split, or an `Error` naming [`STREAM_GROW_NO_SPLIT_POINT`]/
    /// [`SPLIT_KEY_NOT_TOKEN_VIABLE`] for an expected skip (or any other
    /// real error) — the caller (`admin::action_stream_grow`) classifies
    /// these, never treating one tablet's skip as a failure of the whole
    /// multi-tablet action. Same shape as
    /// [`force_seal_tablet`](Self::force_seal_tablet) (resolve → local or
    /// forward, retry until a deadline), except a `Forward` reply is
    /// returned immediately unless it is specifically a stale "not leader
    /// here" refusal (`topology::parse_not_leader_refusal`) — every other
    /// error (including this action's own expected skips) is a terminal
    /// outcome, not a signal to keep retrying.
    pub(crate) async fn grow_stream_tablet(&self, tablet: TabletId) -> ClientResponse {
        let deadline = self.env.now().saturating_add(SCHEMA_COMMIT_TIMEOUT);
        loop {
            match self.resolve_cp_route(tablet) {
                Some(CpRoute::Local(leader)) => {
                    return match median_split_key(&leader).await {
                        None => ClientResponse::Error(STREAM_GROW_NO_SPLIT_POINT.into()),
                        Some(split_key) => self.trigger_split(tablet, split_key).await,
                    };
                }
                Some(CpRoute::Forward(addr)) => {
                    let request = ClientRequest::TriggerAutoSplit { tablet: tablet.0 };
                    match self
                        .forward_to_tablet_leader(Some(tablet), addr, request)
                        .await
                    {
                        ClientResponse::Error(e)
                            if topology::parse_not_leader_refusal(&e).is_some() => {} // chase exhausted mid-election, retry below
                        other => return other,
                    }
                }
                Some(CpRoute::None) | None => {} // not settled yet, retry
            }
            if self.env.now() >= deadline {
                return ClientResponse::Error(
                    "stream grow: did not reach this tablet's leader in time".into(),
                );
            }
            self.env.sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Growth PR3 (ADR 0042 §14): split EVERY tablet of streamed `table` at
    /// its own byte-weighted median, in one action (`POST
    /// /admin/stream/grow`) — each child mints exactly one
    /// `ParentShardId`, so the table's shard count doubles (minus any
    /// tablet [`grow_stream_tablet`](Self::grow_stream_tablet) skips: Fork
    /// E's single-token limit, or an empty/singleton tablet). `Err` only
    /// for a request-shaped problem (the table has no stream, or no
    /// tablets at all yet); a per-tablet skip/error is reported inside the
    /// returned vector, never escalated into the whole call failing — the
    /// caller (`admin::action_stream_grow`) classifies each entry.
    pub(crate) async fn grow_stream(
        &self,
        table: &str,
    ) -> Result<Vec<(TabletId, ClientResponse)>, String> {
        let meta = self.effective_metadata();
        if meta.table_stream(table).is_none() {
            return Err(format!("table `{table}` has no stream enabled"));
        }
        let tablets: Vec<(TabletId, TabletState)> = meta
            .tablets_for_table(table)
            .map(|(id, t)| (*id, t.state))
            .collect();
        if tablets.is_empty() {
            return Err(format!("table `{table}` has no tablets yet"));
        }
        let mut results = Vec::with_capacity(tablets.len());
        for (tablet, state) in tablets {
            // A mid-split tablet is classified up front (`STREAM_GROW_MID_
            // SPLIT`), never routed to: a `Splitting` parent's workflow is
            // already running (kicking it again is an idempotent no-op that
            // the summary would miscount as a fresh split), and a `Building`
            // child refuses splits until activation anyway.
            let response = match state {
                // #454: `tablets` above is a `Metadata` snapshot, but this
                // loop's own body crosses real `await` points per iteration
                // (`grow_stream_tablet`'s resolve/propose/poll cycle) — a
                // CASCADE split can retire a tablet captured `Active` here
                // before its own turn arrives, cutting it over to two
                // (already-`Active`) children between the snapshot and the
                // lookup this iteration performs. `classify_grow_response`
                // folds that vanished-tablet lookup miss into the identical
                // skip a tablet caught `Splitting`/`Building` one beat
                // earlier already gets below — see its own doc for why this
                // is safe.
                TabletState::Active => {
                    classify_grow_response(self.grow_stream_tablet(tablet).await)
                }
                TabletState::Splitting | TabletState::Building => {
                    ClientResponse::Error(STREAM_GROW_MID_SPLIT.into())
                }
            };
            results.push((tablet, response));
        }
        Ok(results)
    }

    /// Delete `index`'s own backfill cursor row (ADR 0045 §5 step 3) on
    /// **every** tablet currently scoped to `table`, wherever each one's
    /// own leader actually runs — the table-wide sibling of
    /// [`clear_backfill_cursor_tablet`](Self::clear_backfill_cursor_tablet),
    /// called once per tablet since each tablet is its own Raft group with
    /// its own cursor row. See `dynamo.rs::drop_index`'s own doc for why
    /// this step exists (a stale cursor row would otherwise silently
    /// poison a later same-named `CreateTableIndex`'s fresh backfill) and
    /// exactly when it runs.
    pub(crate) async fn clear_backfill_cursor_for_table(
        &self,
        table: &str,
        index: &str,
    ) -> Result<(), String> {
        let tablets: Vec<TabletId> = self
            .effective_metadata()
            .tablets_for_table(table)
            .map(|(&id, _)| id)
            .collect();
        for tablet in tablets {
            self.clear_backfill_cursor_tablet(tablet, index).await?;
        }
        Ok(())
    }

    /// Delete `index`'s own backfill cursor row on one `tablet`, wherever
    /// its leader actually runs — mirrors
    /// [`force_seal_tablet`](Self::force_seal_tablet)'s per-tablet
    /// forward/retry shape exactly (a hint-chasing
    /// [`forward_to_tablet_leader`](Self::forward_to_tablet_leader) per
    /// attempt, re-resolving [`resolve_cp_route`](Self::resolve_cp_route)
    /// between chases as the converged-or-timeout backstop).
    async fn clear_backfill_cursor_tablet(
        &self,
        tablet: TabletId,
        index: &str,
    ) -> Result<(), String> {
        let deadline = self.env.now().saturating_add(SCHEMA_COMMIT_TIMEOUT);
        loop {
            match self.resolve_cp_route(tablet) {
                Some(CpRoute::Local(leader)) => {
                    return index_drain::clear_backfill_cursor(&leader, index).await;
                }
                Some(CpRoute::Forward(addr)) => {
                    let request = ClientRequest::ClearBackfillCursor {
                        tablet: tablet.0,
                        index: index.to_owned(),
                    };
                    match self
                        .forward_to_tablet_leader(Some(tablet), addr, request)
                        .await
                    {
                        ClientResponse::PutOk => return Ok(()),
                        ClientResponse::Error(e) if self.env.now() >= deadline => {
                            return Err(e);
                        }
                        ClientResponse::Error(_) => {} // retry below
                        other => {
                            return Err(format!(
                                "unexpected reply to forwarded cursor-clear: {other:?}"
                            ));
                        }
                    }
                }
                Some(CpRoute::None) | None => {} // not settled yet, retry
            }
            if self.env.now() >= deadline {
                return Err("backfill-cursor clear did not reach a tablet leader in time".into());
            }
            self.env.sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// The hot_read scope-transition latch (ADR 0044 phase-1 PR4, narrowing
    /// the ADR 0043 `hot_read` residual — see [[split-seal-duplication-bug]]
    /// and `docs/adr/0043-*.md`'s amendment on the #220 fix): refuses a
    /// hot-read retryably instead of ever risking a stale-wide answer,
    /// whenever this node's own **live** `scope_range()` for `tablet` (the
    /// exact field `animus_cp_data::host::Reconciler::tick` mutates via
    /// `narrow_scope` — see that module's doc) is currently **wider** than
    /// the tablet's range per `meta`.
    ///
    /// **`meta` must come from [`metadata_fresh`](Self::metadata_fresh),
    /// never [`effective_metadata`](Self::effective_metadata)/
    /// `metadata_cached()`.** `index_drain::hot_read`'s own pre-existing
    /// `in_declared_range` filter (2026-08-15) already checks a record's key
    /// against a caller-supplied snapshot, but every prior call site sourced
    /// that snapshot from the possibly-stale `effective_metadata()` mirror.
    /// Reading the group's own live scope needs no new shared state at all —
    /// it is always exactly current the instant the reconciler narrows it
    /// (`RaftKvNode::narrow_scope` sets it synchronously, no propagation
    /// delay) — so cross-checking it against a **freshly fetched** declared
    /// range closes two of the three staleness axes `in_declared_range`
    /// alone left open: (a) a data-only/growth node's ADR 0030 mirror
    /// lagging a `SplitTablet` commit by its own refresh interval, and (b)
    /// this node's own reconciler having observed the split in its cached
    /// `Metadata` but not yet having ticked `narrow_scope` locally.
    ///
    /// **This narrows, but does not fully close, the residual — the same
    /// layer-2 structure the #220 investigation found on the write side.**
    /// For a `ControlHandle::Local` node (every combined node — the common
    /// case), `metadata_fresh()` resolves to `raft.metadata()`, the ADR 0038
    /// published cache a **local, asynchronous control apply task**
    /// maintains, not the control Raft's own commit index directly. In the
    /// sub-window between a `SplitTablet` actually committing and this
    /// node's own control apply task catching its published cache up to it,
    /// `meta` and the live scope are stale **together**: the declared range
    /// still shows the pre-split width, so this check passes and a hot-read
    /// can still observe the fabrication class ADR 0043 describes. Full
    /// closure of this sub-window would need a per-read control-leader
    /// Fetch up to `limit` of `tablet`'s own open-shard hot records with
    /// packed HLC strictly greater than `from_position` (ADR 0042 §7/§8,
    /// PR6's `GetRecords` open-shard path) — the internal `ClientRequest::
    /// StreamHotRead` RPC, forwarded to whichever node currently leads
    /// `tablet`. Mirrors [`force_seal_tablet`](Self::force_seal_tablet)'s
    /// retry shape exactly (there is no client key to derive routing from,
    /// so each attempt is a hint-chasing
    /// [`forward_to_tablet_leader`](Self::forward_to_tablet_leader), with a
    /// fresh [`resolve_cp_route`](Self::resolve_cp_route) between chases) —
    /// acceptable for a `GetRecords` poll, which already
    /// tolerates "not there yet, poll again" as part of the stream's own
    /// eventually consistent contract.
    pub(crate) async fn read_stream_hot_records(
        &self,
        tablet: TabletId,
        from_position: u64,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let deadline = self.env.now().saturating_add(SCHEMA_COMMIT_TIMEOUT);
        loop {
            match self.resolve_cp_route(tablet) {
                Some(CpRoute::Local(leader)) => {
                    // The ADR 0048 scope-transition latch died with the
                    // mutable scope (ADR 0050 rung 7) — immutable ranges
                    // leave no transition window to latch.
                    return Ok(index_drain::hot_read(&leader, from_position, limit)
                        .await
                        .into_iter()
                        .map(|(key, _, value)| (key, value))
                        .collect());
                }
                Some(CpRoute::Forward(addr)) => {
                    let request = ClientRequest::StreamHotRead {
                        tablet: tablet.0,
                        from_position,
                        limit,
                    };
                    match self
                        .forward_to_tablet_leader(Some(tablet), addr, request)
                        .await
                    {
                        ClientResponse::Pairs(pairs) => return Ok(pairs),
                        ClientResponse::Error(e) if self.env.now() >= deadline => {
                            return Err(e);
                        }
                        ClientResponse::Error(_) => {} // retry below
                        other => {
                            return Err(format!(
                                "unexpected reply to forwarded stream hot read: {other:?}"
                            ));
                        }
                    }
                }
                Some(CpRoute::None) | None => {} // not settled yet, retry
            }
            if self.env.now() >= deadline {
                return Err("stream hot read did not reach a tablet leader in time".into());
            }
            self.env.sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }
}

/// #454: fold [`ClientCtx::grow_stream_tablet`]'s own "no such tablet"
/// lookup miss into [`STREAM_GROW_MID_SPLIT`], the identical skip
/// [`ClientCtx::grow_stream`]'s own up-front `match state` already reports
/// for a tablet caught `Splitting`/`Building` a beat earlier.
///
/// `grow_stream` walks a `Metadata` *snapshot* of a table's tablets, but
/// awaits real Raft/network activity per iteration
/// (`grow_stream_tablet`'s resolve/propose/poll cycle, or —
/// `trigger_split`'s own doc — its forwarded `TriggerAutoSplit` twin in
/// `forwarding.rs`). In a **cascade** split, a tablet snapshotted `Active`
/// can be retired by an unrelated cutover (this same `grow_stream` call's
/// own earlier tablet, or a fully independent split) before this loop
/// reaches it: `trigger_split`'s `effective_metadata().tablets.get(&tablet)`
/// then comes back `None`, and returns the literal error
/// `"no such tablet"` — indistinguishable, by string, from a tablet id that
/// never existed.
///
/// This is safe to skip, by the same reasoning `STREAM_GROW_MID_SPLIT`
/// already relies on for the up-front case: the tablet did not vanish —
/// it *just finished being split* (into two already-`Active` children),
/// so there is nothing left for this call to do to it. Nothing is lost:
/// the split is a normal committed one, so its children's `ParentShardId`
/// lineage already covers stream continuity (ADR 0042/0043) exactly as it
/// does for any other split, and — same as a `Splitting` parent's own
/// not-yet-active children today — those children simply wait for the
/// *next* `grow_stream` call to be split in their own turn, rather than
/// this one.
///
/// Deliberately narrow: only the exact `"no such tablet"` message coming
/// back from a `grow_stream_tablet` call is reclassified. Any other error
/// — including a genuine unknown-tablet request outside this walk (e.g.
/// `POST /admin/tablet/split` naming a bad id, still handled by
/// `trigger_split` itself) — passes through unchanged, since this
/// function is never on that path.
fn classify_grow_response(response: ClientResponse) -> ClientResponse {
    match response {
        ClientResponse::Error(e) if e == "no such tablet" => {
            ClientResponse::Error(STREAM_GROW_MID_SPLIT.into())
        }
        other => other,
    }
}

#[cfg(test)]
mod grow_stream_classify_tests {
    use super::classify_grow_response;
    use crate::{ClientResponse, STREAM_GROW_MID_SPLIT};

    /// #454 regression: a tablet that retired mid-walk (its own
    /// `grow_stream_tablet` call surfaces the generic "no such tablet"
    /// lookup miss) must classify as the same skip a tablet caught
    /// `Splitting`/`Building` up front already gets — never as an error.
    /// Red before the fix (the pre-#454 code path had no such mapping at
    /// all — `grow_stream`'s loop pushed `grow_stream_tablet`'s raw
    /// response straight through), green after.
    #[test]
    fn vanished_tablet_lookup_miss_classifies_as_mid_split_skip() {
        let vanished = ClientResponse::Error("no such tablet".into());
        assert_eq!(
            classify_grow_response(vanished),
            ClientResponse::Error(STREAM_GROW_MID_SPLIT.into()),
            "a tablet that retired between grow_stream's snapshot and its \
             own turn must classify identically to STREAM_GROW_MID_SPLIT"
        );
    }

    /// A genuine split (`PutOk`) passes through untouched.
    #[test]
    fn successful_split_passes_through_unchanged() {
        assert_eq!(
            classify_grow_response(ClientResponse::PutOk),
            ClientResponse::PutOk
        );
    }

    /// Every other existing skip/error message passes through unchanged —
    /// this mapping is precise about which exact message it intercepts, not
    /// a blanket swallow of every `ClientResponse::Error`.
    #[test]
    fn unrelated_errors_pass_through_unchanged() {
        for msg in [
            crate::SPLIT_KEY_NOT_TOKEN_VIABLE,
            crate::STREAM_GROW_NO_SPLIT_POINT,
            crate::STREAM_GROW_MID_SPLIT,
            "stream grow: did not reach this tablet's leader in time",
            "some unrelated failure",
        ] {
            let response = ClientResponse::Error(msg.into());
            assert_eq!(
                classify_grow_response(response.clone()),
                response,
                "message {msg:?} must not be reclassified"
            );
        }
    }
}
