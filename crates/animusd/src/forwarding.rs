//! ClientCtx's forwarding/routing cluster (ADR 0061 rung C5 step 2):
//! leader/tablet routing (`cp_route`, `resolve_cp_route`, `tablet_for`,
//! `cp_leader_hint`), one-hop forwarding (`cp_forward`,
//! `forward_to_tablet_leader`, `relay`), route/intra-addr accessors, and
//! the top-level forwarded-request dispatch (`cp_serve_forwarded`).
//! Moved verbatim out of `lib.rs`'s `impl<E: Env> ClientCtx<E>` block --
//! no logic changes.

use std::collections::{BTreeMap, BTreeSet};

use animus_cp_data::{TxnDecisionStatus, TxnOutcome};
use animus_env::{Env, NodeId};
use animus_node::host::RelayClient;
use animus_tablet::{KeyRange, TabletId};

use crate::{
    CLIENT_TIMEOUT, ClientCtx, ClientRequest, ClientResponse, CpRoute, FORWARD_ELECTION_BACKOFF,
    FORWARD_HOP_TIMEOUT, RELAY_TRANSPORT_FAILURE, SCHEMA_POLL_INTERVAL, STALE_READ_REFUSAL,
    STREAM_GROW_NO_SPLIT_POINT, SnapshotRead, TxnAbortReason, decide, dynamo, index_drain,
    median_split_key, relay_request, relay_request_with_timeout, topology,
};

impl<E: Env, R: RelayClient> ClientCtx<E, R> {
    /// The client-API address `id` currently routes to, if known (ADR 0032
    /// PR1) — a single lookup into the live [`client_route`](Self::client_route)
    /// map, kept fresh by [`route_sync_loop`]. Never holds the lock across an
    /// `.await`.
    fn route_addr(&self, id: NodeId) -> Option<String> {
        self.client_route
            .lock()
            .expect("client route poisoned")
            .get(&id)
            .cloned()
    }

    /// A clone of the whole live `client_route` map (ADR 0032 PR1), for a
    /// caller that needs to search/iterate it — cloning out under the lock
    /// keeps every subsequent lookup lock-free (and safe to hold across an
    /// `.await`).
    pub(crate) fn route_snapshot(&self) -> BTreeMap<NodeId, String> {
        self.client_route
            .lock()
            .expect("client route poisoned")
            .clone()
    }

    /// This node's own best-known control-plane leader, as `(id, client-API
    /// address)` — the `leader_hint` every `Status` reply now carries (ADR
    /// 0035 §1), so a `Remote` data node's mirror-sync/live-fetch loop can
    /// hop toward the real leader without a separate `route_addr` lookup on
    /// the *answering* side. `None` if this node doesn't currently know a
    /// leader (mid-election, or — for this node itself, if it's a growth/data
    /// node — no leader signal at all).
    pub(crate) fn control_leader_hint(&self) -> Option<(NodeId, String)> {
        let id = self.control.leader()?;
        let addr = self.route_addr(id.clone())?;
        Some((id, addr))
    }

    /// The intra-cluster RPC address `id` currently routes to (ADR 0047) — the
    /// [`route_addr`](Self::route_addr) sibling for machine-to-machine hops:
    /// `cp_leader_hint`/`other_tablet_replica_addr`/`propose_schema`'s relay
    /// all resolve a forwarding target through this, never through
    /// `route_addr`, since the receiving end (`cp_serve_forwarded`, the
    /// relayed `ProposeSchema`) is only ever reachable on the intra listener.
    /// Kept fresh by [`intra_route_sync_loop`].
    pub(crate) fn intra_addr(&self, id: NodeId) -> Option<String> {
        self.intra_route
            .lock()
            .expect("intra route poisoned")
            .get(&id)
            .cloned()
    }

    /// The [`route_snapshot`](Self::route_snapshot) sibling for the intra
    /// routing table (ADR 0047).
    pub(crate) fn intra_route_snapshot(&self) -> BTreeMap<NodeId, String> {
        self.intra_route
            .lock()
            .expect("intra route poisoned")
            .clone()
    }

    /// This node's own best-known control-plane leader, as `(id, intra
    /// address)` (ADR 0047) — the [`control_leader_hint`](Self::control_leader_hint)
    /// sibling that feeds `intra_leader_hint` on `ClientResponse::Status`/
    /// `MetadataDelta`, and `remote_metadata_watch_loop`'s own dial
    /// candidates via `RemoteControlClient::intra_leader_addr_hint`. Machine-
    /// relay-only — never surfaced to a human (see the root `CLAUDE.md`'s
    /// hint-field-conflation lesson: anything a human reads keeps using
    /// `control_leader_hint`/`leader_hint`).
    pub(crate) fn intra_control_leader_hint(&self) -> Option<(NodeId, String)> {
        let id = self.control.leader()?;
        let addr = self.intra_addr(id.clone())?;
        Some((id, addr))
    }

    /// The id of the tablet whose key range covers `key`, from this node's cached
    /// `Metadata` tablet map (the control plane's placement authority). `None` if no
    /// tablet covers it yet (the cluster is still bootstrapping its first tablet).
    ///
    /// **Table-scoped routing (ADR 0023).** Every table owns its own tablet(s):
    /// a key of table `T` is encoded `escape(T) || …` and routes to the
    /// **table-scoped tablet** (`table: Some(T)`) whose range contains it. There is
    /// no whole-keyspace fallback for table data — a table that has not yet had its
    /// tablet provisioned returns `None` (the caller waits), so a write is never
    /// silently absorbed by a catch-all tablet. A legacy `table: None` tablet may
    /// still exist in a snapshot written before scoping; it is the last-resort owner
    /// only for a **raw, non-table-prefixed** key (e.g. the plain test client),
    /// never for a table whose own tablet exists. Iteration is over a `BTreeMap`, so
    /// the choice is deterministic on every node.
    pub(crate) fn tablet_for(&self, table: &str, key: &[u8]) -> Option<TabletId> {
        // Table-scoped routing (ADR 0023): the table is the routing dimension and the
        // key is `token(pk) || escape(pk) || rk` (no table prefix). We look only at
        // `table`'s tablets and match the key's leading token against their token
        // sub-ranges. Two tables' tablets may share a token range, so we never scan
        // the global tablet map. No catch-all: a key of an unprovisioned table yields
        // `None` and the caller waits. The range-match lookup itself is pure — see
        // `topology::tablet_for_key`.
        topology::tablet_for_key(self.effective_metadata().tablets_for_table(table), key)
    }

    /// Resolve how to reach the CP group leader for an op on `key` (shared by every
    /// CP op — read/write/delete/scan — so the leader-resolution + forwarding policy
    /// lives in one place). The key first resolves to its **owning tablet** (Phase 2
    /// multi-tablet CP), then to that tablet's group:
    ///
    /// - this node hosts the tablet's current leader → serve **locally**;
    /// - this node hosts a replica that points at a **remote** leader → **forward**
    ///   there (ADR 0017 #3b);
    /// - this node hosts a replica but the group is still electing → **wait** for it
    ///   to settle (don't forward — the only "route" might be this very node, and
    ///   forwarding a CP op to a non-leader just errors; the edges must not flap
    ///   during election);
    /// - this node hosts **no** replica of the tablet → it can never serve locally,
    ///   so forward to any known route (the receiver serves iff it is the leader,
    ///   else the client retries with fresh routing);
    /// - the tablet itself is not in the map yet (bootstrap) → **wait** for it.
    pub(crate) async fn cp_route(&self, table: &str, key: &[u8]) -> CpRoute<E> {
        let deadline = self.env.now().saturating_add(CLIENT_TIMEOUT);
        loop {
            if let Some(tablet) = self.tablet_for(table, key)
                && let Some(route) = self.resolve_cp_route(tablet)
            {
                return route;
            }
            if self.env.now() >= deadline {
                return CpRoute::None;
            }
            self.env.sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// One attempt at resolving a *known* tablet's group leader to a [`CpRoute`], or
    /// `None` if it isn't settled yet (caller should wait + retry). The leader-
    /// resolution policy behind [`cp_route`](Self::cp_route) (key→tablet→leader).
    ///
    /// The branching itself — serve locally / forward-to-hint / forward-anywhere
    /// / wait — is the pure [`topology::decide_cp_route`]; this method's job is
    /// only to gather its inputs (cheaply, and lazily where a fact needs a
    /// `Metadata` deep clone) and execute the resulting decision.
    pub(crate) fn resolve_cp_route(&self, tablet: TabletId) -> Option<CpRoute<E>> {
        // ADR 0044 phase-1 PR4 (the wake-on-demand edge): wake any locally
        // registered replica of this tablet before deciding anything, so a
        // first touch on a possibly-quiesced cold group doesn't wait out its
        // own idle-detection latency on top of ordinary election-wait.
        // `RaftKvNode::wake()` is cheap and safe on every other state (not
        // quiesced, or this node isn't the leader) — an idempotent notify
        // that costs one inert extra loop iteration at worst.
        if let Some(group) = self.edge.local_cp(tablet) {
            group.wake();
        }
        let leader = self.edge.cp_leader(tablet);
        if let Some(leader) = leader {
            return Some(CpRoute::Local(leader));
        }
        // Forward only to a concrete leader *hint* a local replica gives us.
        let forward_hint = self.cp_forward_target(tablet);
        if let Some(addr) = forward_hint {
            return Some(CpRoute::Forward(addr));
        }
        // No local leader and no leader hint. Whether this node hosts *any* local
        // handle for the group is cheap (no `Metadata` clone); only fetch the
        // metadata-derived facts (`is_replica`, a fallback forward address) in the
        // one case that needs them, matching `decide_cp_route`'s own short-circuit
        // order (avoids the "re-clone `Metadata` per request" cost the wire edges
        // already learned to snapshot around).
        //
        // A registered local handle only counts as "this node hosts a replica"
        // for routing if this node's *own durable Raft config* still lists it as
        // a voter (a local, non-`Metadata` check — `CpGroup::config()`). ADR 0029
        // introduced a window where that is not true: a node moved off a tablet
        // (a healthy rebalance/repair swap) keeps its handle registered until the
        // release-GC's grace period confirms the move and erases it. Before that
        // gate closes, `local_cp` still returns `Some`, and this used to make
        // every branch below short-circuit to `Wait` forever — routing waited on
        // a group this node had already left, instead of forwarding to the
        // node(s) that actually replicate it now. A departing/stale handle must
        // fall through to the metadata-derived path below exactly as if there
        // were no local handle at all.
        //
        // A control-only node (ADR 0035 PR3, `self.data` is `None`) never hosts
        // a local handle at all (`local_group` is always `None` for it), so
        // `has_local_replica`/`is_replica` are correctly `false` without ever
        // needing a real `base_id` — this is the "zero new rejection code"
        // degrade path: a control node is just the limit case of "hosts
        // nothing," handled by the same logic every other non-replica node
        // already goes through.
        let local_group = self.edge.local_cp(tablet);
        let has_local_replica = match (&self.data, local_group) {
            (Some(data), Some(g)) => g.config().contains(&data.base_id),
            _ => false,
        };
        let (is_replica, fallback_forward) = if has_local_replica {
            (false, None)
        } else {
            let meta = self.effective_metadata();
            let replicas = meta.tablets.get(&tablet).map(|t| &t.replicas);
            let is_replica = self
                .data
                .as_ref()
                .is_some_and(|data| replicas.is_some_and(|r| r.contains(&data.base_id)));
            // Intra-flavored (ADR 0047): this is a forwarding target — the
            // receiving node's `cp_serve_forwarded` is only reachable on the
            // intra listener.
            let route = self.intra_route_snapshot();
            let fallback = replicas
                .into_iter()
                .flatten()
                .find_map(|id| route.get(id).cloned())
                .or_else(|| route.values().next().cloned());
            (is_replica, fallback)
        };
        // `has_local_leader: false` and `forward_hint: None` here are exactly the
        // facts already established by the two early returns above — `Local` is
        // therefore unreachable from this call by construction.
        if let topology::RouteDecision::Forward(addr) =
            topology::decide_cp_route(false, None, has_local_replica, is_replica, fallback_forward)
        {
            return Some(CpRoute::Forward(addr));
        }
        None
    }

    /// This node's local replica's own leader **hint** for `tablet` — `(id,
    /// client-API address)` — as it currently sees it (`leader()` hint →
    /// `client_route`). `None` if this node hosts no replica of `tablet`, the
    /// replica has no leader hint yet (mid-election), or the hinted id has no
    /// known route. The one shared lookup behind both
    /// [`cp_forward_target`](Self::cp_forward_target) (this node deciding
    /// where to route/forward *before* proposing) and a "not the leader
    /// here" refusal's embedded hint
    /// ([`topology::format_not_leader_refusal`]) — a node refusing a
    /// forwarded op always hosts *some* local replica of the tablet (that's
    /// why it was targeted), so its own knowledge of the group's leader is
    /// exactly the hint a forwarder chasing a wrong first guess needs.
    fn cp_leader_hint(&self, tablet: TabletId) -> Option<(NodeId, String)> {
        // Since ADR 0026 Stage B a tablet's CP group member id **is** simply the
        // base `raftkv` id, so the local replica's leader hint is already an
        // `intra_route` key — no more base<->member translation needed.
        // Intra-flavored (ADR 0047): the receiving end of a forward
        // (`cp_serve_forwarded`) is only ever reachable on the intra port.
        let leader = self.edge.local_cp(tablet).and_then(|n| n.leader())?;
        let addr = self.intra_addr(leader.clone())?;
        Some((leader, addr))
    }

    /// The intra-cluster address to forward a `tablet` op to — see
    /// [`cp_leader_hint`](Self::cp_leader_hint) (the caller waits rather than
    /// guessing when there is no hint yet, so it never forwards a CP op to a
    /// non-leader, including itself).
    fn cp_forward_target(&self, tablet: TabletId) -> Option<String> {
        self.cp_leader_hint(tablet).map(|(_, addr)| addr)
    }

    /// A "not the leader here" refusal for a forwarded CP op that resolved to
    /// `tablet` (or `None`, if this node couldn't even resolve which tablet
    /// the op belongs to) — enriched with this node's own
    /// [`cp_leader_hint`](Self::cp_leader_hint) for `tablet`, if it has one.
    fn not_leader_refusal(&self, tablet: Option<TabletId>) -> ClientResponse {
        let hint = tablet.and_then(|t| self.cp_leader_hint(t));
        ClientResponse::Error(topology::format_not_leader_refusal(hint))
    }

    /// Another known client-API address for `tablet`, distinct from every
    /// address already in `tried` — the fallback
    /// [`cp_forward`](Self::cp_forward)'s hinted retry chases once the
    /// refusal's own leader hint is exhausted (already tried, or absent
    /// because the refusing node's own replica was mid-election). Gathers
    /// the tablet's replicas in `Metadata` order (deterministic) and its
    /// known routes, then hands off to the pure walk,
    /// [`decide::other_tablet_replica_addr`] (ADR 0061 A6) — `None` once
    /// every known replica address has been tried (or the tablet/its route
    /// isn't known at all).
    fn other_tablet_replica_addr(
        &self,
        tablet: TabletId,
        tried: &BTreeSet<String>,
    ) -> Option<String> {
        let meta = self.effective_metadata();
        let replicas = meta.tablets.get(&tablet)?.replicas.clone();
        // Intra-flavored (ADR 0047): this is a forwarding fallback, same as
        // `cp_leader_hint` above.
        let route = self.intra_route_snapshot();
        decide::other_tablet_replica_addr(&replicas, &route, tried)
    }

    /// Forward a CP op for `(table, key)` to `addr` (wrapped so the receiver
    /// serves-or-errors, never re-forwards) and relay its reply. Carries the
    /// current span's trace context (ADR 0027) so the receiving node's
    /// handling of the forwarded op joins the same distributed trace.
    ///
    /// **Hinted retry — closes the "zero-replica blind-forward" hazard (root
    /// `CLAUDE.md`).** A node with no local replica of the op's tablet can
    /// only *guess* a first forward target among the tablet's replicas
    /// (`resolve_cp_route`'s no-local-replica fallback); previously a wrong
    /// guess errored forever, because the receiver never re-forwards
    /// (routing stays bounded to one hop by design) and this method had no
    /// better address to retry with. Now a "not the leader here" refusal
    /// carries the refusing (replica-hosting) node's own leader hint
    /// (`topology::format_not_leader_refusal`), and this is the single choke
    /// point every CP forward call goes through (all six call sites), so the
    /// retry lives here once: on a parseable not-leader refusal, retry at the
    /// hint's address if untried, else at another of the tablet's known
    /// replica addresses ([`other_tablet_replica_addr`](Self::other_tablet_replica_addr)),
    /// skipping every address already tried. Bounded to at most one pass
    /// over {hint} ∪ replicas (each address tried at most once — the
    /// tablet's replica set is small and finite) and to the overall
    /// [`CLIENT_TIMEOUT`] budget for the *whole* sequence, not per attempt,
    /// so a forwarder chasing a bad guess still fails within one hop's usual
    /// time budget rather than several multiples of it. The one-hop
    /// invariant itself is unchanged: only the *forwarder* retries; the
    /// receiver ([`cp_serve_forwarded`](Self::cp_serve_forwarded)) still only
    /// ever serves-or-refuses, never re-forwards.
    ///
    /// **Leaderless pass — wait out the election, don't give up.** When a
    /// whole pass exhausts with every candidate refusing `leader_hint=none`,
    /// the tablet's group has no elected leader *yet* — the split-child /
    /// first-provision formation window, or a leader crash mid-election —
    /// a state that resolves itself within an election timeout or two. The
    /// local-serve path already waits for exactly this
    /// (`RouteDecision::Wait`); the forwarded path now does too: back off
    /// [`FORWARD_ELECTION_BACKOFF`], clear the tried-set, and run another
    /// pass, still hard-bounded by the same overall deadline. Gated on the
    /// tablet being resolvable so an op this node can't even map to a tablet
    /// keeps failing fast instead of consuming the whole budget.
    pub(crate) async fn cp_forward(
        &self,
        table: &str,
        key: &[u8],
        addr: String,
        request: ClientRequest,
    ) -> ClientResponse {
        let tablet = self.tablet_for(table, key);
        self.forward_to_tablet_leader(tablet, addr, request).await
    }

    /// The tablet-id-addressed core of [`cp_forward`](Self::cp_forward) —
    /// the ONE hint-chasing forward implementation, shared with every
    /// internal RPC addressed by **tablet id** rather than by a client key
    /// ([`seed_child_rows`](Self::seed_child_rows), [`force_seal_tablet`](Self::force_seal_tablet),
    /// [`grow_stream_tablet`](Self::grow_stream_tablet),
    /// `clear_backfill_cursor_tablet`, [`read_stream_hot_records`](Self::read_stream_hot_records)).
    ///
    /// Those callers used to relay once and, on a "not the leader here"
    /// refusal, re-run `resolve_cp_route` from scratch — which **never
    /// converges when this node hosts no replica of the target tablet**:
    /// the no-local-replica fallback deterministically returns the same
    /// first replica address every time, that follower refuses with a
    /// leader hint every time, and the hint was thrown away every time.
    /// The split-build driver hit exactly this (ADR 0050 fork F5 places a
    /// child at fresh homes, so on a >RF-node cluster the parent's leader
    /// routinely hosts no replica of one child): seeding that child spun
    /// against the same follower forever and the split never converged,
    /// never froze, never cut over — the parent kept all its keys with two
    /// empty/half-seeded `Building` children parked beside it, indefinitely.
    /// Chasing the refusal's own embedded hint here (identically to a
    /// client-key forward) is what actually reaches the leader.
    ///
    /// **A dead first guess is chased too (issue #316), not just a "wrong
    /// but reachable" one.** The fix above only helps when the guessed
    /// candidate is alive and answers with a proper refusal — it did
    /// nothing when the candidate itself was a node that had since
    /// crashed/been killed: [`relay_request_with_timeout`] folds every
    /// transport-level failure into one plain-text sentinel
    /// ([`RELAY_TRANSPORT_FAILURE`]), which doesn't parse as a "not the
    /// leader here" refusal, so the pre-fix chase gave up on the very
    /// first unreachable hop. Since the guess is deterministic (both the
    /// no-local-replica fallback above and a refusal's own embedded hint
    /// are plain reads, not liveness-checked), every later call — the
    /// split-build driver's next tick included — reproduced the identical
    /// dead end forever: exactly `split_survives_losing_one_childs_
    /// leader_mid_build`'s reported hang (`tests/split_build.rs`), which
    /// kills a `Building` child's own leader mid-build. The fix: a
    /// transport failure now gets the identical "no hint" treatment a
    /// live-but-mid-election refusal already gets — try another known
    /// replica — rather than a terminal `return resp`.
    ///
    /// **A reachable-but-slow candidate is bounded too (issue #585), not
    /// just an outright-dead one.** The fix above only helps once a
    /// transport failure has actually happened; each hop's own transport
    /// timeout used to be `remaining` — the *entire* time left before
    /// `deadline` — handed whole to [`relay_request_with_timeout`]. A
    /// candidate that accepts the TCP connection but is merely slow (a
    /// loaded sandbox, a starved disk) or simply never answers isn't a
    /// transport failure at all until its own timeout fires — so it could
    /// consume the *whole* remaining budget on one hop, leaving the loop
    /// nothing to retry with: it would find `now >= deadline` immediately
    /// after and give up having tried exactly one replica, bimodally,
    /// under load. Each hop's timeout is now capped to
    /// [`FORWARD_HOP_TIMEOUT`] (see that constant's own doc for the sizing
    /// rationale) — `remaining.min(FORWARD_HOP_TIMEOUT)` below — so a slow
    /// candidate can eat at most one hop's worth of budget, never the
    /// whole chase's; the overall [`CLIENT_TIMEOUT`] ceiling on the whole
    /// sequence is unchanged, since the final hop before `deadline` still
    /// gets whatever (smaller) time is actually left.
    pub(crate) async fn forward_to_tablet_leader(
        &self,
        tablet: Option<TabletId>,
        addr: String,
        request: ClientRequest,
    ) -> ClientResponse {
        let deadline = self.env.now().saturating_add(CLIENT_TIMEOUT);
        let mut tried: BTreeSet<String> = BTreeSet::new();
        let mut next = addr;
        loop {
            tried.insert(next.clone());
            let remaining = deadline.duration_since(self.env.now());
            // Issue #585: cap this hop's own transport timeout to
            // `FORWARD_HOP_TIMEOUT` rather than handing it the whole
            // `remaining` budget — see `FORWARD_HOP_TIMEOUT`'s own doc and
            // this method's doc above. The final hop before `deadline`
            // still gets the smaller of the two, so the overall
            // `CLIENT_TIMEOUT` ceiling is unchanged.
            let resp = relay_request_with_timeout(
                next.clone(),
                &ClientRequest::Forwarded {
                    request: Box::new(request.clone()),
                    traceparent: crate::otel::current_traceparent(),
                },
                remaining.min(FORWARD_HOP_TIMEOUT),
            )
            .await;
            let ClientResponse::Error(e) = &resp else {
                return resp;
            };
            // A genuine "not the leader here" refusal carries a hint (or
            // `None` if the refusing replica is itself mid-election) —
            // chase it below exactly as before. A **transport** failure
            // (issue #316: the candidate itself is unreachable — e.g. it
            // was just killed) gets the identical no-hint treatment: `next`
            // was a live guess that turned out wrong in a different way,
            // but the fix is the same either way, "try another known
            // replica" — never a terminal `return resp` for either. Any
            // OTHER error is a genuine application-level failure from a
            // live, leading peer (e.g. a rejected propose) and stays
            // terminal, unchanged.
            let hint = if e.as_str() == RELAY_TRANSPORT_FAILURE {
                None
            } else if let Some(hint) = topology::parse_not_leader_refusal(e) {
                hint
            } else {
                return resp;
            };
            if self.env.now() >= deadline {
                return resp;
            }
            let candidate = hint
                .filter(|(_, a)| !tried.contains(a))
                .map(|(_, a)| a)
                .or_else(|| tablet.and_then(|t| self.other_tablet_replica_addr(t, &tried)));
            // The pure decision (ADR 0061 A6) over the already-gathered
            // candidate — see `decide::ForwardRetryStep`'s own doc.
            match decide::decide_forward_retry(candidate, tablet.is_some()) {
                decide::ForwardRetryStep::Retry(a) => next = a,
                decide::ForwardRetryStep::WaitElection => {
                    // Every known candidate refused with no leader to point
                    // at: the group is mid-election (formation window after a
                    // split/provision, or a crashed leader). Wait it out and
                    // re-run the pass — bounded by the same overall deadline.
                    let remaining = deadline.duration_since(self.env.now());
                    if remaining.is_zero() {
                        return resp;
                    }
                    self.env
                        .sleep(FORWARD_ELECTION_BACKOFF.min(remaining))
                        .await;
                    tried.clear();
                    // `next` unchanged: re-probe the same replica first — once
                    // the election completes it either serves or hints.
                }
                decide::ForwardRetryStep::GiveUp => return resp,
            }
        }
    }

    /// Send `request` to a peer node's client API over a fresh connection and
    /// return its reply (or an error on any transport failure). The cross-node
    /// relay primitive for CP forwarding (A1) and schema-DDL relay (A2). Thin
    /// wrapper over the free [`relay_request`] (ADR 0035 PR4 — extracted so
    /// [`control_handle::RemoteControlClient`], which has no `ClientCtx` of
    /// its own, can use the identical wire primitive).
    pub(crate) async fn relay(&self, addr: String, request: ClientRequest) -> ClientResponse {
        relay_request(addr, &request).await
    }

    /// Serve a **forwarded** CP op locally: this node must lead the op's tablet (it
    /// does not re-forward — bounding routing to one hop). The op's `(table, key)`
    /// resolves to its owning tablet, then to that tablet's leader on this node.
    pub(crate) async fn cp_serve_forwarded(&self, inner: ClientRequest) -> ClientResponse {
        match inner {
            ClientRequest::Put { key, value, table } => {
                let tablet = self.tablet_for(&table, &key);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match Self::cp_put_local(&leader, key, value).await {
                    Ok(()) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            ClientRequest::PutBatch { entries, table } => {
                // All entries share one tablet (the forwarder grouped by tablet), so
                // resolve the leader by the first key and serve the whole batch here.
                let Some(first) = entries.first().map(|(k, _)| k.clone()) else {
                    return ClientResponse::PutOk; // empty batch is a no-op
                };
                let tablet = self.tablet_for(&table, &first);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match Self::cp_batch_local(&leader, entries).await {
                    Ok(()) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            ClientRequest::KindWrite {
                table,
                writes,
                change_log,
            } => {
                // Every write shares one tablet (they share a partition key), so
                // resolve the leader by the first key and serve the whole entry.
                let Some(first) = writes.first().map(|(_, k, _)| k.clone()) else {
                    return ClientResponse::PutOk; // empty batch is a no-op
                };
                let tablet = self.tablet_for(&table, &first);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                // The identical confirm `cp_kind_write_raw`'s own Local arm
                // runs — never a second implementation (`cp_kind_local`'s
                // Some-base-write requirement wrongly refused a forwarded
                // whole-partition raw DELETE, whose base write is a
                // tombstone; see `cp_kind_raw_local`'s doc).
                match Self::cp_kind_raw_local(&leader, writes, change_log).await {
                    Ok(()) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            // ADR 0046 U3: the evaluate-at-leader write RPC — resolve the
            // leader by the item's own base key, recomputed here from
            // `pk`/`sk` rather than trusted from the caller (the same
            // discipline `Get`'s arm below already follows), then defer to
            // the identical leader-side evaluator `ClientCtx::
            // cp_kind_write_item`'s own `Local` branch calls in-process.
            ClientRequest::KindWriteItem {
                table,
                pk,
                sk,
                op,
                condition,
            } => {
                let key = dynamo::item_key(&pk, sk.as_ref());
                let tablet = self.tablet_for(&table, &key);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                let meta = self.effective_metadata();
                match dynamo::kind_write_item_at_leader(
                    self,
                    &leader,
                    &meta,
                    &table,
                    &pk,
                    sk.as_ref(),
                    op,
                    condition.as_ref(),
                    // `ClientRequest::KindWriteItem` is always a forwarded
                    // *client* write (ADR 0051 §7) — the TTL reaper only
                    // ever acts on a tablet it already leads, so it never
                    // forwards through this arm.
                    false,
                )
                .await
                {
                    Ok(dynamo::KindWriteOutcome::Ok {
                        old,
                        new,
                        collection_bytes,
                    }) => ClientResponse::KindWriteOk {
                        old,
                        new,
                        collection_bytes,
                    },
                    Ok(dynamo::KindWriteOutcome::ConditionFailed) => {
                        ClientResponse::ConditionFailed
                    }
                    // Preserve the error's own code across the hop (a typed
                    // evaluation error — e.g. size() on an N attribute, a
                    // real ValidationException — must not degrade to a 500
                    // just because the leader was remote); see
                    // `dynamo::encode_relayed_error`.
                    Err(e) => ClientResponse::Error(dynamo::encode_relayed_error(&e)),
                }
            }
            // ADR 0055: an eventual read is answered by whichever replica
            // of the tablet this node happens to hold — the forwarder chose
            // this node for hosting one, not for leading it. Serve-or-refuse
            // only, exactly like the strong arm below: the refusal is the
            // forwarder's signal to fall back to the linearizable path, so
            // it never re-forwards and never waits out an election.
            ClientRequest::Get {
                key,
                table,
                stale: true,
            } => {
                let Some(tablet) = self.tablet_for(&table, &key) else {
                    return ClientResponse::Error(STALE_READ_REFUSAL.into());
                };
                match self.cp_stale_local(tablet) {
                    Some(group) if group.scope_range().contains(&key) => {
                        match group.stale_get_served(&key).await {
                            Some(v) => ClientResponse::Value(v),
                            None => ClientResponse::Error(STALE_READ_REFUSAL.into()),
                        }
                    }
                    _ => ClientResponse::Error(STALE_READ_REFUSAL.into()),
                }
            }
            ClientRequest::Get {
                key,
                table,
                stale: false,
            } => {
                let tablet = self.tablet_for(&table, &key);
                match tablet.and_then(|t| self.edge.cp_leader(t)) {
                    // Read-side scope pre-check + served/absent disambiguation
                    // (ADR 0033) — the same `cp_get_local` decision as `cp_read`'s
                    // Local arm. Serve-or-error only (never re-forward, never
                    // wait): the forwarder's own retry loop re-resolves routing on
                    // a `"; retry"` error.
                    Some(leader) => match self.cp_get_local_resolving(&leader, &key).await {
                        Ok(v) => ClientResponse::Value(v),
                        Err(e) => ClientResponse::Error(e),
                    },
                    None => self.not_leader_refusal(tablet),
                }
            }
            // ADR 0018 §2, torn-pair-fix stack PR2: the non-blocking
            // single-shot analog of `Get` just above, the forwarding
            // payload behind `ClientCtx::cp_read_snapshot` — see
            // `GetSnapshot`'s own doc. Same serve-or-error discipline as
            // `Get` (never re-forward, never wait) — a still-`Pending`
            // outcome maps to `ClientResponse::Unresolved`, distinct from
            // `Get`'s own `"; retry"` `Error`, since the two callers'
            // outer loops act on those differently.
            ClientRequest::GetSnapshot { key, table } => {
                let tablet = self.tablet_for(&table, &key);
                match tablet.and_then(|t| self.edge.cp_leader(t)) {
                    Some(leader) => match self.cp_get_local_snapshot(&leader, &key).await {
                        Ok(SnapshotRead::Value(v)) => ClientResponse::Value(v),
                        Ok(SnapshotRead::Unresolved) => ClientResponse::Unresolved,
                        Err(e) => ClientResponse::Error(e),
                    },
                    None => self.not_leader_refusal(tablet),
                }
            }
            ClientRequest::Delete { key, table } => {
                let tablet = self.tablet_for(&table, &key);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match Self::cp_delete_local(&leader, key).await {
                    Ok(()) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            // ADR 0055's scan arm — same serve-or-refuse discipline as the
            // eventual `Get` above.
            ClientRequest::Scan {
                start,
                end,
                limit,
                reverse,
                table,
                stale: true,
            } => {
                let Some(tablet) = self.tablet_for(&table, &start) else {
                    return ClientResponse::Error(STALE_READ_REFUSAL.into());
                };
                let requested = KeyRange::new(start.clone(), end.clone());
                match self.cp_stale_local(tablet) {
                    Some(group) if group.scope_range().contains_range(&requested) => {
                        ClientResponse::Pairs(
                            group
                                .stale_scan(&start, end.as_deref(), limit, reverse)
                                .await,
                        )
                    }
                    _ => ClientResponse::Error(STALE_READ_REFUSAL.into()),
                }
            }
            ClientRequest::Scan {
                start,
                end,
                limit,
                reverse,
                table,
                stale: false,
            } => {
                let tablet = self.tablet_for(&table, &start);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                // Read-side scope pre-check (ADR 0033) — the same
                // `cp_scan_local` decision as `cp_scan_one`'s Local arm: a
                // scope lagging the metadata-derived scan window would
                // silently truncate results, not error.
                match Self::cp_scan_local(&leader, &start, end.as_deref(), limit, reverse).await {
                    Ok(p) => ClientResponse::Pairs(p),
                    Err(e) => ClientResponse::Error(e),
                }
            }
            // ADR 0041 §5: the LSI `Query` forwarding payload. `start`/`end`
            // resolve to one tablet by construction (the forwarder already
            // checked this in `cp_scan_kind`), so resolve the leader by
            // `start` alone.
            // ADR 0055's kind-scoped scan arm (an eventual LSI/GSI page).
            ClientRequest::KindScan {
                table,
                kind,
                start,
                end,
                limit,
                reverse,
                stale: true,
            } => {
                let Some(tablet) = self.tablet_for(&table, &start) else {
                    return ClientResponse::Error(STALE_READ_REFUSAL.into());
                };
                let requested = KeyRange::new(start.clone(), end.clone());
                match self.cp_stale_local(tablet) {
                    Some(group) if group.scope_range().contains_range(&requested) => {
                        ClientResponse::Pairs(
                            group
                                .stale_scan_kind(kind, &start, end.as_deref(), limit, reverse)
                                .await,
                        )
                    }
                    _ => ClientResponse::Error(STALE_READ_REFUSAL.into()),
                }
            }
            ClientRequest::KindScan {
                table,
                kind,
                start,
                end,
                limit,
                reverse,
                stale: false,
            } => {
                let tablet = self.tablet_for(&table, &start);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match Self::cp_scan_kind_local(
                    &leader,
                    kind,
                    &start,
                    end.as_deref(),
                    limit,
                    reverse,
                )
                .await
                {
                    Ok(p) => ClientResponse::Pairs(p),
                    Err(e) => ClientResponse::Error(e),
                }
            }
            // ADR 0042/0043 round-3 sealer PR: the force-seal RPC —
            // addressed by `tablet` directly (see the variant's own doc for
            // why there is no client key to derive it from).
            ClientRequest::ForceSeal { tablet } => {
                let tablet = TabletId(tablet);
                let Some(leader) = self.edge.cp_leader(tablet) else {
                    return self.not_leader_refusal(Some(tablet));
                };
                let table = self
                    .effective_metadata()
                    .tablets
                    .get(&tablet)
                    .and_then(|t| t.table.clone());
                let Some(table) = table else {
                    return ClientResponse::Error("no such tablet".into());
                };
                match index_drain::seal_now(self, &table, tablet, &leader).await {
                    Ok(_) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            // ADR 0059 §9, Train 3: the PITR force-seal RPC — the PITR twin
            // of `ForceSeal` just above, identical shape.
            ClientRequest::ForcePitrSeal { tablet } => {
                let tablet = TabletId(tablet);
                let Some(leader) = self.edge.cp_leader(tablet) else {
                    return self.not_leader_refusal(Some(tablet));
                };
                let table = self
                    .effective_metadata()
                    .tablets
                    .get(&tablet)
                    .and_then(|t| t.table.clone());
                let Some(table) = table else {
                    return ClientResponse::Error("no such tablet".into());
                };
                match index_drain::pitr_seal_now(self, &table, tablet, &leader).await {
                    Ok(_) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            // Growth PR3 (ADR 0042 §14): the manual-growth split-trigger
            // RPC — addressed by `tablet` directly, mirroring `ForceSeal`
            // just above. Materializes this tablet's own live pairs
            // (leader-local — only reachable once this arm confirms this
            // node hosts it) and splits at their byte-weighted median via
            // `trigger_split`, which itself applies F11 rounding and Fork
            // E's single-token skip.
            ClientRequest::TriggerAutoSplit { tablet } => {
                let tablet = TabletId(tablet);
                let Some(leader) = self.edge.cp_leader(tablet) else {
                    return self.not_leader_refusal(Some(tablet));
                };
                match median_split_key(&leader).await {
                    None => ClientResponse::Error(STREAM_GROW_NO_SPLIT_POINT.into()),
                    Some(split_key) => self.trigger_split(tablet, split_key).await,
                }
            }
            // ADR 0042 §7/§8, PR6: the open-shard hot-read RPC — addressed
            // by `tablet` directly, mirroring `ForceSeal` just above (see
            // this variant's own doc for why). Leader-local, no ReadIndex
            // barrier (F8) — `index_drain::hot_read` is the one function
            // that knows how to filter/sort/limit the tablet's own hot tail.
            ClientRequest::StreamHotRead {
                tablet,
                from_position,
                limit,
            } => {
                let tablet = TabletId(tablet);
                let Some(leader) = self.edge.cp_leader(tablet) else {
                    return self.not_leader_refusal(Some(tablet));
                };
                // The ADR 0048 scope-transition latch died with the mutable
                // scope (ADR 0050 rung 7): ranges are immutable and a split
                // retires the parent whole, so there is no transition window
                // left to latch.
                let pairs = index_drain::hot_read(&leader, from_position, limit)
                    .await
                    .into_iter()
                    .map(|(key, _, value)| (key, value))
                    .collect();
                ClientResponse::Pairs(pairs)
            }
            // ADR 0045 §5 step 3: the backfill-cursor-cleanup RPC —
            // addressed by `tablet` directly, mirroring `ForceSeal`/
            // `StreamHotRead` above (see this variant's own doc for why).
            ClientRequest::ClearBackfillCursor { tablet, index } => {
                let tablet = TabletId(tablet);
                let Some(leader) = self.edge.cp_leader(tablet) else {
                    return self.not_leader_refusal(Some(tablet));
                };
                match index_drain::clear_backfill_cursor(&leader, &index).await {
                    Ok(()) => ClientResponse::PutOk,
                    Err(e) => ClientResponse::Error(e),
                }
            }
            // ADR 0018 §2/PR4: the four internal 2PC coordinator RPCs.
            // Routed by the first write key (`TxnPrepare`) or one of `keys`
            // (`TxnResolve`) — **never** `record_key` for a non-anchor
            // participant, whose own tablet is a different table's keyspace
            // entirely (see each variant's doc). `TxnDecide`/`TxnStatus`
            // always target the anchor's own tablet, so `record_key` (which
            // lives there by construction) is the right routing key.
            ClientRequest::TxnPrepare {
                table,
                anchor,
                writes,
                conditions,
                participant_spans,
                pending_kind_writes,
            } => {
                let Some(first) = writes.first().map(|w| w.key.clone()).or_else(|| {
                    pending_kind_writes
                        .first()
                        .map(|p| dynamo::item_key(&p.pk, p.sk.as_ref()))
                }) else {
                    return ClientResponse::Error(
                        TxnAbortReason::Other("txn prepare: writes must be non-empty".into())
                            .encode(),
                    );
                };
                let tablet = self.tablet_for(&table, &first);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                // ADR 0046 U3 (PR2): the shared local-stage step also used by
                // `txn_prepare`'s own `CpRoute::Local` branch — see
                // `txn_stage_local`'s doc.
                match self
                    .txn_stage_local(
                        &leader,
                        &table,
                        anchor,
                        writes,
                        conditions,
                        participant_spans,
                        pending_kind_writes,
                    )
                    .await
                {
                    Ok((txn_id, record_key, record_table, ts, outcome)) => {
                        ClientResponse::TxnPrepared {
                            txn_id,
                            record_key,
                            record_table,
                            ts,
                            outcome,
                        }
                    }
                    // ADR 0018's 2026-08-24 `CancellationReasons` amendment
                    // (issue #374 C2b): encode the typed reason into this
                    // hop's only error channel — `txn_prepare`'s `Forward`
                    // branch decodes it back out via `TxnAbortReason::decode`.
                    Err(e) => ClientResponse::Error(e.encode()),
                }
            }
            ClientRequest::TxnDecide {
                table,
                txn_id,
                record_key,
                commit,
                min_commit_ts,
                orphan_created_ts,
            } => {
                let tablet = self.tablet_for(&table, &record_key);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                // ADR 0018 §2/PR5: resolves nothing here (the caller does,
                // uniformly, for every participant including the anchor's
                // own keys) — and reports the record's ACTUAL decision,
                // which may differ from what was proposed (a duelling
                // recovery decision may have already won). See
                // `ClientCtx::txn_decide_anchor`'s doc for the full account.
                // `orphan_created_ts` overrides `commit`/`min_commit_ts`
                // entirely — a recovery pusher that found no record at all
                // (the orphan-record fix).
                let decide_ok = if let Some(created_ts) = orphan_created_ts {
                    leader
                        .txn_abort_orphan(txn_id.clone(), record_key.clone(), created_ts)
                        .await
                        .is_some()
                } else if commit {
                    leader
                        .txn_commit_at_least(txn_id.clone(), record_key.clone(), min_commit_ts)
                        .await
                        .is_some()
                } else {
                    leader
                        .txn_abort(txn_id.clone(), record_key.clone())
                        .await
                        .is_some()
                };
                if !decide_ok {
                    return ClientResponse::Error(
                        "CP group leader moved during anchor decide; retry".into(),
                    );
                }
                match leader.txn_status_local(&record_key).await {
                    Some(TxnDecisionStatus::Committed { commit_ts }) => {
                        ClientResponse::TxnDecided {
                            outcome: TxnOutcome::Committed { commit_ts },
                        }
                    }
                    Some(TxnDecisionStatus::Aborted) => ClientResponse::TxnDecided {
                        outcome: TxnOutcome::Aborted,
                    },
                    Some(TxnDecisionStatus::Pending) => ClientResponse::Error(
                        "txn decide: record still Pending immediately after its own decide \
                         applied — protocol bug"
                            .into(),
                    ),
                    None => {
                        ClientResponse::Error("CP group leader moved after decide; retry".into())
                    }
                }
            }
            ClientRequest::TxnResolve {
                table,
                txn_id,
                record_key,
                keys,
                outcome,
            } => {
                let Some(first) = keys.first().cloned() else {
                    return ClientResponse::PutOk; // nothing to resolve
                };
                let tablet = self.tablet_for(&table, &first);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match leader.txn_resolve(txn_id, record_key, keys, outcome).await {
                    Some((_, outcome)) => ClientResponse::TxnResolved { outcome },
                    None => {
                        ClientResponse::Error("CP group leader moved during resolve; retry".into())
                    }
                }
            }
            ClientRequest::TxnStatus { table, record_key } => {
                let tablet = self.tablet_for(&table, &record_key);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match leader.txn_status_local(&record_key).await {
                    Some(status) => ClientResponse::TxnStatusReply { status },
                    None => ClientResponse::Error(
                        "CP group leader moved, or no record yet, during status query; retry"
                            .into(),
                    ),
                }
            }
            // ADR 0018 §2/PR5: the two recovery-only internal RPCs — see
            // `ClientCtx::txn_record_view`/`txn_verify`, the one callers.
            ClientRequest::TxnRecordView { table, record_key } => {
                let tablet = self.tablet_for(&table, &record_key);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match leader.txn_record_view(&record_key).await {
                    Some(view) => ClientResponse::TxnRecordViewReply { view },
                    None => ClientResponse::Error(
                        "CP group leader moved during record view query; retry".into(),
                    ),
                }
            }
            ClientRequest::TxnVerify {
                table,
                span,
                txn_id,
            } => {
                let tablet = self.tablet_for(&table, &span.start);
                let Some(leader) = tablet.and_then(|t| self.edge.cp_leader(t)) else {
                    return self.not_leader_refusal(tablet);
                };
                match leader.txn_verify_staged(&span, &txn_id).await {
                    Some(staged) => ClientResponse::TxnVerifyReply { staged },
                    None => ClientResponse::Error(
                        "CP group leader moved during txn verify; retry".into(),
                    ),
                }
            }
            // ADR 0061 rung C1 hardening (root `CLAUDE.md`'s "grep every
            // gating match site when adding a variant to a
            // replicated/forwarded command enum" lesson — the exact
            // precedent `wire::is_relayable_command` set): every remaining
            // `ClientRequest` variant is named explicitly below, in place of
            // the `_` wildcard this replaces, so a 29th variant is a compile
            // error here until someone deliberately decides whether it needs
            // a real arm above. Byte-for-byte behavior-preserving — all
            // seven fell through the old wildcard to this exact response,
            // and still do.
            //
            // None of these seven is a payload `cp_serve_forwarded` is ever
            // handed in production: each is either served directly by
            // `handle_request`'s own top-level match (never itself wrapped
            // in `Forwarded`) or would be a protocol violation if it were.
            //
            // - `Status`/`JoinInfo`: answered locally by whichever node the
            //   client happens to be connected to (`handle_request`'s own
            //   arms) — neither needs leader resolution, so neither is ever
            //   forwarded.
            // - `WatchMetadata`: served only by a genuine control-group
            //   replica via `ClientCtx::watch_metadata` (`handle_request`'s
            //   own arm) — a `Remote`-handle node rejects it outright rather
            //   than forwarding it on (see that method's own doc).
            // - `ProposeSchema`: `handle_request`'s own arm gates it
            //   (`is_relayable_command`) and reaches the control leader via
            //   `ClientCtx::relay`/`propose_schema` directly — it IS a
            //   one-hop relay mechanism, never the payload of another one.
            // - `SplitTablet`: `handle_request`'s own arm calls
            //   `ClientCtx::trigger_split` directly, which reaches the
            //   control leader through `MetaCommand::SplitTablet`'s own
            //   `is_relayable_command` relay — not through `Forwarded`.
            // - `Txn`: `handle_request`'s own arm calls `ClientCtx::cp_txn`,
            //   the 2PC coordinator — it forwards its own `TxnPrepare`/
            //   `TxnDecide`/`TxnResolve`/`TxnStatus`/`TxnRecordView`/
            //   `TxnVerify` sub-RPCs (handled by the arms above), but the
            //   top-level `Txn` request itself is never re-wrapped.
            // - `Forwarded`: a nested forward would be a second hop —
            //   `cp_serve_forwarded`'s only caller (`handle_request`'s own
            //   `Forwarded` arm) already unwrapped one, and this function
            //   never re-forwards (the documented one-hop invariant, ADR
            //   0017 #3b) — so a `Forwarded` carrying another `Forwarded`
            //   has nothing valid to do here.
            ClientRequest::Status
            | ClientRequest::Forwarded { .. }
            | ClientRequest::ProposeSchema(_)
            | ClientRequest::SplitTablet { .. }
            | ClientRequest::JoinInfo
            | ClientRequest::WatchMetadata { .. }
            | ClientRequest::Txn { .. } => {
                ClientResponse::Error("unexpected forwarded request".into())
            }
        }
    }
}
