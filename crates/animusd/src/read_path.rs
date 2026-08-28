//! ClientCtx's read-path cluster (ADR 0061 rung C5 step 2): linearizable
//! point reads/scans (`cp_read`, `cp_read_snapshot`, `cp_scan*`) and the
//! eventually-consistent (ADR 0055) read fast path. Moved verbatim out of
//! `lib.rs`'s `impl<E: Env> ClientCtx<E>` blocks -- no logic changes.

use animus_cp_data::{FastRead, IntentInfo, TxnDecisionStatus};
use animus_env::{Env, Metric};
use animus_node::host::RelayClient;
use animus_tablet::{KeyRange, TabletId};

use crate::{
    CLIENT_TIMEOUT, ClientCtx, ClientRequest, ClientResponse, CpGroup, CpRoute, ReadConsistency,
    SCHEMA_POLL_INTERVAL, STALE_READ_FORWARD_TIMEOUT, SnapshotRead, decide,
    relay_request_with_timeout,
};

impl<E: Env, R: RelayClient> ClientCtx<E, R> {
    /// The local replica this node may serve an **eventually-consistent**
    /// read of `tablet` from (ADR 0055), or `None` if it may not.
    ///
    /// Three conditions, all local and all cheap:
    ///
    /// - this node has a data role at all (a control-only node hosts no
    ///   tablet, ADR 0035);
    /// - the local handle is a voter in the group's **own durable Raft
    ///   config** — the identical check
    ///   [`resolve_cp_route`](Self::resolve_cp_route) makes, and for the
    ///   identical reason: a node moved off a tablet by a rebalance keeps
    ///   its handle registered until the release-GC erases it (ADR 0029),
    ///   and that departing handle's engine is not this tablet's state to
    ///   serve;
    /// - the replica passes [`RaftKvNode::stale_read_ready`] — it knows a
    ///   leader and its engine holds everything it knows to be committed.
    ///
    /// **Deliberately does not `wake()` the group**, unlike
    /// `resolve_cp_route`'s wake-on-demand edge (ADR 0048 PR4). An eventual
    /// read needs no Raft activity whatsoever, and a quiesced group is idle
    /// by construction — hence fully applied, hence exactly as current as it
    /// will ever be. Waking a fleet's worth of cold groups to serve reads
    /// that do not need them waking is precisely the cost ADR 0044's
    /// cheap-groups roadmap exists to avoid.
    /// Count one eventually-consistent read's outcome (ADR 0055, ADR 0015).
    ///
    /// Silently a no-op on a control-only node, which has no data-role
    /// metrics sink — and no replicas either, so it can only ever record
    /// fallbacks. `self.data()` would panic there; `resolve_cp_route`'s own
    /// rule (this path must never panic) applies just as much to counting as
    /// to routing.
    fn record_eventual_read(&self, metric: Metric) {
        if let Some(data) = self.data.as_ref() {
            data.raftkv_metrics.incr(metric);
        }
    }

    pub(crate) fn cp_stale_local(&self, tablet: TabletId) -> Option<CpGroup<E>> {
        let data = self.data.as_ref()?;
        let group = self.edge.local_cp(tablet)?;
        (group.config().contains(&data.base_id) && group.stale_read_ready()).then_some(group)
    }

    /// Where to send an eventually-consistent read this node cannot serve
    /// itself (ADR 0055): **any** replica of `tablet`, deliberately not its
    /// leader.
    ///
    /// This is the one place the eventual path's routing genuinely differs
    /// in kind rather than in cost from [`cp_forward_target`](Self::cp_forward_target):
    /// there is no leader to resolve, nothing to hint at, and nothing to
    /// chase — every voter holds an answer this read is allowed to return.
    /// Intra-flavored (ADR 0047), like every forwarding target: the
    /// receiving node's `cp_serve_forwarded` is only reachable there.
    ///
    /// Picks the first **other** replica with a known intra address, in
    /// `NodeId` order, which is deterministic on every node. This node is
    /// excluded deliberately: this is only reached after
    /// [`cp_stale_local`](Self::cp_stale_local) already declined, so relaying
    /// to ourselves would spend a round trip re-deriving the identical
    /// refusal.
    ///
    /// It deliberately does **not** spread a table's forwarded eventual reads
    /// across its replicas — read-spreading here comes from clients reaching
    /// different nodes, each answering locally, not from a coordinator fanning
    /// out. A replica-picking policy (latency, load) is a later question and a
    /// bigger one; this returns a correct, stable answer until it is asked.
    fn cp_stale_forward_target(&self, tablet: TabletId) -> Option<String> {
        let meta = self.effective_metadata();
        let replicas = &meta.tablets.get(&tablet)?.replicas;
        let me = self.data.as_ref().map(|d| &d.base_id);
        let route = self.intra_route_snapshot();
        replicas
            .iter()
            .filter(|id| Some(*id) != me)
            .find_map(|id| route.get(id).cloned())
    }

    /// One-shot `Forwarded` relay for an eventually-consistent read (ADR
    /// 0055).
    ///
    /// Deliberately **not** [`forward_to_tablet_leader`](Self::forward_to_tablet_leader):
    /// that function's whole job is chasing a group's leader through
    /// not-the-leader refusals and election backoff, and an eventual read
    /// has no leader to chase — a refusal from the replica it asked means
    /// "not cheaply, then", which is a fallback signal, not something to
    /// retry. One connection, one reply, [`STALE_READ_FORWARD_TIMEOUT`],
    /// no retries, no waiting out an election.
    async fn relay_stale_read(&self, addr: String, request: ClientRequest) -> ClientResponse {
        relay_request_with_timeout(
            addr,
            &ClientRequest::Forwarded {
                request: Box::new(request),
                traceparent: crate::otel::current_traceparent(),
            },
            STALE_READ_FORWARD_TIMEOUT,
        )
        .await
    }

    /// One attempt at serving an **eventually-consistent** point read of
    /// `key` cheaply (ADR 0055) — locally if this node holds a serveable
    /// replica, else one forwarded hop to a replica that might.
    ///
    /// `None` means "not served cheaply"; the caller
    /// ([`cp_read`](Self::cp_read)) falls through to the linearizable path,
    /// which is always correct. `Some(v)` is a served answer, with the
    /// inner `Option` carrying genuine presence/absence exactly as
    /// [`RaftKvNode::stale_get_served`] defines it.
    async fn cp_read_eventual(&self, table: &str, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let served = self.cp_read_eventual_inner(table, key).await;
        if served.is_none() {
            self.record_eventual_read(Metric::CpEventualReadsFellBack);
        }
        served
    }

    /// [`cp_read_eventual`](Self::cp_read_eventual)'s body, split out only so
    /// the fallback counter has exactly one place to live rather than one per
    /// `return None`.
    async fn cp_read_eventual_inner(&self, table: &str, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let tablet = self.tablet_for(table, key)?;
        if let Some(group) = self.cp_stale_local(tablet) {
            // The same read-side scope pre-check the linearizable local arm
            // makes (ADR 0033): routing that has raced a split crossover
            // must fall back, never answer from a scope that does not own
            // the key.
            if !group.scope_range().contains(key) {
                return None;
            }
            let served = group.stale_get_served(key).await?;
            self.record_eventual_read(Metric::CpEventualReadsLocal);
            return Some(served);
        }
        let addr = self.cp_stale_forward_target(tablet)?;
        let request = ClientRequest::Get {
            key: key.to_vec(),
            table: table.to_owned(),
            stale: true,
        };
        match self.relay_stale_read(addr, request).await {
            ClientResponse::Value(v) => {
                self.record_eventual_read(Metric::CpEventualReadsForwarded);
                Some(v)
            }
            _ => None,
        }
    }

    /// [`cp_read_eventual`](Self::cp_read_eventual)'s scan twin — one
    /// attempt at serving one tablet's share of an eventually-consistent
    /// base-scope range scan (ADR 0055). `None` falls back to
    /// [`cp_scan_one`](Self::cp_scan_one)'s linearizable loop.
    async fn cp_scan_one_eventual(
        &self,
        table: &str,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        let tablet = self.tablet_for(table, start)?;
        if let Some(group) = self.cp_stale_local(tablet) {
            // The scan-side scope pre-check (ADR 0033, `cp_scan_local`'s own
            // rationale): a scope narrower than the requested window would
            // silently truncate the page rather than error, so fall back
            // instead of serving a short answer.
            let requested = KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec));
            if !group.scope_range().contains_range(&requested) {
                return None;
            }
            return Some(group.stale_scan(start, end, limit, reverse).await);
        }
        let addr = self.cp_stale_forward_target(tablet)?;
        let request = ClientRequest::Scan {
            start: start.to_vec(),
            end: end.map(<[u8]>::to_vec),
            limit,
            reverse,
            table: table.to_owned(),
            stale: true,
        };
        match self.relay_stale_read(addr, request).await {
            ClientResponse::Pairs(p) => Some(p),
            _ => None,
        }
    }

    /// [`cp_scan_one_eventual`](Self::cp_scan_one_eventual)'s **kind-scoped**
    /// sibling (ADR 0041 §3 scopes) — one tablet's share of an
    /// eventually-consistent LSI `Query`/`Scan`. `None` falls back to
    /// [`cp_scan_kind_one`](Self::cp_scan_kind_one)'s linearizable loop.
    async fn cp_scan_kind_one_eventual(
        &self,
        table: &str,
        kind: u8,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        let tablet = self.tablet_for(table, start)?;
        if let Some(group) = self.cp_stale_local(tablet) {
            let requested = KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec));
            if !group.scope_range().contains_range(&requested) {
                return None;
            }
            self.record_eventual_read(Metric::CpEventualReadsLocal);
            return Some(
                group
                    .stale_scan_kind(kind, start, end, limit, reverse)
                    .await,
            );
        }
        let addr = self.cp_stale_forward_target(tablet)?;
        let request = ClientRequest::KindScan {
            table: table.to_owned(),
            kind,
            start: start.to_vec(),
            end: end.map(<[u8]>::to_vec),
            limit,
            reverse,
            stale: true,
        };
        match self.relay_stale_read(addr, request).await {
            ClientResponse::Pairs(p) => {
                self.record_eventual_read(Metric::CpEventualReadsForwarded);
                Some(p)
            }
            _ => None,
        }
    }

    /// As [`cp_get_local`](Self::cp_get_local), but additionally chases a
    /// **foreign intent** (ADR 0018 §2/PR4 — a multi-participant
    /// transaction's intent whose covering record lives on a *different*
    /// tablet, so this replica has no local copy to resolve against): tries
    /// the non-blocking [`RaftKvNode::linearizable_get_served_fast`] first;
    /// on `Foreign`, routes a [`ClientCtx::txn_status`] query to the
    /// record's actual owner and, once decided, finishes the read via
    /// [`RaftKvNode::resolve_intent_given_status`] — the exact round trip
    /// `foreign_intent_resolves_via_the_anchor_records_status` (`animus-cp-
    /// data`'s `tests/txn_multi.rs`) proves at the primitive level.
    ///
    /// **ADR 0018 §2/PR5 (lifting PR4's deferral)**: a still-`Pending`
    /// status (or a failed status query — the same "can't confirm, treat
    /// conservatively" posture) no longer immediately reports "retry" —
    /// this pushes the transaction via [`txn_recover`](Self::txn_recover)
    /// first. `txn_recover` itself declines (returns `Pending`, unchanged
    /// behavior) while the record hasn't sat `Pending` past
    /// [`animus_cp_data::RECOVERY_GRACE`] yet — a still-live coordinator's
    /// ordinary in-flight commit is never disturbed by this.
    ///
    /// Falls back to the bounded local wait
    /// ([`cp_get_local`](Self::cp_get_local)) for a **locally**-`Pending`
    /// intent (the single-participant/anchor case, unchanged from PR3 — the
    /// background `txn_resolver_loop`, not this synchronous read path, is
    /// what eventually pushes a stale local record) and for the
    /// still-undecided foreign case after a declined push (the caller's own
    /// retry loop — `cp_read`'s `"; retry"` handling — tries again).
    pub(crate) async fn cp_get_local_resolving(
        &self,
        leader: &CpGroup<E>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, String> {
        if !leader.scope_range().contains(key) {
            return Err(format!(
                "key {key:?} outside tablet's current range (stale routing, likely a split crossover); retry"
            ));
        }
        match leader.linearizable_get_served_fast(key).await {
            Some(FastRead::Value(v)) => Ok(v),
            // Deliberately still the *blocking* chase, unchanged from PR3
            // (ADR 0018 §2, torn-pair-fix stack PR2's own doc): correct for
            // a genuinely single-key read (`cp_read`/plain `GetItem`), where
            // waiting out a contended local intent is the right behavior —
            // never for a `TransactGetItems` round, which uses
            // `cp_get_local_snapshot` below instead. `info` (now carried by
            // this variant, ADR 0018 §2 amendment) is unused on this arm;
            // see `cp_get_local_snapshot` for the single-shot alternative
            // that *does* need it.
            Some(FastRead::Pending(_)) => match leader.linearizable_get_served(key).await {
                Some(v) => Ok(v),
                None => Err("CP group leader moved; retry".into()),
            },
            Some(FastRead::Foreign(info)) => {
                let status = self.confirm_or_push(&info).await;
                match status {
                    TxnDecisionStatus::Committed { .. } | TxnDecisionStatus::Aborted => {
                        match leader
                            .resolve_intent_given_status(key, &info.txn_id, status)
                            .await
                        {
                            Some(v) => Ok(v),
                            None => Err("transaction resolution race; retry".into()),
                        }
                    }
                    TxnDecisionStatus::Pending => {
                        Err("transaction covering this key is still pending; retry".into())
                    }
                }
            }
            None => Err("CP group leader moved; retry".into()),
        }
    }

    /// The shared "confirm-or-push" step behind both
    /// [`cp_get_local_resolving`](Self::cp_get_local_resolving)'s
    /// foreign-intent arm and [`cp_get_local_snapshot`](Self::cp_get_local_snapshot)
    /// (ADR 0018 §2, torn-pair-fix stack PR2): a single status query for
    /// the transaction `info` describes, routed to its actual owner
    /// (`ClientCtx::txn_status`, transparently local when `info.record_key`
    /// happens to resolve back to this same tablet — see [`IntentInfo`]'s
    /// updated doc). A still-`Pending` status (or a failed query — the same
    /// "can't confirm, treat conservatively" posture) pushes it once via
    /// [`txn_recover`](Self::txn_recover) before giving up (`txn_recover`
    /// itself declines, returning `Pending` unchanged, while the record
    /// hasn't sat `Pending` past [`animus_cp_data::RECOVERY_GRACE`] yet — a
    /// still-live coordinator's ordinary in-flight commit is never
    /// disturbed). Never retries past that single push — the two callers
    /// differ only in what they do with a still-`Pending` result
    /// afterwards (one reports a retryable error for `cp_read`'s own outer
    /// loop to chase; the other reports "unresolved this instant" for a
    /// quiescent round to discard).
    async fn confirm_or_push(&self, info: &IntentInfo) -> TxnDecisionStatus {
        match self.txn_status(&info.record_table, &info.record_key).await {
            Ok(TxnDecisionStatus::Pending) | Err(_) => match self
                .txn_recover(
                    &info.record_table,
                    &info.record_key,
                    &info.txn_id,
                    Some(info.version),
                )
                .await
            {
                Ok(s) => s,
                Err(_) => TxnDecisionStatus::Pending,
            },
            Ok(s) => s,
        }
    }

    /// Non-blocking, single-shot analog of
    /// [`cp_get_local_resolving`](Self::cp_get_local_resolving) — the read
    /// primitive `TransactGetItems`'s quiescent round needs (ADR 0018 §2,
    /// torn-pair-fix stack PR2): every branch below makes **exactly one**
    /// resolution attempt, never a per-key wait/retry, so every key of a
    /// round samples at approximately the same instant regardless of
    /// whether its own intent happened to be local or foreign — the
    /// asymmetry `cp_get_local_resolving` deliberately keeps (a correct,
    /// intentional design for a genuinely single-key read) is exactly what
    /// let a `TransactGetItems` round accept a torn snapshot: seed
    /// `[`FastRead::Pending`]'s bounded *blocking* chase against
    /// `[`FastRead::Foreign`]'s *immediate*-give-up-and-outer-retry shape,
    /// and under a tight back-to-back writer the two keys of one round
    /// systematically sample different instants — a corpus/production
    /// reproduction that stabilized as a genuine, repeatable failure (see
    /// `docs/engineering-lessons.md`'s Testing entries on this
    /// investigation, and the ADR 0018 §2 amendment for the full account).
    ///
    /// Both `Pending` and `Foreign` now carry the identical [`IntentInfo`]
    /// shape (this same amendment), so [`confirm_or_push`](Self::confirm_or_push)
    /// handles them in one arm: one status query (transparently local or
    /// cross-tablet) plus, if still `Pending`, one push attempt — never a
    /// second query, never a sleep-and-retry. A still-undecided outcome, or
    /// a resolve landing on a race (something else already resolved this
    /// exact key underneath), maps to [`SnapshotRead::Unresolved`] rather
    /// than the retryable `"; retry"` error `cp_get_local_resolving` would
    /// report — this function's caller (the round loop, not this call)
    /// decides what "unresolved" means.
    pub(crate) async fn cp_get_local_snapshot(
        &self,
        leader: &CpGroup<E>,
        key: &[u8],
    ) -> Result<SnapshotRead, String> {
        if !leader.scope_range().contains(key) {
            return Err(format!(
                "key {key:?} outside tablet's current range (stale routing, likely a split crossover); retry"
            ));
        }
        match leader.linearizable_get_served_fast(key).await {
            Some(FastRead::Value(v)) => Ok(SnapshotRead::Value(v)),
            Some(FastRead::Pending(info)) | Some(FastRead::Foreign(info)) => {
                let status = self.confirm_or_push(&info).await;
                match status {
                    TxnDecisionStatus::Committed { .. } | TxnDecisionStatus::Aborted => {
                        match leader
                            .resolve_intent_given_status(key, &info.txn_id, status)
                            .await
                        {
                            Some(v) => Ok(SnapshotRead::Value(v)),
                            // A resolution race (something else resolved or
                            // overwrote this key between the status query
                            // and here) — not sampled cleanly this instant,
                            // never a hard error; the round loop retries.
                            None => Ok(SnapshotRead::Unresolved),
                        }
                    }
                    TxnDecisionStatus::Pending => Ok(SnapshotRead::Unresolved),
                }
            }
            None => Err("CP group leader moved; retry".into()),
        }
    }

    /// Non-blocking analog of [`cp_read`](Self::cp_read), backing
    /// `TransactGetItems`'s quiescent round (`dynamo::quiescent_multi_get`,
    /// ADR 0018 §2, torn-pair-fix stack PR2): routing/leadership failures
    /// ("leader moved", stale scope) are retried internally exactly like
    /// `cp_read` — bounded by [`CLIENT_TIMEOUT`], the same routing
    /// discipline every CP primitive shares — since those are never a
    /// meaningful round-level signal; only an unresolved intent
    /// ([`SnapshotRead::Unresolved`]) is surfaced to the caller, since only
    /// the round loop (never this per-key call) may retry on that.
    pub(crate) async fn cp_read_snapshot(
        &self,
        table: &str,
        key: Vec<u8>,
    ) -> Result<SnapshotRead, String> {
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            let err = match self.cp_route(table, &key).await {
                CpRoute::Local(leader) => match self.cp_get_local_snapshot(&leader, &key).await {
                    Ok(outcome) => return Ok(outcome),
                    Err(e) => e,
                },
                CpRoute::Forward(addr) => {
                    match self
                        .cp_forward(
                            table,
                            &key,
                            addr,
                            ClientRequest::GetSnapshot {
                                key: key.clone(),
                                table: table.to_owned(),
                            },
                        )
                        .await
                    {
                        ClientResponse::Value(v) => return Ok(SnapshotRead::Value(v)),
                        ClientResponse::Unresolved => return Ok(SnapshotRead::Unresolved),
                        ClientResponse::Error(e) => e,
                        other => {
                            return Err(format!(
                                "unexpected reply to forwarded CP snapshot read: {other:?}"
                            ));
                        }
                    }
                }
                CpRoute::None => return Err("no CP group leader reachable".into()),
            };
            if !decide::read_should_retry(&err) || tokio::time::Instant::now() >= deadline {
                return Err(err);
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Serve a linearizable **scan** on a known-leader local handle, enforcing
    /// the read-side scope pre-check — the scan flavor of
    /// [`cp_get_local`](Self::cp_get_local): `linearizable_scan` filters every
    /// row through the group's live scope (`strip_in_range`), so a scope that
    /// has not yet caught up to the metadata-derived request window (a
    /// split's narrow in flight) would **silently truncate** the results
    /// rather than error. Shared by [`cp_scan_one`] and `cp_serve_forwarded`'s
    /// `Scan` arm.
    pub(crate) async fn cp_scan_local(
        leader: &CpGroup<E>,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let requested = KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec));
        if !leader.scope_range().contains_range(&requested) {
            return Err(format!(
                "scan window {requested:?} outside tablet's current range (stale routing, likely a split crossover); retry"
            ));
        }
        // Same range, same barrier either way — `reverse` only decides which
        // end of it `limit` keeps and what order the rows come back in.
        let served = if reverse {
            leader.linearizable_scan_rev(start, end, limit).await
        } else {
            leader.linearizable_scan(start, end, limit).await
        };
        match served {
            Some(p) => Ok(p),
            None => Err("CP group leader moved; retry".into()),
        }
    }

    /// Linearizable CP **read** of `key` (ADR 0017): ReadIndex on the group leader,
    /// forwarded to the leader's node if this node isn't it. `Ok(None)` is an
    /// absent key — and **only** a genuinely served absent (ADR 0033 read-path
    /// fix): a read-barrier failure (deposed/mid-election leader) is a
    /// retryable condition, never reported as absence, and a leader whose live
    /// `scope_range()` does not contain `key` (this node's routing raced a
    /// split's narrow — metadata says the group owns the
    /// key, its scope hasn't caught up) is likewise retried until routing and
    /// scope agree, mirroring the write side's pre-propose range check. `Err`
    /// is "no leader reachable / did not become serveable in time". The CP
    /// read primitive the wire edges call directly.
    pub(crate) async fn cp_read(
        &self,
        table: &str,
        key: Vec<u8>,
        consistency: ReadConsistency,
    ) -> Result<Option<Vec<u8>>, String> {
        // ADR 0055: try the cheap path first for a `ConsistentRead: false`
        // read, and fall straight through to the linearizable loop below
        // when no replica can serve it. The strong path is untouched — a
        // `Strong` read compiles down to exactly what it always did.
        if consistency.is_eventual()
            && let Some(v) = self.cp_read_eventual(table, &key).await
        {
            return Ok(v);
        }
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            let err = match self.cp_route(table, &key).await {
                CpRoute::Local(leader) => match self.cp_get_local_resolving(&leader, &key).await {
                    Ok(v) => return Ok(v),
                    Err(e) => e,
                },
                CpRoute::Forward(addr) => {
                    match self
                        .cp_forward(
                            table,
                            &key,
                            addr,
                            ClientRequest::Get {
                                key: key.clone(),
                                table: table.to_owned(),
                                stale: false,
                            },
                        )
                        .await
                    {
                        ClientResponse::Value(v) => return Ok(v),
                        ClientResponse::Error(e) => e,
                        other => {
                            return Err(format!(
                                "unexpected reply to forwarded CP read: {other:?}"
                            ));
                        }
                    }
                }
                CpRoute::None => return Err("no CP group leader reachable".into()),
            };
            if !decide::read_should_retry(&err) || tokio::time::Instant::now() >= deadline {
                return Err(err);
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Linearizable CP range **scan** of `table` over `[start, end)` up to `limit`
    /// keys (ADR 0017/0023): a **per-table fan-out**. The scan is split across the
    /// `table`'s tablets whose token sub-range overlaps `[start, end)` (token order),
    /// each scanned on its own group leader (ReadIndex, forwarded if this node isn't
    /// it) and merged — so the result is in token order, the only order a hash ring
    /// offers. A freshly created table has a single whole-ring tablet, so the loop
    /// runs once; a split table fans out across its halves.
    pub(crate) async fn cp_scan(
        &self,
        table: &str,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: Option<usize>,
        reverse: bool,
        consistency: ReadConsistency,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        // The table's tablets overlapping [start, end), in token (range.start) order.
        // `end == None` is unbounded above (a whole-table scan).
        //
        // `effective_metadata()`, not `self.control.metadata_cached()`
        // directly (ADR 0035 PR5 staleness-audit fix): the latter is
        // permanently empty on a control-plane-follower-less growth node
        // (ADR 0030), which would silently compute zero overlapping ranges
        // and make `cp_scan` return an empty result forever on such a
        // node — the exact staleness class `cp_put`/`cp_get`/`cp_batch_write`'s
        // `has_table_tablet` gate already guards against, just missed here.
        let mut ranges: Vec<KeyRange> = self
            .effective_metadata()
            .tablets_for_table(table)
            // ADR 0050: a `Building` split child overlaps its still-serving
            // parent — scanning both would double-serve (or serve a
            // half-copied engine's slice of) the overlap.
            .filter(|(_, t)| t.is_routable())
            .map(|(_, t)| t.range.clone())
            .filter(|r| {
                // [r.start, r.end) overlaps [start, end), each upper bound optional.
                end.as_deref().is_none_or(|e| r.start.as_slice() < e)
                    && r.end.as_deref().is_none_or(|re| start.as_slice() < re)
            })
            .collect();
        ranges.sort();
        // Descending: visit the overlapping tablets highest-token-first too,
        // so `limit` fills from the top of the whole scanned span rather than
        // from the top of merely its lowest tablet.
        if reverse {
            ranges.reverse();
        }
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for r in ranges {
            if let Some(l) = limit
                && out.len() >= l
            {
                break;
            }
            // Clip the scan window to this tablet's sub-range; the exclusive upper
            // bound is the lesser of the tablet's end and the scan's end (None = ∞).
            let sub_start = start.clone().max(r.start);
            let sub_end: Option<Vec<u8>> = match (r.end, &end) {
                (None, e) => e.clone(),
                (Some(re), None) => Some(re),
                (Some(re), Some(e)) => Some(re.min(e.clone())),
            };
            if let Some(se) = &sub_end
                && sub_start.as_slice() >= se.as_slice()
            {
                continue;
            }
            let remaining = limit.map(|l| l - out.len());
            out.extend(
                self.cp_scan_one(table, sub_start, sub_end, remaining, reverse, consistency)
                    .await?,
            );
        }
        if let Some(l) = limit {
            out.truncate(l);
        }
        Ok(out)
    }

    /// Scan a single tablet's sub-range on its group leader (the body the fan-out
    /// [`cp_scan`](Self::cp_scan) calls per overlapping tablet). `start` resolves to
    /// exactly one tablet of `table`, so it routes/forwards like any other CP op.
    /// `end == None` is unbounded above (the last tablet of a whole-table scan).
    async fn cp_scan_one(
        &self,
        table: &str,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: Option<usize>,
        reverse: bool,
        consistency: ReadConsistency,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        // ADR 0055: the cheap path, per tablet — a fan-out falls back only
        // for the sub-ranges no replica could serve, never wholesale.
        if consistency.is_eventual()
            && let Some(p) = self
                .cp_scan_one_eventual(table, &start, end.as_deref(), limit, reverse)
                .await
        {
            return Ok(p);
        }
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            let err = match self.cp_route(table, &start).await {
                CpRoute::Local(leader) => {
                    match Self::cp_scan_local(&leader, &start, end.as_deref(), limit, reverse).await
                    {
                        Ok(p) => return Ok(p),
                        Err(e) => e,
                    }
                }
                CpRoute::Forward(addr) => {
                    let request = ClientRequest::Scan {
                        start: start.clone(),
                        end: end.clone(),
                        limit,
                        reverse,
                        table: table.to_owned(),
                        stale: false,
                    };
                    match self.cp_forward(table, &start, addr, request).await {
                        ClientResponse::Pairs(p) => return Ok(p),
                        ClientResponse::Error(e) => e,
                        other => {
                            return Err(format!(
                                "unexpected reply to forwarded CP scan: {other:?}"
                            ));
                        }
                    }
                }
                CpRoute::None => return Err("no CP group leader reachable".into()),
            };
            if !decide::read_should_retry(&err) || tokio::time::Instant::now() >= deadline {
                return Err(err);
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Linearizable CP range scan of one of `table`'s non-base row-kind
    /// scopes over `[start, end)` (ADR 0041 §3/§5) — the LSI `Query` read
    /// primitive. **Not** a per-table fan-out like [`cp_scan`](Self::cp_scan):
    /// an LSI query is scoped to one base partition, which is one tablet by
    /// construction (the same tablet the base row itself lives on), so
    /// `start` and `end` must resolve to that same tablet — checked here
    /// rather than assumed, mirroring [`cp_kind_write`](Self::cp_kind_write)'s
    /// cross-tablet guard: silently scanning only the first tablet's share of
    /// a straddling range would be a silent partial read. `limit` is pushed
    /// down to [`cp_scan_kind_one`](Self::cp_scan_kind_one) — the LSI `Query`
    /// pagination primitive (`animusd::dynamo`'s bounded, windowed
    /// `paginated_kind_examine_one`) now pages the same way a base/GSI
    /// `Query` does, rather than the `None`-always gap this used to have.
    #[allow(clippy::too_many_arguments)] // one kind-scoped Query's full shape
    pub(crate) async fn cp_scan_kind(
        &self,
        table: &str,
        kind: u8,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: Option<usize>,
        reverse: bool,
        consistency: ReadConsistency,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let start_tablet = self
            .tablet_for(table, &start)
            .ok_or_else(|| format!("no tablet owns the kind-scan start of table `{table}`"))?;
        if self.tablet_for(table, &end) != Some(start_tablet) {
            return Err(format!(
                "kind-scan range of table `{table}` spans more than one tablet; \
                 an LSI query is scoped to one partition"
            ));
        }
        self.cp_scan_kind_one(table, kind, start, Some(end), limit, reverse, consistency)
            .await
    }

    /// A **table-wide fan-out** of the kind-scoped scan (ADR 0041 §5) — the
    /// LSI `Scan` read primitive. Unlike [`cp_scan_kind`](Self::cp_scan_kind)'s
    /// single-tablet routing (an LSI `Query` is scoped to one base partition,
    /// hence one tablet by construction), a table-wide `Scan` against an LSI
    /// sweeps every tablet of `table`'s own ring in token order — mirroring
    /// [`cp_scan`](Self::cp_scan)'s per-table fan-out exactly, but scanning
    /// each overlapping tablet's `kind`-scoped scope instead of its base
    /// scope. `end == None` is unbounded above (a whole-table scan); the one
    /// tablet whose *own* metadata range end is also `None` (an unsplit or
    /// not-yet-split tail tablet) is asked to scan `[sub_start, None)` too —
    /// no finite byte string can bound a kind scope's logical keyspace in
    /// general (see [`RaftKvNode::linearizable_scan_kind`]'s doc), so that
    /// bound is derived inside the primitive itself, not computed here.
    pub(crate) async fn cp_scan_kind_table(
        &self,
        table: &str,
        kind: u8,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: Option<usize>,
        consistency: ReadConsistency,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        // The table's tablets overlapping [start, end), in token order — the
        // identical range math `cp_scan` uses (see that method's doc for the
        // `effective_metadata()` staleness-audit rationale, which applies
        // here unchanged).
        let mut ranges: Vec<KeyRange> = self
            .effective_metadata()
            .tablets_for_table(table)
            // ADR 0050: skip `Building` children (see `cp_scan`'s own filter).
            .filter(|(_, t)| t.is_routable())
            .map(|(_, t)| t.range.clone())
            .filter(|r| {
                end.as_deref().is_none_or(|e| r.start.as_slice() < e)
                    && r.end.as_deref().is_none_or(|re| start.as_slice() < re)
            })
            .collect();
        ranges.sort();
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for r in ranges {
            if let Some(l) = limit
                && out.len() >= l
            {
                break;
            }
            let sub_start = start.clone().max(r.start);
            let sub_end: Option<Vec<u8>> = match (r.end, &end) {
                (None, e) => e.clone(),
                (Some(re), None) => Some(re),
                (Some(re), Some(e)) => Some(re.min(e.clone())),
            };
            if let Some(se) = &sub_end
                && sub_start.as_slice() >= se.as_slice()
            {
                continue;
            }
            // Per-tablet cap (ADR 0041 §5 as-built) — the identical
            // `remaining` math `cp_scan` applies across its own tablets: how
            // many more rows this table-wide fan-out still needs after what
            // prior tablets already contributed. Threaded into the
            // `KindScan` request so a tablet with far more matching rows
            // than `remaining` doesn't ship its whole sub-range over the
            // wire only to be truncated here — this is still **not
            // pushdown** (`StorageEngine::scan` has no limit of its own; see
            // `RaftKvNode::local_scan_kind`'s doc), just a smaller reply and
            // less coordinator-side memory.
            let remaining = limit.map(|l| l - out.len());
            out.extend(
                self.cp_scan_kind_one(
                    table,
                    kind,
                    sub_start,
                    sub_end,
                    remaining,
                    false,
                    consistency,
                )
                .await?,
            );
        }
        if let Some(l) = limit {
            out.truncate(l);
        }
        Ok(out)
    }

    /// Scan a single tablet's kind-scoped sub-range on its group leader (the
    /// body both [`cp_scan_kind`](Self::cp_scan_kind) and
    /// [`cp_scan_kind_table`](Self::cp_scan_kind_table) call). `start`
    /// resolves to exactly one tablet of `table`, so it routes/forwards like
    /// any other CP op. `end == None` is unbounded above. `limit` is a
    /// **per-tablet cap, not pushdown** (see `RaftKvNode::local_scan_kind`'s
    /// doc) — [`cp_scan_kind`](Self::cp_scan_kind) always passes `None` (an
    /// LSI `Query` has no `Limit`, ADR 0041); only
    /// [`cp_scan_kind_table`](Self::cp_scan_kind_table) passes a real value.
    #[allow(clippy::too_many_arguments)] // one tablet's kind-scoped page, plus consistency
    async fn cp_scan_kind_one(
        &self,
        table: &str,
        kind: u8,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: Option<usize>,
        reverse: bool,
        consistency: ReadConsistency,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        // ADR 0055, per tablet — see `cp_scan_one`'s identical arm.
        if consistency.is_eventual()
            && let Some(p) = self
                .cp_scan_kind_one_eventual(table, kind, &start, end.as_deref(), limit, reverse)
                .await
        {
            return Ok(p);
        }
        let deadline = tokio::time::Instant::now() + CLIENT_TIMEOUT;
        loop {
            let err = match self.cp_route(table, &start).await {
                CpRoute::Local(leader) => {
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
                        Ok(p) => return Ok(p),
                        Err(e) => e,
                    }
                }
                CpRoute::Forward(addr) => {
                    let request = ClientRequest::KindScan {
                        table: table.to_owned(),
                        kind,
                        start: start.clone(),
                        end: end.clone(),
                        limit,
                        reverse,
                        stale: false,
                    };
                    match self.cp_forward(table, &start, addr, request).await {
                        ClientResponse::Pairs(p) => return Ok(p),
                        ClientResponse::Error(e) => e,
                        other => {
                            return Err(format!(
                                "unexpected reply to forwarded CP kind scan: {other:?}"
                            ));
                        }
                    }
                }
                CpRoute::None => return Err("no CP group leader reachable".into()),
            };
            if !decide::read_should_retry(&err) || tokio::time::Instant::now() >= deadline {
                return Err(err);
            }
            tokio::time::sleep(SCHEMA_POLL_INTERVAL).await;
        }
    }

    /// Serve a linearizable **kind-scoped scan** on a known-leader local
    /// handle, enforcing the read-side scope pre-check (ADR 0033) — the
    /// kind-scan dual of [`cp_scan_local`](Self::cp_scan_local): a scope that
    /// has not yet caught up to the metadata-derived request window (a
    /// split's narrow in flight) would otherwise silently truncate the
    /// results rather than error. `end == None` is unbounded above; `limit`
    /// is a **per-tablet cap, not pushdown** (see
    /// `RaftKvNode::local_scan_kind`'s doc).
    pub(crate) async fn cp_scan_kind_local(
        leader: &CpGroup<E>,
        kind: u8,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let requested = KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec));
        if !leader.scope_range().contains_range(&requested) {
            return Err(format!(
                "scan window {requested:?} outside tablet's current range (stale routing, likely a split crossover); retry"
            ));
        }
        let served = if reverse {
            leader
                .linearizable_scan_kind_rev(kind, start, end, limit)
                .await
        } else {
            leader.linearizable_scan_kind(kind, start, end, limit).await
        };
        match served {
            Some(p) => Ok(p),
            None => Err("CP group leader moved; retry".into()),
        }
    }

    /// Route a CP-mode **read** for the plain client API (returns a wire
    /// [`ClientResponse`]). Thin adapter over [`cp_read`](Self::cp_read).
    pub(crate) async fn cp_get(&self, table: &str, key: Vec<u8>, stale: bool) -> ClientResponse {
        // A table with no tablet has no data (ADR 0023) — absent, no routing wait.
        // `effective_metadata` (not `metadata_cached()` directly): on a growth
        // node (ADR 0030) the local raft never reflects a table created
        // before it existed.
        if !self.effective_metadata().has_table_tablet(table) {
            return ClientResponse::Value(None);
        }
        match self
            .cp_read(table, key, ReadConsistency::from_consistent_read(!stale))
            .await
        {
            Ok(v) => ClientResponse::Value(v),
            Err(e) => ClientResponse::Error(e),
        }
    }
}
