//! [`EmbeddedCluster`]: a whole multi-node AnimusDB cluster inside one
//! process, driven by a **virtual clock** the embedder advances by hand. The
//! public face of the `embedded` cargo feature (which also builds for
//! `wasm32-unknown-unknown`, so the cluster can run in a browser tab).
//!
//! It is a thin, panic-free wrapper over the in-crate `SimCluster` fixture
//! (ADR 0061 rung D1): one deterministic `animus_sim::Simulator`, a real
//! multi-voter control-plane Raft quorum, a real per-tablet Raft group on every
//! replica, a per-node tablet-host reconciler, and the very same generic
//! request path (`dynamo::execute_item_op_as` over `ClientCtx<SimEnv, _>`)
//! the production DynamoDB edge uses. There are no sockets, no threads, no
//! wall clock and no real I/O: **a run is a pure function of its seed and of
//! the sequence of calls made on it**, so two clusters built from the same seed
//! and driven identically return byte-identical responses and statuses.
//!
//! # Driving it
//!
//! Nothing happens between calls. The embedder (a JS `requestAnimationFrame`
//! pump, a test) alternates:
//!
//! 1. [`EmbeddedCluster::submit`] a DynamoDB-wire request — it returns at once
//!    with a [`RequestId`], the request is now in flight inside the simulation;
//! 2. [`EmbeddedCluster::advance`] virtual time (any `Duration`; the work the
//!    simulator does is proportional to it);
//! 3. [`EmbeddedCluster::poll`] the id until it yields `(status, body)`.
//!
//! Fault injection ([`crash`](EmbeddedCluster::crash),
//! [`restart`](EmbeddedCluster::restart),
//! [`partition`](EmbeddedCluster::partition),
//! [`heal_all`](EmbeddedCluster::heal_all)) and growth
//! ([`grow`](EmbeddedCluster::grow)) are likewise synchronous calls between
//! advances. [`EmbeddedCluster::status`] returns a `serde`-serializable
//! [`ClusterStatus`] snapshot for a UI.
//!
//! # What is and is not served
//!
//! Item operations (`PutItem`, `GetItem`, `UpdateItem`, `DeleteItem`, `Query`,
//! `Scan`, `Batch*`, `Transact*`, PartiQL), `CreateTable`/`DeleteTable`/
//! `ListTables`/`DescribeTable`/`UpdateTable` throughput, and the four
//! DynamoDB Streams reads are served. Backup, export, import, restore, tagging
//! and index/stream `UpdateTable` answer with the same "unsupported by generic
//! dispatch" error `SimCluster` returns. Global secondary indexes **are**
//! maintained: each node runs the per-tablet change-consumer loop
//! (`index_drain::change_consumer_loop`) on the virtual clock, so a GSI `Query`
//! sees a write once the loop has drained it (a few hundred virtual
//! milliseconds).
//!
//! The two web UIs are served too (ADR 0082's 2026-10-31 amendment):
//! [`EmbeddedCluster::submit_http`] runs a `/console/api/*` (data console, ADR
//! 0052) or `/admin/*` (operator dashboard, ADR 0021) request through the same
//! `route`/`dispatch` the real listeners call, without blocking `advance`. The
//! UI assets themselves are the embedder's to serve.
//!
//! # Determinism and memory
//!
//! No `HashMap`, no wall clock, no `tokio::spawn` here: tasks are spawned on
//! the simulator through the `Env` seam. The simulator's trace buffer is turned
//! off at construction, so a cluster left running for hours does not grow an
//! event log.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::EnvExt;
use serde::Serialize;

use crate::sim_cluster::SimCluster;

/// Handle for a request submitted with [`EmbeddedCluster::submit`].
pub type RequestId = u64;

/// How long a CP-data tablet group may sit idle before it quiesces (stops its
/// Raft timers until touched). Production's default (ADR 0048): an idle
/// browser tab should cost almost no simulator work.
const QUIESCE_AFTER: Duration = Duration::from_secs(crate::DEFAULT_QUIESCE_AFTER_SECS);

/// The virtual step [`EmbeddedCluster::run_request`] advances by between two
/// checks of an in-flight request.
const RUN_REQUEST_STEP: Duration = Duration::from_millis(10);

/// A caller error: a node index that does not exist, an impossible cluster
/// shape. The cluster is left untouched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddedError(pub String);

impl std::fmt::Display for EmbeddedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for EmbeddedError {}

/// One wire response: HTTP-style status, content type and the body.
type Response = (u16, &'static str, String);

/// The `Content-Type` of a DynamoDB-wire response.
const DYNAMO_CONTENT_TYPE: &str = "application/x-amz-json-1.0";

/// The `Content-Type` of a locally built JSON error.
const JSON_CONTENT_TYPE: &str = "application/json";

/// The slot a spawned request future fills when it resolves.
type Slot = Arc<Mutex<Option<Response>>>;

/// One HTTP-style response to a [`EmbeddedCluster::submit_http`] request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpResponse {
    /// HTTP status.
    pub status: u16,
    /// The `Content-Type` the real edge would send.
    pub content_type: &'static str,
    /// The response body (JSON for every API route).
    pub body: String,
}

/// An in-flight request: the node it was sent to and its result slot.
struct Pending {
    node: u64,
    slot: Slot,
}

/// A deterministic in-process AnimusDB cluster on a virtual clock — see the
/// module doc.
pub struct EmbeddedCluster {
    cluster: SimCluster,
    pending: BTreeMap<RequestId, Pending>,
    next_request: RequestId,
    /// Nodes `crash`ed and not yet `restart`ed or healed.
    down: BTreeSet<u64>,
    /// Normalised `(low, high)` pairs partitioned and not yet healed.
    partitions: BTreeSet<(u64, u64)>,
}

impl EmbeddedCluster {
    /// Build a `nodes`-node cluster from `seed` and settle its control group
    /// (so the first request finds a leader). Every node runs the control plane
    /// and hosts data; each node is a voter of the control quorum.
    ///
    /// `replication` is the fixture's default replica count for tables it
    /// hosts by hand; a `CreateTable` over the wire is placed by the control
    /// plane's own placement policy regardless.
    ///
    /// # Errors
    /// `nodes` is zero, or `replication` is outside `1..=nodes`.
    pub fn new(seed: u64, nodes: usize, replication: usize) -> Result<Self, EmbeddedError> {
        if nodes == 0 {
            return Err(EmbeddedError("a cluster needs at least one node".into()));
        }
        if !(1..=nodes).contains(&replication) {
            return Err(EmbeddedError(format!(
                "replication must be between 1 and the node count ({nodes}), got {replication}"
            )));
        }
        let cluster =
            SimCluster::new_with_cp_quiescence(seed, nodes, replication, Some(QUIESCE_AFTER));
        // A long-lived embedder must not accumulate an event log. Recording is
        // observational only; it never feeds back into the simulation.
        cluster.simulator().set_trace_cap(Some(0));
        let this = Self {
            cluster,
            pending: BTreeMap::new(),
            next_request: 0,
            down: BTreeSet::new(),
            partitions: BTreeSet::new(),
        };
        for node in 0..nodes as u64 {
            this.spawn_change_consumer(node);
        }
        Ok(this)
    }

    /// Spawn the per-node change-consumer loop (GSI maintenance, stream and
    /// PITR sealing arms, change-log trim) on `node`'s env. `SimCluster` itself
    /// never spawns it (its tests drive each arm on demand); an embedder wants
    /// the background behaviour. A node restart drops it with the rest of that
    /// node's tasks, so [`restart`](Self::restart) and [`grow`](Self::grow)
    /// respawn it.
    fn spawn_change_consumer(&self, node: u64) {
        // The slot is never filled (the loop never returns); dropping it is fine.
        drop(
            self.cluster
                .spawn_detached(node, crate::index_drain::change_consumer_loop),
        );
    }

    fn check_node(&self, node: u64) -> Result<(), EmbeddedError> {
        if (node as usize) < self.cluster.node_count() {
            Ok(())
        } else {
            Err(EmbeddedError(format!(
                "no node {node}: the cluster has {} nodes (0..{})",
                self.cluster.node_count(),
                self.cluster.node_count()
            )))
        }
    }

    /// The number of nodes, including any added with [`grow`](Self::grow).
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.cluster.node_count()
    }

    /// Send a DynamoDB-wire request (`target` is the `X-Amz-Target` value, for
    /// example `DynamoDB_20120810.PutItem`; `body` the JSON payload) to `node`
    /// **without advancing time**. The request is in flight inside the
    /// simulation; [`advance`](Self::advance) it forward and
    /// [`poll`](Self::poll) the returned id.
    ///
    /// A request to a node that is currently crashed completes immediately
    /// with status `503` (the connection a real client would be refused).
    ///
    /// # Errors
    /// `node` does not exist.
    pub fn submit(
        &mut self,
        node: u64,
        target: &str,
        body: &str,
    ) -> Result<RequestId, EmbeddedError> {
        self.check_node(node)?;
        let id = self.next_request;
        self.next_request += 1;
        let slot: Slot = Arc::new(Mutex::new(None));
        if self.down.contains(&node) {
            *slot.lock().expect("result slot poisoned") = Some(unavailable(node, "is down"));
        } else {
            let handle = self.cluster.handle();
            let (target, body) = (target.to_owned(), body.as_bytes().to_vec());
            let out = slot.clone();
            handle.env(node).spawn_task(async move {
                let (status, body) =
                    if target.starts_with(animus_dynamo::streams_wire::TARGET_PREFIX) {
                        handle.dynamo_streams(node, &target, &body).await
                    } else {
                        handle.dynamo(node, &target, &body).await
                    };
                *out.lock().expect("result slot poisoned") =
                    Some((status, DYNAMO_CONTENT_TYPE, body));
            });
        }
        self.pending.insert(id, Pending { node, slot });
        Ok(id)
    }

    /// Send an HTTP-style request for the **data console** (`/console/api/*`,
    /// ADR 0052) or the **operator dashboard's admin API** (`/admin/*`, ADR
    /// 0020/0021) to `node`, **without advancing time**. `path` is the request
    /// target, query string included (`/admin/raftkv?tablet=3`). The request
    /// runs through the very `animus_node::console::route` /
    /// `animus_node::admin::dispatch` the real listeners call, over the same
    /// `ClientCtx` the DynamoDB path uses, so both UIs see this cluster's live
    /// state. [`advance`](Self::advance) and [`poll`](Self::poll_http) it like
    /// [`submit`](Self::submit).
    ///
    /// The static UI assets (`/console/ui/*`, `/admin/ui/*`) are *not* served
    /// here: the embedder ships the HTML/CSS/JS (the real files under
    /// `crates/animusd/src`). Such a path, and any other unknown path, answers
    /// `404`. A request to a crashed node completes at once with `503`.
    ///
    /// Actions the fixture cannot perform (S3 backups, an index `UpdateTable`,
    /// OpenTelemetry) answer with the same error the sim fixture returns; they
    /// never panic.
    ///
    /// # Errors
    /// `node` does not exist.
    pub fn submit_http(
        &mut self,
        node: u64,
        method: &str,
        path: &str,
        body: &str,
    ) -> Result<RequestId, EmbeddedError> {
        self.check_node(node)?;
        let id = self.next_request;
        self.next_request += 1;
        let slot: Slot = Arc::new(Mutex::new(None));
        let (path_only, query) = path.split_once('?').unwrap_or((path, ""));
        let is_console = path_only.starts_with("/console/api/");
        let is_admin = path_only.starts_with("/admin/") && !path_only.starts_with("/admin/ui/");
        if self.down.contains(&node) {
            *slot.lock().expect("result slot poisoned") = Some(unavailable(node, "is down"));
        } else if !is_console && !is_admin {
            *slot.lock().expect("result slot poisoned") = Some((
                404,
                JSON_CONTENT_TYPE,
                serde_json::json!({
                    "error": format!("no such API route: {path_only}"),
                })
                .to_string(),
            ));
        } else {
            let handle = self.cluster.handle();
            let (method, path_only, query, body) = (
                method.to_owned(),
                path_only.to_owned(),
                query.to_owned(),
                body.as_bytes().to_vec(),
            );
            let out = slot.clone();
            handle.env(node).spawn_task(async move {
                let result = if is_console {
                    handle
                        .console(node, &method, &path_only, &query, &body)
                        .await
                } else {
                    let (status, body) =
                        handle.admin(node, &method, &path_only, &query, &body).await;
                    (status, JSON_CONTENT_TYPE, body)
                };
                *out.lock().expect("result slot poisoned") = Some(result);
            });
        }
        self.pending.insert(id, Pending { node, slot });
        Ok(id)
    }

    /// [`poll`](Self::poll) for a [`submit_http`](Self::submit_http) request,
    /// keeping the content type.
    pub fn poll_http(&mut self, id: RequestId) -> Option<HttpResponse> {
        self.take(id)
            .map(|(status, content_type, body)| HttpResponse {
                status,
                content_type,
                body,
            })
    }

    /// The result of request `id` if it has finished, consuming it; `None`
    /// while it is still in flight (and for an id that was already returned or
    /// never issued).
    pub fn poll(&mut self, id: RequestId) -> Option<(u16, String)> {
        self.take(id).map(|(status, _, body)| (status, body))
    }

    fn take(&mut self, id: RequestId) -> Option<Response> {
        let done = self
            .pending
            .get(&id)?
            .slot
            .lock()
            .expect("result slot poisoned")
            .take();
        if done.is_some() {
            self.pending.remove(&id);
        }
        done
    }

    /// Give up on request `id` (the embedder's own timeout). The simulation
    /// still finishes the work; only the result is discarded. Returns whether
    /// the id was known.
    pub fn forget(&mut self, id: RequestId) -> bool {
        self.pending.remove(&id).is_some()
    }

    /// Advance virtual time by `dur`, running every timer, message delivery
    /// and task that falls due. The cost is proportional to `dur` and to the
    /// cluster's activity; an embedder pumping from an animation frame should
    /// pass small steps.
    pub fn advance(&mut self, dur: Duration) {
        self.cluster.run_for(dur);
    }

    /// Convenience for tests and scripted callers: [`submit`](Self::submit),
    /// then advance in small virtual steps until the request finishes or
    /// `budget` of virtual time has passed (the request is then abandoned and
    /// `None` returned).
    ///
    /// # Errors
    /// `node` does not exist.
    pub fn run_request(
        &mut self,
        node: u64,
        target: &str,
        body: &str,
        budget: Duration,
    ) -> Result<Option<(u16, String)>, EmbeddedError> {
        let id = self.submit(node, target, body)?;
        let mut waited = Duration::ZERO;
        loop {
            if let Some(done) = self.poll(id) {
                return Ok(Some(done));
            }
            if waited >= budget {
                self.forget(id);
                return Ok(None);
            }
            self.advance(RUN_REQUEST_STEP);
            waited += RUN_REQUEST_STEP;
        }
    }

    /// Crash `node`: it is muted (drops every message, runs nothing) but keeps
    /// its durable state, exactly as in the simulator's fault model. Requests
    /// already in flight on it may never finish; new ones get `503`.
    ///
    /// # Errors
    /// `node` does not exist.
    pub fn crash(&mut self, node: u64) -> Result<(), EmbeddedError> {
        self.check_node(node)?;
        self.cluster.crash(node);
        self.down.insert(node);
        Ok(())
    }

    /// Restart `node` as a fresh process on its surviving state (a crashed
    /// node is un-muted first). Requests that were in flight on it complete
    /// with `503`: their connection died with the old process.
    ///
    /// # Errors
    /// `node` does not exist.
    pub fn restart(&mut self, node: u64) -> Result<(), EmbeddedError> {
        self.check_node(node)?;
        self.cluster.restart(node);
        self.down.remove(&node);
        for pending in self.pending.values().filter(|p| p.node == node) {
            let mut slot = pending.slot.lock().expect("result slot poisoned");
            if slot.is_none() {
                *slot = Some(unavailable(node, "restarted; the connection was reset"));
            }
        }
        self.spawn_change_consumer(node);
        Ok(())
    }

    /// Partition `a` from `b` in both directions until [`heal_all`](Self::heal_all).
    ///
    /// # Errors
    /// Either node does not exist, or `a == b`.
    pub fn partition(&mut self, a: u64, b: u64) -> Result<(), EmbeddedError> {
        self.check_node(a)?;
        self.check_node(b)?;
        if a == b {
            return Err(EmbeddedError("cannot partition a node from itself".into()));
        }
        self.cluster.partition(a, b);
        self.partitions.insert((a.min(b), a.max(b)));
        Ok(())
    }

    /// Heal every partition and un-mute every crashed node.
    pub fn heal_all(&mut self) {
        self.cluster.heal_all();
        self.partitions.clear();
        self.down.clear();
    }

    /// Add a combined (control voter + data) node, wait for the cluster to
    /// admit it, and return its index. Advances virtual time while it joins.
    ///
    /// # Errors
    /// A node is crashed or a partition is in place. The fixture only returns
    /// once *every* node has seen the new member, so a node that cannot hear
    /// the cluster would make it give up with a panic; refusing up front keeps
    /// the cluster usable. [`heal_all`](Self::heal_all) (and restart any
    /// crashed node) first.
    pub fn grow(&mut self) -> Result<u64, EmbeddedError> {
        if !self.down.is_empty() || !self.partitions.is_empty() {
            return Err(EmbeddedError(
                "cannot add a node while a node is down or a partition is in place: \
                 heal and restart first"
                    .into(),
            ));
        }
        let node = self.cluster.grow("combined");
        self.spawn_change_consumer(node);
        Ok(node)
    }

    /// A serializable snapshot of the cluster for a UI: virtual time, per-node
    /// up/down, the control leader, the tablets each node hosts and leads, the
    /// simulator's work counters. Pure read: advances nothing.
    #[must_use]
    pub fn status(&self) -> ClusterStatus {
        let count = self.cluster.node_count() as u64;
        let nodes: Vec<NodeStatus> = (0..count)
            .map(|id| {
                let up = !self.down.contains(&id);
                let (hosted, led) = if up {
                    let groups = self.cluster.group_leads_terms(id);
                    (
                        groups.iter().map(|(t, _, _)| t.0).collect(),
                        groups
                            .iter()
                            .filter(|(_, leads, _)| *leads)
                            .map(|(t, _, _)| t.0)
                            .collect(),
                    )
                } else {
                    (Vec::new(), Vec::new())
                };
                NodeStatus {
                    id,
                    up,
                    control_leader: up && self.cluster.is_control_leader(id),
                    hosted_tablets: hosted,
                    led_tablets: led,
                }
            })
            .collect();
        let control_leader = nodes.iter().find(|n| n.control_leader).map(|n| n.id);
        // Table and tablet layout from the freshest live node's view.
        let (tables, tablets) = nodes.iter().find(|n| n.up).map_or_else(
            || (Vec::new(), Vec::new()),
            |n| {
                let meta = self.cluster.metadata(n.id);
                let tables = meta
                    .table_schemas()
                    .map(|(name, _)| name.to_string())
                    .collect();
                let tablets = meta
                    .tablets
                    .values()
                    .map(|t| TabletStatus {
                        id: t.id.0,
                        table: t.table.as_ref().map(ToString::to_string),
                        replicas: t.replicas.iter().map(ToString::to_string).collect(),
                        state: format!("{:?}", t.state),
                        leader_node: self
                            .cluster
                            .leader_index_of(t.id)
                            .filter(|l| !self.down.contains(l)),
                    })
                    .collect();
                (tables, tablets)
            },
        );
        let stats = self.cluster.sim_stats();
        ClusterStatus {
            seed: self.cluster.seed(),
            virtual_time_ms: self.cluster.sim_now().0 / 1_000_000,
            control_leader,
            nodes,
            tables,
            tablets,
            partitions: self.partitions.iter().copied().collect(),
            pending_requests: self.pending.len(),
            task_polls: stats.task_polls,
            timer_fires: stats.timer_fires,
        }
    }
}

/// The `503` a request gets when its node is down or restarts under it.
fn unavailable(node: u64, why: &str) -> Response {
    (
        503,
        DYNAMO_CONTENT_TYPE,
        serde_json::json!({
            "__type": "com.amazon.coral.availability#ServiceUnavailableException",
            "message": format!("node {node} {why}"),
        })
        .to_string(),
    )
}

/// A point-in-time view of an [`EmbeddedCluster`], for a UI. All fields are
/// plain data and serialize to JSON.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ClusterStatus {
    /// The seed the cluster was built from (replays the run).
    pub seed: u64,
    /// Elapsed virtual time in milliseconds.
    pub virtual_time_ms: u64,
    /// The node currently leading the control plane, if one does.
    pub control_leader: Option<u64>,
    /// Every node, by index.
    pub nodes: Vec<NodeStatus>,
    /// Tables in the replicated catalog.
    pub tables: Vec<String>,
    /// Every tablet in the replicated map.
    pub tablets: Vec<TabletStatus>,
    /// Partitioned `(low, high)` node pairs.
    pub partitions: Vec<(u64, u64)>,
    /// Requests submitted and not yet polled or forgotten.
    pub pending_requests: usize,
    /// The simulator's task-poll count so far (a work/cost counter).
    pub task_polls: u64,
    /// The simulator's timer/delivery count so far (a work/cost counter).
    pub timer_fires: u64,
}

/// One node of a [`ClusterStatus`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NodeStatus {
    /// The node index.
    pub id: u64,
    /// `false` between a [`crash`](EmbeddedCluster::crash) and the next
    /// [`restart`](EmbeddedCluster::restart) or [`heal_all`](EmbeddedCluster::heal_all).
    pub up: bool,
    /// Whether this node believes it leads the control plane.
    pub control_leader: bool,
    /// Tablets this node hosts a replica of (empty while down).
    pub hosted_tablets: Vec<u64>,
    /// Of those, the tablets whose Raft group this node leads.
    pub led_tablets: Vec<u64>,
}

/// One tablet of a [`ClusterStatus`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TabletStatus {
    /// The tablet id.
    pub id: u64,
    /// The table it belongs to.
    pub table: Option<String>,
    /// The nodes replicating it, as `Display` node ids.
    pub replicas: Vec<String>,
    /// Lifecycle state (`Active`, `Splitting`, ...).
    pub state: String,
    /// The node whose group currently believes it leads this tablet.
    pub leader_node: Option<u64>,
}

#[cfg(all(test, feature = "embedded"))]
mod tests {
    use super::*;
    use crate::sim_support::env_seed;

    const PUT: &str = "DynamoDB_20120810.PutItem";
    const GET: &str = "DynamoDB_20120810.GetItem";
    const CREATE: &str = "DynamoDB_20120810.CreateTable";
    const QUERY: &str = "DynamoDB_20120810.Query";
    const SCAN: &str = "DynamoDB_20120810.Scan";

    /// A generous virtual budget for one request (a fresh cluster may still be
    /// electing a tablet leader).
    const BUDGET: Duration = Duration::from_secs(30);

    fn run(c: &mut EmbeddedCluster, node: u64, target: &str, body: &str) -> (u16, String) {
        c.run_request(node, target, body, BUDGET)
            .expect("node exists")
            .unwrap_or_else(|| panic!("{target} on node {node} never finished: {body}"))
    }

    fn ok(c: &mut EmbeddedCluster, node: u64, target: &str, body: &str) -> String {
        let (status, resp) = run(c, node, target, body);
        assert_eq!(
            status,
            200,
            "{target} on node {node} failed: {resp} (seed={})",
            c.status().seed
        );
        resp
    }

    fn create_docs(c: &mut EmbeddedCluster) {
        ok(
            c,
            0,
            CREATE,
            r#"{"TableName":"docs","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
               "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        );
    }

    fn put(c: &mut EmbeddedCluster, node: u64, id: &str, v: &str) -> (u16, String) {
        run(
            c,
            node,
            PUT,
            &format!(r#"{{"TableName":"docs","Item":{{"id":{{"S":"{id}"}},"v":{{"S":"{v}"}}}}}}"#),
        )
    }

    fn get(c: &mut EmbeddedCluster, node: u64, id: &str) -> String {
        ok(
            c,
            node,
            GET,
            &format!(
                r#"{{"TableName":"docs","ConsistentRead":true,"Key":{{"id":{{"S":"{id}"}}}}}}"#
            ),
        )
    }

    #[test]
    fn rejects_impossible_shapes_and_unknown_nodes() {
        assert!(EmbeddedCluster::new(1, 0, 1).is_err());
        assert!(EmbeddedCluster::new(1, 3, 0).is_err());
        assert!(EmbeddedCluster::new(1, 3, 4).is_err());
        let mut c = EmbeddedCluster::new(1, 3, 3).expect("3 nodes");
        assert!(c.submit(3, PUT, "{}").is_err());
        assert!(c.crash(9).is_err());
        assert!(c.restart(9).is_err());
        assert!(c.partition(0, 0).is_err());
        assert!(c.partition(0, 7).is_err());
        assert_eq!(c.poll(42), None, "an unknown id is simply not ready");
    }

    /// The whole non-blocking surface: submit returns at once, the request is
    /// in flight until time advances, then poll yields it exactly once.
    #[test]
    fn submit_poll_advance_cycle() {
        let seed = env_seed(0xE4B0_0001);
        let mut c = EmbeddedCluster::new(seed, 3, 3).unwrap();
        let id = c
            .submit(
                0,
                CREATE,
                r#"{"TableName":"docs","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
                    "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
            )
            .unwrap();
        assert_eq!(c.status().pending_requests, 1);
        assert_eq!(
            c.poll(id),
            None,
            "nothing runs until time advances (seed={seed})"
        );
        let mut result = None;
        for _ in 0..3000 {
            c.advance(Duration::from_millis(10));
            if let Some(r) = c.poll(id) {
                result = Some(r);
                break;
            }
        }
        let (status, body) =
            result.unwrap_or_else(|| panic!("CreateTable never finished (seed={seed})"));
        assert_eq!(status, 200, "{body} (seed={seed})");
        assert_eq!(c.poll(id), None, "a result is handed out once");
        assert_eq!(c.status().pending_requests, 0);
        assert!(!c.forget(id));
    }

    #[test]
    fn create_put_get_query_scan() {
        let seed = env_seed(0xE4B0_0002);
        let mut c = EmbeddedCluster::new(seed, 3, 3).unwrap();
        create_docs(&mut c);
        for (id, v) in [("a", "1"), ("b", "2"), ("c", "3")] {
            let (status, body) = put(&mut c, 1, id, v);
            assert_eq!(status, 200, "PutItem({id}): {body} (seed={seed})");
        }
        // Read back through a different node than the write.
        let got = get(&mut c, 2, "b");
        assert!(got.contains(r#""v":{"S":"2"}"#), "seed={seed}: {got}");

        let q = ok(
            &mut c,
            0,
            QUERY,
            r#"{"TableName":"docs","ConsistentRead":true,"KeyConditionExpression":"id = :i",
                "ExpressionAttributeValues":{":i":{"S":"c"}}}"#,
        );
        assert!(
            q.contains("\"Count\":1") && q.contains(r#""v":{"S":"3"}"#),
            "seed={seed}: {q}"
        );

        let s = ok(
            &mut c,
            0,
            SCAN,
            r#"{"TableName":"docs","ConsistentRead":true}"#,
        );
        assert!(s.contains("\"Count\":3"), "seed={seed}: {s}");

        let (status, body) = run(&mut c, 0, "DynamoDB_20120810.NoSuchOperation", "{}");
        assert_ne!(status, 200, "an unknown operation must fail: {body}");

        let st = c.status();
        assert_eq!(st.tables, vec!["docs".to_owned()]);
        assert_eq!(st.tablets.len(), 1, "one tablet for one small table");
        assert!(
            st.control_leader.is_some(),
            "control leader known (seed={seed})"
        );
        assert!(st.nodes.iter().all(|n| n.up));
        assert!(st.virtual_time_ms > 0 && st.task_polls > 0);
        let json = serde_json::to_string(&st).expect("status serializes");
        assert!(json.contains("virtual_time_ms"), "{json}");
    }

    /// Global secondary indexes are maintained by the change-consumer loop the
    /// cluster spawns on every node.
    #[test]
    fn gsi_query_sees_writes_once_the_background_loop_drains_them() {
        let seed = env_seed(0xE4B0_0003);
        let mut c = EmbeddedCluster::new(seed, 3, 3).unwrap();
        ok(
            &mut c,
            0,
            CREATE,
            r#"{"TableName":"users","AttributeDefinitions":[{"AttributeName":"email","AttributeType":"S"},{"AttributeName":"id","AttributeType":"S"}],
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                "GlobalSecondaryIndexes":[{"IndexName":"by-email",
                  "KeySchema":[{"AttributeName":"email","KeyType":"HASH"}],
                  "Projection":{"ProjectionType":"ALL"}}]}"#,
        );
        for (id, email) in [("u1", "a@x"), ("u2", "b@x"), ("u3", "a@x")] {
            ok(
                &mut c,
                0,
                PUT,
                &format!(
                    r#"{{"TableName":"users","Item":{{"id":{{"S":"{id}"}},"email":{{"S":"{email}"}}}}}}"#
                ),
            );
        }
        // No manual drain: just let virtual time pass.
        c.advance(Duration::from_secs(5));
        let body = ok(
            &mut c,
            1,
            QUERY,
            r#"{"TableName":"users","IndexName":"by-email","KeyConditionExpression":"email = :e",
                "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#,
        );
        assert!(body.contains("\"Count\":2"), "seed={seed}: {body}");
        assert!(
            body.contains(r#""id":{"S":"u1"}"#) && body.contains(r#""id":{"S":"u3"}"#),
            "seed={seed}: {body}"
        );
    }

    /// One scripted session; everything observable is collected so two runs
    /// can be compared byte for byte.
    fn scripted_session(seed: u64) -> Vec<String> {
        let mut c = EmbeddedCluster::new(seed, 3, 3).unwrap();
        let mut log = Vec::new();
        create_docs(&mut c);
        for i in 0..6 {
            let (s, b) = put(&mut c, i % 3, &format!("k{i}"), &format!("v{i}"));
            log.push(format!("put{i}:{s}:{b}"));
        }
        c.crash(2).unwrap();
        c.advance(Duration::from_secs(3));
        let (s, b) = put(&mut c, 0, "after-crash", "x");
        log.push(format!("put-after-crash:{s}:{b}"));
        c.restart(2).unwrap();
        c.advance(Duration::from_secs(5));
        log.push(format!("get:{}", get(&mut c, 2, "after-crash")));
        log.push(format!(
            "status:{}",
            serde_json::to_string(&c.status()).unwrap()
        ));
        log
    }

    #[test]
    fn same_seed_twice_is_byte_identical() {
        let seed = env_seed(0xE4B0_0004);
        let first = scripted_session(seed);
        let second = scripted_session(seed);
        assert_eq!(
            first, second,
            "a run must be a pure function of its seed (seed={seed})"
        );
        assert!(first.iter().any(|l| l.starts_with("put0:200")), "{first:?}");
    }

    #[test]
    fn leader_crash_then_a_write_through_a_survivor_succeeds() {
        let seed = env_seed(0xE4B0_0005);
        let mut c = EmbeddedCluster::new(seed, 3, 3).unwrap();
        create_docs(&mut c);
        assert_eq!(put(&mut c, 0, "before", "1").0, 200);

        let tablet = c.status().tablets[0].clone();
        let leader = tablet
            .leader_node
            .unwrap_or_else(|| panic!("the tablet has a leader (seed={seed})"));
        c.crash(leader).unwrap();
        assert!(!c.status().nodes[leader as usize].up);

        // A request sent to the crashed node is refused outright.
        let (refused, _) = run(&mut c, leader, GET, "{}");
        assert_eq!(refused, 503);

        let survivor = (leader + 1) % 3;
        // The survivors must elect a new group leader; the write's own retry
        // budget covers it, so a bounded retry loop over virtual time is the
        // honest client behaviour.
        let mut wrote = false;
        for _ in 0..20 {
            let (status, _) = put(&mut c, survivor, "after", "2");
            if status == 200 {
                wrote = true;
                break;
            }
            c.advance(Duration::from_secs(1));
        }
        assert!(
            wrote,
            "no write succeeded through survivor {survivor} (seed={seed})"
        );
        assert!(get(&mut c, survivor, "after").contains(r#""v":{"S":"2"}"#));
        assert!(get(&mut c, survivor, "before").contains(r#""v":{"S":"1"}"#));

        // The old leader rejoins and converges on the same data.
        c.restart(leader).unwrap();
        c.advance(Duration::from_secs(10));
        assert!(
            get(&mut c, leader, "after").contains(r#""v":{"S":"2"}"#),
            "restarted node must converge (seed={seed})"
        );
        assert!(c.status().nodes.iter().all(|n| n.up));
    }

    #[test]
    fn partition_then_heal_converges() {
        let seed = env_seed(0xE4B0_0006);
        let mut c = EmbeddedCluster::new(seed, 3, 3).unwrap();
        create_docs(&mut c);
        assert_eq!(put(&mut c, 0, "base", "1").0, 200);

        // Cut node 2 off from the other two (a minority of one).
        c.partition(2, 0).unwrap();
        c.partition(2, 1).unwrap();
        assert_eq!(c.status().partitions, vec![(0, 2), (1, 2)]);
        c.advance(Duration::from_secs(3));

        let mut wrote = false;
        for _ in 0..20 {
            if put(&mut c, 0, "majority", "2").0 == 200 {
                wrote = true;
                break;
            }
            c.advance(Duration::from_secs(1));
        }
        assert!(
            wrote,
            "the majority side must keep accepting writes (seed={seed})"
        );

        c.heal_all();
        assert!(c.status().partitions.is_empty());
        c.advance(Duration::from_secs(15));
        for node in 0..3 {
            let got = get(&mut c, node, "majority");
            assert!(
                got.contains(r#""v":{"S":"2"}"#),
                "node {node} must see the majority write after the heal: {got} (seed={seed})"
            );
        }
    }

    #[test]
    fn grow_adds_a_serving_node() {
        let seed = env_seed(0xE4B0_0007);
        let mut c = EmbeddedCluster::new(seed, 3, 3).unwrap();
        create_docs(&mut c);
        assert_eq!(put(&mut c, 0, "k", "1").0, 200);
        let new = c.grow().unwrap();
        assert_eq!(new, 3);
        assert_eq!(c.node_count(), 4);
        assert!(
            get(&mut c, new, "k").contains(r#""v":{"S":"1"}"#),
            "seed={seed}"
        );
        assert_eq!(c.status().nodes.len(), 4);
    }

    /// Growing while a node is down or cut off is refused, not a panic that
    /// would poison the cluster.
    #[test]
    fn grow_is_refused_while_degraded() {
        let seed = env_seed(0xE4B0_0009);
        let mut c = EmbeddedCluster::new(seed, 3, 3).unwrap();
        c.crash(2).unwrap();
        assert!(c.grow().is_err(), "seed={seed}");
        c.heal_all();
        c.partition(0, 1).unwrap();
        assert!(c.grow().is_err(), "seed={seed}");
        c.heal_all();
        assert_eq!(c.grow().unwrap(), 3, "seed={seed}");
    }

    /// A request whose node is restarted under it is answered `503`, not left
    /// to hang forever.
    #[test]
    fn restart_resolves_in_flight_requests_on_that_node() {
        let seed = env_seed(0xE4B0_0008);
        let mut c = EmbeddedCluster::new(seed, 3, 3).unwrap();
        create_docs(&mut c);
        let id = c
            .submit(
                1,
                GET,
                r#"{"TableName":"docs","ConsistentRead":true,"Key":{"id":{"S":"zz"}}}"#,
            )
            .unwrap();
        c.restart(1).unwrap();
        let (status, _) = c.poll(id).expect("restart resolves the in-flight request");
        assert_eq!(status, 503);
    }

    /// The trace buffer is off: a long idle stretch retains no events.
    #[test]
    fn idle_cluster_retains_no_trace() {
        let mut c = EmbeddedCluster::new(env_seed(0xE4B0_0009), 3, 3).unwrap();
        c.advance(Duration::from_secs(60));
        assert!(c.cluster.simulator().trace().is_empty());
    }

    fn http(
        c: &mut EmbeddedCluster,
        node: u64,
        method: &str,
        path: &str,
        body: &str,
    ) -> HttpResponse {
        let id = c
            .submit_http(node, method, path, body)
            .expect("node exists");
        for _ in 0..3000 {
            if let Some(r) = c.poll_http(id) {
                return r;
            }
            c.advance(Duration::from_millis(10));
        }
        panic!("http request {method} {path} did not finish");
    }

    #[test]
    fn console_and_admin_see_a_table_created_over_the_dynamo_wire() {
        let seed = env_seed(11);
        let mut c = EmbeddedCluster::new(seed, 3, 3).expect("cluster");
        let create = r#"{"TableName":"orders","BillingMode":"PAY_PER_REQUEST",
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#;
        assert_eq!(run(&mut c, 0, CREATE, create).0, 200, "seed {seed}");
        let put = r#"{"TableName":"orders","Item":{"id":{"S":"a"},"n":{"N":"1"}}}"#;
        assert_eq!(run(&mut c, 0, PUT, put).0, 200, "seed {seed}");
        // The data console lists it from any node, as JSON.
        for node in 0..3 {
            let r = http(&mut c, node, "GET", "/console/api/tables", "");
            assert_eq!(r.status, 200, "seed {seed} node {node}: {}", r.body);
            assert!(
                r.body.contains("orders"),
                "seed {seed} node {node}: {}",
                r.body
            );
            assert_eq!(r.content_type, "application/json");
        }
        // ... and scans its items through the console's own endpoint.
        let r = http(
            &mut c,
            1,
            "POST",
            "/console/api/tables/orders/items/scan",
            "{}",
        );
        assert_eq!(r.status, 200, "seed {seed}: {}", r.body);
        assert!(r.body.contains("\"a\""), "seed {seed}: {}", r.body);
        // The dashboard's admin API answers with cluster state, query included.
        let r = http(&mut c, 2, "GET", "/admin/status", "");
        assert_eq!(r.status, 200, "seed {seed}: {}", r.body);
        assert!(r.body.contains("orders"), "seed {seed}: {}", r.body);
        let r = http(&mut c, 0, "GET", "/admin/raftkv?x=1", "");
        assert_eq!(r.status, 200, "seed {seed}: {}", r.body);
    }

    #[test]
    fn http_to_unknown_paths_and_down_nodes_is_refused_not_panicking() {
        let mut c = EmbeddedCluster::new(env_seed(12), 3, 3).expect("cluster");
        assert_eq!(http(&mut c, 0, "GET", "/nope", "").status, 404);
        // Static assets are the embedder's, not the cluster's.
        assert_eq!(http(&mut c, 0, "GET", "/admin/ui/overview", "").status, 404);
        assert!(c.submit_http(9, "GET", "/admin/status", "").is_err());
        c.crash(1).expect("crash");
        assert_eq!(http(&mut c, 1, "GET", "/admin/status", "").status, 503);
        // Actions the fixture cannot perform answer an error body, never a panic.
        for (m, p, b) in [
            ("GET", "/admin/backups", ""),
            ("GET", "/admin/backup-store", ""),
            (
                "POST",
                "/admin/data/dynamo",
                r#"{"op":"CreateBackup","payload":{"TableName":"x","BackupName":"b"}}"#,
            ),
            ("POST", "/admin/data/dynamo", "not json"),
            ("POST", "/console/api/tables", "{}"),
            ("GET", "/console/api/tables/missing", ""),
        ] {
            let r = http(&mut c, 0, m, p, b);
            assert!(r.status < 600, "{m} {p}");
        }
    }

    #[test]
    fn http_results_are_a_function_of_the_seed() {
        let drive = |seed| {
            let mut c = EmbeddedCluster::new(seed, 3, 3).expect("cluster");
            let a = http(&mut c, 0, "GET", "/admin/config", "");
            let b = http(&mut c, 1, "GET", "/console/api/tables", "");
            (
                a.status,
                a.body,
                b.status,
                b.body,
                c.status().virtual_time_ms,
            )
        };
        let seed = env_seed(13);
        assert_eq!(drive(seed), drive(seed), "seed {seed}");
    }
}
