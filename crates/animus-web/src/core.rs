//! The plain-Rust core of the browser build: no JS types, native-testable.
//!
//! [`Core`] owns an `EmbeddedCluster` and the request pump. The embedder calls
//! [`Core::tick`] with the real milliseconds elapsed since its previous call
//! and a speed multiplier; the core advances the cluster's *virtual* clock by
//! `real_ms * speed` (clamped so a returning background tab does not burst) and
//! returns every request that finished or timed out.

use std::collections::BTreeMap;
use std::time::Duration;

use animusd::embedded::{ClusterStatus, EmbeddedCluster, EmbeddedError, RequestId};

/// Replica count the fixture uses for tables it hosts by hand (a wire
/// `CreateTable` is placed by the control plane's own policy regardless).
const REPLICATION: usize = 3;

/// The most real time one [`Core::tick`] counts, in milliseconds. A tab coming
/// back from the background reports a huge frame gap; virtual time simply
/// resumes rather than racing to catch up (and never flaps Raft elections).
pub const MAX_TICK_REAL_MS: f64 = 100.0;

/// The fastest speed multiplier [`Core::tick`] honours.
pub const MAX_SPEED: f64 = 100.0;

/// A request that has been in flight this long in virtual time is abandoned
/// and answered with a `504`.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The largest cluster the demo builds.
pub const MAX_NODES: usize = 9;

/// A finished request, ready to hand back to the caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Completed {
    /// The id [`Core::submit`] returned.
    pub id: RequestId,
    /// HTTP-style status.
    pub status: u16,
    /// The response body (JSON for every route the cluster itself serves).
    pub body: String,
    /// The `Content-Type` the real edge would send.
    pub content_type: String,
}

/// A request awaiting its result.
struct InFlight {
    /// Virtual time (microseconds) past which the request is abandoned.
    deadline_us: u64,
}

/// The embedded cluster plus its request pump.
pub struct Core {
    cluster: EmbeddedCluster,
    in_flight: BTreeMap<RequestId, InFlight>,
    /// Virtual time elapsed, in microseconds, as this core has advanced it.
    now_us: u64,
}

impl Core {
    /// Build an `nodes`-node cluster from `seed` and let its control group
    /// settle.
    ///
    /// # Errors
    /// `nodes` is zero or above [`MAX_NODES`].
    pub fn new(seed: u64, nodes: usize) -> Result<Self, EmbeddedError> {
        if nodes > MAX_NODES {
            return Err(EmbeddedError(format!(
                "the demo runs at most {MAX_NODES} nodes, got {nodes}"
            )));
        }
        let cluster = EmbeddedCluster::new(seed, nodes, REPLICATION.min(nodes))?;
        let mut core = Self {
            cluster,
            in_flight: BTreeMap::new(),
            now_us: 0,
        };
        core.sync_clock();
        Ok(core)
    }

    fn sync_clock(&mut self) {
        self.now_us = self.cluster.status().virtual_time_ms.saturating_mul(1000);
    }

    /// Send a DynamoDB-wire request to `node` and return its id. Nothing runs
    /// until [`tick`](Self::tick).
    ///
    /// # Errors
    /// `node` does not exist.
    pub fn submit(
        &mut self,
        node: u64,
        target: &str,
        body: &str,
    ) -> Result<RequestId, EmbeddedError> {
        let id = self.cluster.submit(node, target, body)?;
        Ok(self.track(id))
    }

    /// Send an HTTP-style request for the data console (`/console/api/*`) or
    /// the operator dashboard's admin API (`/admin/*`) to `node` and return its
    /// id. `path` is the request target including any query string. Resolved
    /// by [`tick`](Self::tick) like [`submit`](Self::submit), with the same
    /// 30-virtual-second timeout.
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
        let id = self.cluster.submit_http(node, method, path, body)?;
        Ok(self.track(id))
    }

    fn track(&mut self, id: RequestId) -> RequestId {
        let deadline_us = self
            .now_us
            .saturating_add(u64::try_from(REQUEST_TIMEOUT.as_micros()).unwrap_or(u64::MAX));
        self.in_flight.insert(id, InFlight { deadline_us });
        id
    }

    /// Advance virtual time by `real_ms * speed` (see [`MAX_TICK_REAL_MS`] and
    /// [`MAX_SPEED`]; non-finite or non-positive inputs advance nothing) and
    /// return every request that finished or timed out, oldest id first.
    pub fn tick(&mut self, real_ms: f64, speed: f64) -> Vec<Completed> {
        let virtual_us = virtual_micros(real_ms, speed);
        if virtual_us > 0 {
            self.cluster.advance(Duration::from_micros(virtual_us));
            self.now_us = self.now_us.saturating_add(virtual_us);
        }
        self.resolve()
    }

    /// Poll every in-flight request once and time out the overdue ones.
    pub fn resolve(&mut self) -> Vec<Completed> {
        let mut done = Vec::new();
        let ids: Vec<RequestId> = self.in_flight.keys().copied().collect();
        for id in ids {
            if let Some(r) = self.cluster.poll_http(id) {
                self.in_flight.remove(&id);
                done.push(Completed {
                    id,
                    status: r.status,
                    body: r.body,
                    content_type: r.content_type.to_owned(),
                });
            } else if self
                .in_flight
                .get(&id)
                .is_some_and(|f| self.now_us >= f.deadline_us)
            {
                self.in_flight.remove(&id);
                self.cluster.forget(id);
                done.push(Completed {
                    id,
                    status: 504,
                    content_type: "application/json".to_owned(),
                    body: serde_json::json!({
                        "__type": "com.amazon.coral.availability#RequestTimeoutException",
                        "message": format!(
                            "no answer within {} virtual seconds (the node may be down or cut off from a quorum)",
                            REQUEST_TIMEOUT.as_secs()
                        ),
                    })
                    .to_string(),
                });
            }
        }
        done
    }

    /// Requests submitted and not yet returned by [`tick`](Self::tick).
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    /// Crash `node` (see `EmbeddedCluster::crash`).
    ///
    /// # Errors
    /// `node` does not exist.
    pub fn crash(&mut self, node: u64) -> Result<(), EmbeddedError> {
        self.cluster.crash(node)
    }

    /// Restart `node`. Requests that were in flight on it complete with `503`
    /// at the next [`tick`](Self::tick).
    ///
    /// # Errors
    /// `node` does not exist.
    pub fn restart(&mut self, node: u64) -> Result<(), EmbeddedError> {
        self.cluster.restart(node)
    }

    /// Partition `a` from `b` until [`heal_all`](Self::heal_all).
    ///
    /// # Errors
    /// A node does not exist, or `a == b`.
    pub fn partition(&mut self, a: u64, b: u64) -> Result<(), EmbeddedError> {
        self.cluster.partition(a, b)
    }

    /// Heal every partition and un-mute every crashed node.
    pub fn heal_all(&mut self) {
        self.cluster.heal_all();
    }

    /// Add a node and return its index. Advances virtual time while it joins.
    ///
    /// # Errors
    /// The cluster is already at [`MAX_NODES`], or a node is down or a
    /// partition is in place (heal first).
    pub fn grow(&mut self) -> Result<u64, EmbeddedError> {
        if self.cluster.node_count() >= MAX_NODES {
            return Err(EmbeddedError(format!(
                "the demo runs at most {MAX_NODES} nodes"
            )));
        }
        let node = self.cluster.grow()?;
        self.sync_clock();
        Ok(node)
    }

    /// The cluster snapshot.
    #[must_use]
    pub fn status(&self) -> ClusterStatus {
        self.cluster.status()
    }

    /// The cluster snapshot as a JSON string (what the page renders).
    #[must_use]
    pub fn status_json(&self) -> String {
        serde_json::to_string(&self.cluster.status()).unwrap_or_else(|e| {
            serde_json::json!({ "error": format!("status not serializable: {e}") }).to_string()
        })
    }
}

/// Virtual microseconds a tick of `real_ms` at `speed` is worth.
fn virtual_micros(real_ms: f64, speed: f64) -> u64 {
    if !real_ms.is_finite() || !speed.is_finite() || real_ms <= 0.0 || speed <= 0.0 {
        return 0;
    }
    let virtual_ms = real_ms.min(MAX_TICK_REAL_MS) * speed.min(MAX_SPEED);
    // Truncating cast of a non-negative, bounded value (<= 100 * 100 * 1000).
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let us = (virtual_ms * 1000.0) as u64;
    us
}

#[cfg(test)]
mod tests {
    use super::*;

    const CREATE: &str = "DynamoDB_20120810.CreateTable";
    const PUT: &str = "DynamoDB_20120810.PutItem";
    const GET: &str = "DynamoDB_20120810.GetItem";

    const CREATE_BODY: &str = r#"{"TableName":"tbl","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"}"#;

    /// Tick in 16 ms frames until `id` resolves (at most 60 virtual seconds).
    fn resolve_id(core: &mut Core, id: RequestId) -> Completed {
        for _ in 0..4000 {
            if let Some(c) = core.tick(16.0, 1.0).into_iter().find(|c| c.id == id) {
                return c;
            }
        }
        panic!("request {id} never resolved");
    }

    fn run(core: &mut Core, node: u64, target: &str, body: &str) -> Completed {
        let id = core.submit(node, target, body).expect("submit");
        resolve_id(core, id)
    }

    #[test]
    fn virtual_micros_clamps_and_rejects_garbage() {
        assert_eq!(virtual_micros(16.0, 1.0), 16_000);
        assert_eq!(virtual_micros(16.0, 2.5), 40_000);
        assert_eq!(virtual_micros(10_000.0, 1.0), 100_000);
        assert_eq!(virtual_micros(100.0, 1000.0), 10_000_000);
        for (r, s) in [
            (f64::NAN, 1.0),
            (1.0, f64::INFINITY),
            (-1.0, 1.0),
            (1.0, 0.0),
        ] {
            assert_eq!(virtual_micros(r, s), 0);
        }
    }

    #[test]
    fn table_put_get_resolve_through_the_pump() {
        let mut core = Core::new(7, 3).expect("cluster");
        let create = run(&mut core, 0, CREATE, CREATE_BODY);
        assert_eq!(create.status, 200, "{}", create.body);
        let put = run(
            &mut core,
            1,
            PUT,
            r#"{"TableName":"tbl","Item":{"id":{"S":"a"},"v":{"S":"hello"}}}"#,
        );
        assert_eq!(put.status, 200, "{}", put.body);
        let get = run(
            &mut core,
            2,
            GET,
            r#"{"TableName":"tbl","Key":{"id":{"S":"a"}},"ConsistentRead":true}"#,
        );
        assert_eq!(get.status, 200, "{}", get.body);
        assert!(get.body.contains("hello"), "{}", get.body);
        assert_eq!(core.in_flight(), 0);
    }

    #[test]
    fn same_seed_same_calls_same_bytes() {
        let drive = || {
            let mut core = Core::new(11, 3).expect("cluster");
            let a = run(&mut core, 0, CREATE, CREATE_BODY);
            let b = run(
                &mut core,
                0,
                PUT,
                r#"{"TableName":"tbl","Item":{"id":{"S":"k"}}}"#,
            );
            (a.body, b.body, core.status_json())
        };
        assert_eq!(drive(), drive());
    }

    #[test]
    fn unknown_node_is_an_error_not_a_panic() {
        let mut core = Core::new(1, 3).expect("cluster");
        assert!(core.submit(9, PUT, "{}").is_err());
        assert!(core.crash(9).is_err());
        assert!(core.partition(0, 0).is_err());
        assert!(Core::new(1, 0).is_err());
        assert!(Core::new(1, MAX_NODES + 1).is_err());
    }

    #[test]
    fn request_to_a_down_node_is_a_503_and_survivors_still_serve() {
        let mut core = Core::new(3, 3).expect("cluster");
        assert_eq!(run(&mut core, 0, CREATE, CREATE_BODY).status, 200);
        core.crash(0).expect("crash");
        assert_eq!(
            run(
                &mut core,
                0,
                GET,
                r#"{"TableName":"tbl","Key":{"id":{"S":"a"}},"ConsistentRead":true}"#
            )
            .status,
            503
        );
        let put = run(
            &mut core,
            1,
            PUT,
            r#"{"TableName":"tbl","Item":{"id":{"S":"b"}}}"#,
        );
        assert_eq!(put.status, 200, "{}", put.body);
        core.heal_all();
        core.restart(0).expect("restart");
        let get = run(
            &mut core,
            0,
            GET,
            r#"{"TableName":"tbl","Key":{"id":{"S":"b"}},"ConsistentRead":true}"#,
        );
        assert_eq!(get.status, 200, "{}", get.body);
        assert!(get.body.contains("\"b\""), "{}", get.body);
    }

    #[test]
    fn stuck_request_times_out_with_504() {
        // Cut node 0 off from both peers: it cannot reach a control quorum, so
        // a CreateTable sent to it never completes.
        let mut core = Core::new(5, 3).expect("cluster");
        core.partition(0, 1).expect("partition");
        core.partition(0, 2).expect("partition");
        let c = run(&mut core, 0, CREATE, CREATE_BODY);
        assert!(
            c.status == 504 || c.status >= 400,
            "an isolated node must not acknowledge: {} {}",
            c.status,
            c.body
        );
        assert_eq!(core.in_flight(), 0);
    }

    #[test]
    fn grow_a_seven_node_cluster() {
        let mut core = Core::new(9, 7).expect("cluster");
        assert_eq!(run(&mut core, 0, CREATE, CREATE_BODY).status, 200);
        assert_eq!(core.grow().expect("grow"), 7);
        assert_eq!(core.status().nodes.len(), 8);
    }

    #[test]
    fn grow_while_degraded_is_an_error_and_the_cluster_survives() {
        let mut core = Core::new(6, 3).expect("cluster");
        core.crash(2).expect("crash");
        assert!(core.grow().is_err());
        core.heal_all();
        core.restart(2).expect("restart");
        assert_eq!(run(&mut core, 0, CREATE, CREATE_BODY).status, 200);
        assert_eq!(core.grow().expect("grow after heal"), 3);
    }

    /// Whatever a visitor clicks, in any order, must not panic the cluster
    /// (a panic in the browser build leaves the whole object unusable).
    #[test]
    fn random_clicks_never_panic() {
        for seed in 0..3_u64 {
            let mut core = Core::new(seed, 5).expect("cluster");
            let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let mut next = |n: u64| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x % n
            };
            let mut submitted = 0_usize;
            let mut done = 0_usize;
            for step in 0..60 {
                let nodes = core.status().nodes.len() as u64;
                match next(9) {
                    0 => drop(core.crash(next(nodes))),
                    1 => drop(core.restart(next(nodes))),
                    2 => drop(core.partition(next(nodes), next(nodes))),
                    3 => core.heal_all(),
                    4 if step % 7 == 0 => drop(core.grow()),
                    5 => {
                        drop(core.submit(next(nodes), CREATE, CREATE_BODY));
                        submitted += 1;
                    }
                    6 => {
                        drop(core.submit(
                            next(nodes),
                            PUT,
                            r#"{"TableName":"tbl","Item":{"id":{"S":"k"}}}"#,
                        ));
                        submitted += 1;
                    }
                    7 => {
                        const PATHS: [&str; 5] = [
                            "/console/api/tables",
                            "/admin/status",
                            "/admin/raftkv",
                            "/admin/storage/lsm?tablet=1",
                            "/admin/peers",
                        ];
                        drop(core.submit_http(next(nodes), "GET", PATHS[next(5) as usize], ""));
                        submitted += 1;
                    }
                    _ => {}
                }
                for _ in 0..next(20) {
                    done += core.tick(16.0, 1.0).len();
                }
            }
            // Everything submitted resolves (answered or timed out).
            for _ in 0..40 {
                done += core.tick(100.0, 10.0).len();
            }
            assert!(done <= submitted, "seed={seed}");
            assert_eq!(core.in_flight(), 0, "seed={seed}");
        }
    }

    #[test]
    fn console_and_dashboard_requests_resolve_through_the_pump() {
        let mut core = Core::new(8, 3).expect("cluster");
        assert_eq!(run(&mut core, 0, CREATE, CREATE_BODY).status, 200);
        let id = core
            .submit_http(1, "GET", "/console/api/tables", "")
            .expect("submit");
        let tables = resolve_id(&mut core, id);
        assert_eq!(tables.status, 200, "{}", tables.body);
        assert!(tables.body.contains("tbl"), "{}", tables.body);
        assert_eq!(tables.content_type, "application/json");
        let id = core
            .submit_http(2, "GET", "/admin/status", "")
            .expect("submit");
        let status = resolve_id(&mut core, id);
        assert_eq!(status.status, 200, "{}", status.body);
        assert!(status.body.contains("tbl"), "{}", status.body);
        assert!(core.submit_http(7, "GET", "/admin/status", "").is_err());
        assert_eq!(core.in_flight(), 0);
    }

    #[test]
    fn grow_adds_a_node_and_advances_the_clock() {
        let mut core = Core::new(2, 3).expect("cluster");
        let before = core.status().virtual_time_ms;
        let node = core.grow().expect("grow");
        assert_eq!(node, 3);
        let status = core.status();
        assert_eq!(status.nodes.len(), 4);
        assert!(status.virtual_time_ms > before);
    }

    #[test]
    fn status_json_has_the_fields_the_page_reads() {
        let core = Core::new(4, 3).expect("cluster");
        let v: serde_json::Value = serde_json::from_str(&core.status_json()).expect("json");
        for key in [
            "seed",
            "virtual_time_ms",
            "control_leader",
            "nodes",
            "tables",
            "tablets",
            "partitions",
        ] {
            assert!(v.get(key).is_some(), "missing {key}: {v}");
        }
        assert_eq!(v["nodes"].as_array().map(Vec::len), Some(3));
    }
}
