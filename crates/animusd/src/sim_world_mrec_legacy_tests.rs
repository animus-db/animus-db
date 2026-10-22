//! ADR 0075's 2026-10-21 amendment: the **legacy (2017.11.29) global tables
//! API** over the DynamoDB wire between three real 3-node `SimCluster`s in one
//! `SimWorld` (Regions `a`, `b`, `c`; the full-mesh harness of
//! `sim_world_mrec_e2e_tests`).
//!
//! Every cluster is finalized to `MAX_SUPPORTED` (so `Gate::LegacyGlobalTables`
//! is open) and holds a pre-created empty `tbl` with a `NEW_AND_OLD_IMAGES`
//! stream, as the legacy API requires. Per seed:
//!
//! * `lifecycle`: **fault** a partition during `CreateGlobalTable` (the
//!   verification cannot reach Region `b`: a retryable `InternalServerError`,
//!   nothing created), heal, create `[a, b]`; a second partition right after
//!   the create stalls the replica, the heal lets it converge; writes in each
//!   Region converge; `UpdateGlobalTable` adds `c` (which receives the rows)
//!   and later removes it; `Describe`/`ListGlobalTables` agree on every
//!   cluster; `Describe`/`UpdateGlobalTableSettings` converge.
//! * `errors`: a missing table in a Region, a missing/wrong-view-type stream,
//!   a non-empty table, a bad `ReplicationGroup`, `GlobalTableAlreadyExists`,
//!   `ReplicaAlreadyExists`, `ReplicaNotFound` and `GlobalTableNotFound` are
//!   each the exception type the AWS model lists for the operation.
//!
//! `ANIMUS_MREC_WORLD_SEEDS=K` sets the depth (>= 20 seeds), `ANIMUS_SEED=<s>`
//! replays one.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use serde_json::Value;

use super::mrec_peer::{MrecConfig, PeerClient};
use super::mrec_saga::mrec_saga_table;
use super::mrec_shipper::mrec_ship_table;
use super::sim_world::{LinkConfig, PeerHandler, SimWorld};
use super::sim_world_mrec_e2e_tests::mesh_cfg;
use super::sim_world_mrec_overrides_tests::open_overrides_gate;
use super::sim_world_mrec_saga_tests::{NODES, item_body, key_body, saga_seeds};
use super::sim_world_mrec_tests::{A, B, LAT, TABLE, handler, open_mrec_gate};

const C: usize = 2;
const REGIONS: [&str; 3] = ["a", "b", "c"];
const NEW_AND_OLD: &str = r#"{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}"#;
const KEYS_ONLY: &str = r#"{"StreamEnabled":true,"StreamViewType":"KEYS_ONLY"}"#;

/// `(region, billing mode, read units, write units)`.
type ReplicaSettingsRow = (String, Option<String>, Option<u64>, Option<u64>);

struct L {
    w: SimWorld,
    cfgs: Vec<Arc<MrecConfig>>,
    seed: u64,
}

impl L {
    /// Three clusters at `MAX_SUPPORTED`. `tables`: which clusters get an
    /// (empty, stream-less) `tbl`.
    fn new(seed: u64, tables: [bool; 3]) -> L {
        let mut w = SimWorld::new(seed, 3, 3, 3);
        for c in 0..3 {
            open_mrec_gate(&mut w.clusters[c], seed);
            open_overrides_gate(&mut w.clusters[c], seed);
        }
        for (c, has_table) in tables.into_iter().enumerate() {
            if has_table {
                w.clusters[c].create_table(TABLE);
            }
        }
        w.sync_clocks();
        w.bridge().set_default_link(LinkConfig::new(LAT));
        let cfgs: Vec<Arc<MrecConfig>> = (0..3).map(mesh_cfg).collect();
        for (c, cfg) in cfgs.iter().enumerate() {
            w.clusters[c].handle().set_mrec_config(cfg.clone());
            let h: PeerHandler = handler(cfg.as_ref().clone(), Arc::new(AtomicU64::new(0)));
            w.set_handler(c, h);
            cfg.edge_client.install(Arc::new(w.peer_client(c)));
        }
        L { w, cfgs, seed }
    }

    fn call(&mut self, c: usize, op: &str, body: &str) -> (u16, Value) {
        let (st, r) = self.w.dynamo(c, 0, op, body);
        let v = serde_json::from_str(&r).unwrap_or(Value::String(r));
        (st, v)
    }

    fn ok(&mut self, c: usize, op: &str, body: &str) -> Value {
        let (st, v) = self.call(c, op, body);
        assert_eq!(st, 200, "seed={}: {op} on {c}: {v}", self.seed);
        v
    }

    /// The call must fail with `status` and the exception type `code`.
    fn err(&mut self, c: usize, op: &str, body: &str, status: u16, code: &str) -> String {
        let (st, v) = self.call(c, op, body);
        let ty = v["__type"].as_str().unwrap_or("").to_owned();
        assert!(
            st == status && ty.ends_with(code),
            "seed={}: {op} on {c}: wanted {status} {code}, got {st} {v}",
            self.seed
        );
        v["message"].as_str().unwrap_or("").to_owned()
    }

    fn stream(&mut self, c: usize, spec: &str) {
        self.ok(
            c,
            "UpdateTable",
            &format!(r#"{{"TableName":"{TABLE}","StreamSpecification":{spec}}}"#),
        );
    }

    fn put(&mut self, c: usize, k: &str, v: &str) {
        self.ok(c, "PutItem", &item_body(k, v, ""));
    }

    fn read(&mut self, c: usize, k: &str) -> Option<String> {
        let (st, v) = self.call(c, "GetItem", &key_body(k, true));
        if st != 200 {
            return None;
        }
        v["Item"]["v"]["S"].as_str().map(str::to_owned)
    }

    fn step(&mut self) {
        for c in 0..3 {
            for node in 0..NODES {
                let mut ctx = self.w.clusters[c].handle().node_ctx(node);
                ctx.mrec = self.cfgs[c].clone();
                let client = self.w.peer_client(c);
                let seed = self.seed;
                self.w
                    .drive(c, node, Duration::from_secs(90), async move {
                        let _ = mrec_saga_table(&ctx, TABLE, &client as &dyn PeerClient).await;
                        let _ = mrec_ship_table(&ctx, TABLE, &client as &dyn PeerClient).await;
                    })
                    .unwrap_or_else(|| panic!("seed={seed}: step {c}/{node} did not finish"));
            }
        }
        self.w.run_for(Duration::from_millis(250));
    }

    fn steps(&mut self, n: usize) {
        for _ in 0..n {
            self.step();
        }
    }

    fn until(&mut self, what: &str, cond: impl Fn(&mut L) -> bool) {
        for _ in 0..300 {
            if cond(self) {
                return;
            }
            self.step();
        }
        let groups: Vec<_> = (0..3).map(|c| self.group(c)).collect();
        panic!(
            "seed={}: timed out waiting for {what}; groups {groups:?}",
            self.seed
        );
    }

    /// `region -> status` of the global table as cluster `c` describes it
    /// (empty when `c` does not know it).
    fn group(&mut self, c: usize) -> Vec<(String, String)> {
        let (st, v) = self.call(c, "DescribeGlobalTable", &gt_body());
        if st != 200 {
            return Vec::new();
        }
        let mut g: Vec<(String, String)> = v["GlobalTableDescription"]["ReplicationGroup"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|r| {
                (
                    r["RegionName"].as_str().unwrap_or("").to_owned(),
                    r["ReplicaStatus"].as_str().unwrap_or("").to_owned(),
                )
            })
            .collect();
        g.sort();
        g
    }

    fn active(&mut self, c: usize, regions: &[&str]) -> bool {
        let want: Vec<(String, String)> = regions
            .iter()
            .map(|r| ((*r).to_owned(), "ACTIVE".to_owned()))
            .collect();
        self.group(c) == want
    }

    fn settings(&mut self, c: usize) -> Vec<ReplicaSettingsRow> {
        let (st, v) = self.call(c, "DescribeGlobalTableSettings", &gt_body());
        if st != 200 {
            return Vec::new();
        }
        v["ReplicaSettings"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|r| {
                (
                    r["RegionName"].as_str().unwrap_or("").to_owned(),
                    r["ReplicaBillingModeSummary"]["BillingMode"]
                        .as_str()
                        .map(str::to_owned),
                    r["ReplicaProvisionedReadCapacityUnits"].as_u64(),
                    r["ReplicaProvisionedWriteCapacityUnits"].as_u64(),
                )
            })
            .collect()
    }

    fn listed(&mut self, c: usize) -> Vec<(String, BTreeSet<String>)> {
        let v = self.ok(c, "ListGlobalTables", "{}");
        v["GlobalTables"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|g| {
                (
                    g["GlobalTableName"].as_str().unwrap_or("").to_owned(),
                    g["ReplicationGroup"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default()
                        .iter()
                        .map(|r| r["RegionName"].as_str().unwrap_or("").to_owned())
                        .collect(),
                )
            })
            .collect()
    }
}

fn gt_body() -> String {
    format!(r#"{{"GlobalTableName":"{TABLE}"}}"#)
}

fn create_gt(regions: &[&str]) -> String {
    let group: Vec<String> = regions
        .iter()
        .map(|r| format!(r#"{{"RegionName":"{r}"}}"#))
        .collect();
    format!(
        r#"{{"GlobalTableName":"{TABLE}","ReplicationGroup":[{}]}}"#,
        group.join(",")
    )
}

fn update_gt(action: &str, region: &str) -> String {
    format!(
        r#"{{"GlobalTableName":"{TABLE}","ReplicaUpdates":[{{"{action}":{{"RegionName":"{region}"}}}}]}}"#
    )
}

#[test]
fn legacy_global_table_lifecycle_converges_under_partitions() {
    for seed in saga_seeds() {
        let mut l = L::new(seed, [true; 3]);
        for c in 0..3 {
            l.stream(c, NEW_AND_OLD);
        }

        // Fault 1: a partition during CreateGlobalTable. The verification
        // cannot reach `b`; nothing is created anywhere.
        l.w.bridge().partition(A, B);
        let msg = l.err(
            A,
            "CreateGlobalTable",
            &create_gt(&["a", "b"]),
            500,
            "InternalServerError",
        );
        assert!(msg.contains("nothing was changed"), "seed={seed}: {msg}");
        l.err(
            A,
            "DescribeGlobalTable",
            &gt_body(),
            400,
            "GlobalTableNotFoundException",
        );
        l.w.bridge().heal();

        let created = l.ok(A, "CreateGlobalTable", &create_gt(&["a", "b"]));
        let d = &created["GlobalTableDescription"];
        assert_eq!(d["GlobalTableName"], TABLE, "seed={seed}: {created}");
        assert_eq!(
            d["ReplicationGroup"].as_array().map(Vec::len),
            Some(2),
            "seed={seed}: {created}"
        );

        // Fault 2: the replica cannot be built while the clusters are cut
        // off; it converges after the heal.
        l.w.bridge().partition(A, B);
        l.steps(6);
        assert!(
            !l.active(A, &["a", "b"]),
            "seed={seed}: ACTIVE while partitioned: {:?} / {:?}",
            l.group(A),
            l.w.bridge_log().iter().rev().take(8).collect::<Vec<_>>()
        );
        l.w.bridge().heal();
        l.until("a and b active", |l| {
            l.active(A, &["a", "b"]) && l.active(B, &["a", "b"])
        });
        for c in [A, B] {
            let (_, v) = l.call(c, "DescribeTable", &format!(r#"{{"TableName":"{TABLE}"}}"#));
            assert_eq!(
                v["Table"]["GlobalTableVersion"], "2017.11.29",
                "seed={seed}: {v}"
            );
        }
        // Already exists, from either end.
        l.err(
            A,
            "CreateGlobalTable",
            &create_gt(&["a", "b"]),
            400,
            "GlobalTableAlreadyExistsException",
        );

        // Writes in each Region converge.
        l.put(A, "ka", "from-a");
        l.put(B, "kb", "from-b");
        l.until("a/b writes converge", |l| {
            [A, B].iter().all(|&c| {
                l.read(c, "ka").as_deref() == Some("from-a")
                    && l.read(c, "kb").as_deref() == Some("from-b")
            })
        });

        // UpdateGlobalTable adds `c` (an empty table in the new Region); it
        // receives the existing rows.
        l.err(
            A,
            "UpdateGlobalTable",
            &update_gt("Create", "b"),
            400,
            "ReplicaAlreadyExistsException",
        );
        l.ok(A, "UpdateGlobalTable", &update_gt("Create", "c"));
        l.until("the three-way group", |l| {
            (0..3).all(|c| l.active(c, &REGIONS))
        });
        l.until("c holds the pre-existing rows", |l| {
            l.read(C, "ka").as_deref() == Some("from-a")
                && l.read(C, "kb").as_deref() == Some("from-b")
        });
        l.put(C, "kc", "from-c");
        l.until("a write in each Region converges", |l| {
            (0..3).all(|c| l.read(c, "kc").as_deref() == Some("from-c"))
        });

        // Describe and List agree on every cluster.
        let want: BTreeSet<String> = REGIONS.iter().map(|r| (*r).to_owned()).collect();
        for c in 0..3 {
            assert_eq!(
                l.listed(c),
                vec![(TABLE.to_owned(), want.clone())],
                "seed={seed}: ListGlobalTables on {c}"
            );
        }

        // Settings: PAY_PER_REQUEST everywhere, then PROVISIONED writes and a
        // per-Region read capacity converge on every cluster.
        for c in 0..3 {
            let s = l.settings(c);
            assert_eq!(s.len(), 3, "seed={seed}: {s:?}");
            assert!(
                s.iter()
                    .all(|r| r.1.as_deref() == Some("PAY_PER_REQUEST") && r.3.is_none()),
                "seed={seed}: settings on {c}: {s:?}"
            );
        }
        l.ok(
            A,
            "UpdateGlobalTableSettings",
            &format!(
                r#"{{"GlobalTableName":"{TABLE}","GlobalTableBillingMode":"PROVISIONED","GlobalTableProvisionedWriteCapacityUnits":100,"ReplicaSettingsUpdate":[{{"RegionName":"b","ReplicaProvisionedReadCapacityUnits":7}}]}}"#
            ),
        );
        l.until("provisioned settings on every cluster", |l| {
            (0..3).all(|c| {
                let s = l.settings(c);
                s.len() == 3
                    && s.iter()
                        .all(|r| r.1.as_deref() == Some("PROVISIONED") && r.3 == Some(100))
                    && s.iter().any(|r| r.0 == "b" && r.2 == Some(7))
            })
        });
        l.err(
            A,
            "UpdateGlobalTableSettings",
            &format!(
                r#"{{"GlobalTableName":"{TABLE}","ReplicaSettingsUpdate":[{{"RegionName":"z","ReplicaProvisionedReadCapacityUnits":3}}]}}"#
            ),
            400,
            "ReplicaNotFoundException",
        );

        // UpdateGlobalTable removes `c`: a and b keep replicating, c leaves.
        l.ok(A, "UpdateGlobalTable", &update_gt("Delete", "c"));
        l.until("c removed from a and b", |l| {
            l.active(A, &["a", "b"]) && l.active(B, &["a", "b"])
        });
        l.until("c no longer a global table", |l| l.group(C).is_empty());
        l.put(B, "kb2", "after-removal");
        l.until("a/b still replicate", |l| {
            l.read(A, "kb2").as_deref() == Some("after-removal")
        });
        assert_eq!(
            l.read(C, "kb2"),
            None,
            "seed={seed}: c still receives writes after its removal"
        );
        l.err(
            A,
            "UpdateGlobalTable",
            &update_gt("Delete", "c"),
            400,
            "ReplicaNotFoundException",
        );
    }
}

#[test]
fn legacy_global_table_errors_are_the_modeled_exceptions() {
    for seed in saga_seeds() {
        // `c` has no table at all yet.
        let mut l = L::new(seed, [true, true, false]);
        let read_body = gt_body();
        for op in ["DescribeGlobalTable", "DescribeGlobalTableSettings"] {
            l.err(A, op, &read_body, 400, "GlobalTableNotFoundException");
        }
        l.err(
            A,
            "UpdateGlobalTable",
            &update_gt("Create", "b"),
            400,
            "GlobalTableNotFoundException",
        );
        l.err(
            A,
            "UpdateGlobalTableSettings",
            &format!(
                r#"{{"GlobalTableName":"{TABLE}","GlobalTableBillingMode":"PAY_PER_REQUEST"}}"#
            ),
            400,
            "GlobalTableNotFoundException",
        );
        assert!(l.listed(A).is_empty(), "seed={seed}");

        // The local table has no stream.
        l.err(
            A,
            "CreateGlobalTable",
            &create_gt(&["a", "b"]),
            400,
            "ValidationException",
        );
        l.stream(A, NEW_AND_OLD);
        // The ReplicationGroup must name this cluster's Region, at least two
        // Regions, and configured peers.
        for group in [vec!["b", "c"], vec!["a"], vec!["a", "z"]] {
            l.err(
                A,
                "CreateGlobalTable",
                &create_gt(&group),
                400,
                "ValidationException",
            );
        }
        // `b` has no stream.
        l.err(
            A,
            "CreateGlobalTable",
            &create_gt(&["a", "b"]),
            400,
            "ValidationException",
        );
        // `c` has no table: TableNotFoundException.
        l.err(
            A,
            "CreateGlobalTable",
            &create_gt(&["a", "c"]),
            400,
            "TableNotFoundException",
        );
        // `c` has the table with the wrong stream view type.
        l.w.clusters[C].create_table(TABLE);
        l.stream(C, KEYS_ONLY);
        let m = l.err(
            A,
            "CreateGlobalTable",
            &create_gt(&["a", "c"]),
            400,
            "ValidationException",
        );
        assert!(m.contains("NEW_AND_OLD_IMAGES"), "seed={seed}: {m}");
        // `b` has the right stream but holds an item.
        l.stream(B, NEW_AND_OLD);
        l.put(B, "x", "y");
        let m = l.err(
            A,
            "CreateGlobalTable",
            &create_gt(&["a", "b"]),
            400,
            "ValidationException",
        );
        assert!(m.contains("empty"), "seed={seed}: {m}");
        // Nothing was created by any refused call.
        assert!(
            l.listed(A).is_empty() && l.group(A).is_empty(),
            "seed={seed}"
        );

        // Empty `b`, a correct stream on `c`: the create goes through.
        l.ok(B, "DeleteItem", &key_body("x", false));
        l.stream(C, r#"{"StreamEnabled":false}"#);
        l.stream(C, NEW_AND_OLD);
        l.ok(A, "CreateGlobalTable", &create_gt(&["a", "b"]));
        l.until("a and b active", |l| {
            l.active(A, &["a", "b"]) && l.active(B, &["a", "b"])
        });
        l.err(
            B,
            "CreateGlobalTable",
            &create_gt(&["a", "b"]),
            400,
            "GlobalTableAlreadyExistsException",
        );
        l.err(
            B,
            "UpdateGlobalTable",
            &update_gt("Create", "a"),
            400,
            "ReplicaAlreadyExistsException",
        );
        l.err(
            B,
            "UpdateGlobalTable",
            &update_gt("Delete", "c"),
            400,
            "ReplicaNotFoundException",
        );
    }
}

#[test]
fn removing_two_regions_in_one_call_leaves_no_link_between_them() {
    for seed in saga_seeds() {
        let mut l = L::new(seed, [true; 3]);
        for c in 0..3 {
            l.stream(c, NEW_AND_OLD);
        }
        l.ok(A, "CreateGlobalTable", &create_gt(&["a", "b", "c"]));
        l.until("the three-way group", |l| {
            (0..3).all(|c| l.active(c, &REGIONS))
        });
        l.put(A, "k0", "v0");
        l.until("k0 everywhere", |l| {
            (0..3).all(|c| l.read(c, "k0").as_deref() == Some("v0"))
        });

        // One call removes both `b` and `c`; the clusters are partitioned
        // while the removal is carried out, then healed.
        l.ok(
            A,
            "UpdateGlobalTable",
            &format!(
                r#"{{"GlobalTableName":"{TABLE}","ReplicaUpdates":[{{"Delete":{{"RegionName":"b"}}}},{{"Delete":{{"RegionName":"c"}}}}]}}"#
            ),
        );
        l.w.bridge().partition(A, B);
        l.steps(3);
        l.w.bridge().heal();
        l.until("every cluster stopped being a global table", |l| {
            (0..3).all(|c| l.group(c).is_empty())
        });
        l.steps(4);

        // No replication link remains: a write in any Region stays there.
        for (from, key) in [(B, "kb"), (C, "kc"), (A, "ka")] {
            l.put(from, key, "x");
            l.steps(8);
            for other in (0..3).filter(|c| *c != from) {
                assert_eq!(
                    l.read(other, key),
                    None,
                    "seed={seed}: write {key} on {from} reached {other} after the removal"
                );
            }
        }
    }
}
