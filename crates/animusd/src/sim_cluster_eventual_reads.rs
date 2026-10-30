//! Issues #1419 (option B) and #1424: the contract of the three forwarded
//! eventual-read arms (`Get`, `Scan`, `KindScan` with `stale: true`, served
//! by `ClientCtx::serve_stale_read`) and of the local eventual path
//! (`read_path.rs`), pinned over a real `SimCluster` with the LSM engine:
//!
//! - **served**: the pre-charge (0.5 RCU) is corrected to the real byte cost;
//! - **not served** (the engine failed under the read): the pre-charge is
//!   **refunded**, the forwarded arm refuses, and the local path errors;
//! - **a failed scan is an error, never an empty page** (to a DynamoDB client
//!   an empty page is a fabricated deletion): the forwarded arm answers
//!   `ClientResponse::Error`, the local path a retryable `"; retry"` error,
//!   and the DynamoDB wire a 503, not a 200 with no items.
//!
//! The fault is a real read failure: flush the LSM so the read goes to
//! SSTable blocks, then `DiskConfig::set_error_prob(1.0)`.

use std::time::Duration;

use animus_node::{ClientRequest, ClientResponse};
use animus_tablet::TabletId;

use super::sim_cluster::SimCluster;
use super::sim_support::env_seed;
use crate::ReadConsistency;

const TABLE: &str = "evt";
const RECORDS: u64 = 4;

fn await_slot<T: Clone>(
    cluster: &mut SimCluster,
    slot: &std::sync::Arc<std::sync::Mutex<Option<T>>>,
    what: &str,
) -> T {
    let mut elapsed = Duration::ZERO;
    loop {
        if let Some(v) = slot.lock().expect("slot poisoned").clone() {
            return v;
        }
        assert!(
            elapsed < Duration::from_secs(30),
            "seed={}: {what} did not resolve",
            cluster.seed()
        );
        cluster.run_for(Duration::from_millis(5));
        elapsed += Duration::from_millis(5);
    }
}

struct Rig {
    cluster: SimCluster,
    node: u64,
    tablet: TabletId,
}

/// A streamed table (so the change kind holds rows too) with `RECORDS` items,
/// flushed to SSTables, a 1 RCU/s read limit (a 300-unit burst, so refill
/// during a test is negligible), and the bucket primed by one served read.
fn setup(seed: u64) -> Rig {
    let mut cluster =
        SimCluster::new_with_lsm_engines(seed, &[crate::config::NodeRole::Both; 3], 3, None);
    let body = format!(
        r#"{{"TableName":"{TABLE}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}],
            "StreamSpecification":{{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
    );
    let (s, b) = cluster.dynamo(0, "DynamoDB_20120810.CreateTable", body.as_bytes());
    assert_eq!(s, 200, "seed={seed}: CreateTable: {b}");
    let tablet = *cluster
        .metadata(0)
        .tablets_for_table(TABLE)
        .next()
        .expect("tablet")
        .0;
    let node = (0..3u64)
        .find(|&n| cluster.is_leader_local(n, tablet))
        .expect("leader");
    for i in 0..RECORDS {
        let put = format!(r#"{{"TableName":"{TABLE}","Item":{{"pk":{{"S":"r{i}"}}}}}}"#);
        let (s, b) = cluster.dynamo(node, "DynamoDB_20120810.PutItem", put.as_bytes());
        assert_eq!(s, 200, "seed={seed}: put: {b}");
    }
    let flushed = cluster.spawn_detached(node, move |ctx| async move {
        let (_, group) = ctx
            .edge
            .hosted_groups()
            .into_iter()
            .find(|(t, _)| *t == tablet)
            .expect("hosted");
        match &group {
            crate::CpGroup::Lsm(n) => n.storage().flush_now().await.map_err(|e| e.to_string()),
            crate::CpGroup::Mem(_) => Err("expected an LSM engine".to_owned()),
        }
    });
    assert_eq!(await_slot(&mut cluster, &flushed, "flush"), Ok(()));
    cluster.run_for(Duration::from_secs(2));
    cluster.set_throttle_defaults_all(Some(1), None);
    let mut rig = Rig {
        cluster,
        node,
        tablet,
    };
    // Prime the bucket (it is created lazily at the first check).
    let primed = rig.forwarded(scan_req());
    assert!(
        matches!(primed, ClientResponse::Pairs(ref p) if p.len() as u64 == RECORDS),
        "seed={seed}: priming scan: {primed:?}"
    );
    rig
}

fn scan_req() -> ClientRequest {
    ClientRequest::Scan {
        start: Vec::new(),
        end: None,
        limit: None,
        reverse: false,
        table: TABLE.into(),
        stale: true,
    }
}

fn kind_scan_req() -> ClientRequest {
    ClientRequest::KindScan {
        table: TABLE.into(),
        kind: animus_cp_data::KIND_CHANGE,
        start: Vec::new(),
        end: None,
        limit: None,
        reverse: false,
        stale: true,
    }
}

impl Rig {
    fn forwarded(&mut self, req: ClientRequest) -> ClientResponse {
        let slot = self
            .cluster
            .spawn_detached(self.node, move |ctx| async move {
                ctx.cp_serve_forwarded(req).await
            });
        await_slot(&mut self.cluster, &slot, "forwarded eventual read")
    }

    fn read_tokens(&mut self) -> f64 {
        let tablet = self.tablet;
        let slot = self
            .cluster
            .spawn_detached(self.node, move |ctx| async move {
                ctx.throttle
                    .snapshot()
                    .into_iter()
                    .find(|e| e.tablet == tablet)
                    .map(|e| e.read_tokens)
            });
        await_slot(&mut self.cluster, &slot, "throttle snapshot").expect("bucket exists")
    }

    fn break_disk(&mut self) {
        let mut broken = animus_sim::DiskConfig::default();
        broken.set_error_prob(1.0);
        self.cluster.set_disk_config(broken);
    }

    fn heal_disk(&mut self) {
        self.cluster
            .set_disk_config(animus_sim::DiskConfig::default());
        self.cluster.run_for(Duration::from_secs(2));
    }

    /// Run `req` through the forwarded arm and return `(response, tokens
    /// spent)`.
    fn charged(&mut self, req: ClientRequest) -> (ClientResponse, f64) {
        let before = self.read_tokens();
        let resp = self.forwarded(req);
        let after = self.read_tokens();
        (resp, before - after)
    }
}

fn pairs_bytes(resp: &ClientResponse) -> usize {
    match resp {
        ClientResponse::Pairs(p) => p.iter().map(|(k, v)| k.len() + v.len()).sum(),
        ClientResponse::Value(v) => v.as_ref().map_or(0, Vec::len),
        other => panic!("not a served response: {other:?}"),
    }
}

fn get_req(key: Vec<u8>) -> ClientRequest {
    ClientRequest::Get {
        key,
        table: TABLE.into(),
        stale: true,
    }
}

fn run_forwarded_arms(seed: u64) {
    let mut r = setup(seed);
    // A real key to `Get`.
    let key = match r.forwarded(scan_req()) {
        ClientResponse::Pairs(p) => p[0].0.clone(),
        other => panic!("seed={seed}: {other:?}"),
    };

    // Served: charged exactly the real cost (>= the 0.5 RCU minimum).
    for (what, req) in [
        ("Get", get_req(key.clone())),
        ("Scan", scan_req()),
        ("KindScan", kind_scan_req()),
    ] {
        let (resp, spent) = r.charged(req);
        assert!(
            !matches!(resp, ClientResponse::Error(_)),
            "seed={seed} {what}: expected served, got {resp:?}"
        );
        let want = animus_dynamo::capacity::read_units(pairs_bytes(&resp), false);
        assert!(
            (spent - want).abs() < 0.1,
            "seed={seed} {what}: served read must be charged {want} RCU, spent {spent}"
        );
        assert!(spent >= 0.4, "seed={seed} {what}: minimum charge, {spent}");
    }

    // Not served (the engine fails under the read): an error or a refusal,
    // never an empty page, and the pre-charge is refunded.
    r.break_disk();
    for (what, req) in [
        ("Get", get_req(key.clone())),
        ("Scan", scan_req()),
        ("KindScan", kind_scan_req()),
    ] {
        let (resp, spent) = r.charged(req);
        assert!(
            matches!(resp, ClientResponse::Error(_)),
            "seed={seed} {what}: a failed read must be an error, not {resp:?}"
        );
        assert!(
            spent.abs() < 0.1,
            "seed={seed} {what}: a read that served nothing must be refunded, spent {spent}"
        );
    }
    r.heal_disk();
    // And the same arms serve again once the disk is back.
    let (resp, _) = r.charged(scan_req());
    assert!(
        matches!(resp, ClientResponse::Pairs(ref p) if p.len() as u64 == RECORDS),
        "seed={seed}: after healing: {resp:?}"
    );
}

fn run_local_path(seed: u64) {
    let mut r = setup(seed);
    let node = r.node;
    let call = |r: &mut Rig, scan: bool| {
        let before = r.read_tokens();
        let slot = r.cluster.spawn_detached(node, move |ctx| async move {
            if scan {
                ctx.cp_scan(
                    TABLE,
                    Vec::new(),
                    None,
                    None,
                    false,
                    ReadConsistency::Eventual,
                )
                .await
                .map(|p| p.len())
            } else {
                ctx.cp_scan_kind(
                    TABLE,
                    animus_cp_data::KIND_CHANGE,
                    Vec::new(),
                    vec![0xFF; 64],
                    None,
                    false,
                    ReadConsistency::Eventual,
                )
                .await
                .map(|p| p.len())
            }
        });
        let out = await_slot(&mut r.cluster, &slot, "local eventual scan");
        (out, before - r.read_tokens())
    };
    for scan in [true, false] {
        let (ok, spent) = call(&mut r, scan);
        assert!(
            ok.is_ok_and(|n| n > 0) && spent >= 0.4,
            "seed={seed} scan={scan}: served"
        );
    }
    r.break_disk();
    for scan in [true, false] {
        let (out, spent) = call(&mut r, scan);
        assert!(
            matches!(&out, Err(e) if e.ends_with("; retry")),
            "seed={seed} scan={scan}: a failed local eventual scan must be a retryable \
             error, not an empty page: {out:?}"
        );
        assert!(
            spent.abs() < 0.1,
            "seed={seed} scan={scan}: refund on a failed read, spent {spent}"
        );
    }
    // The DynamoDB wire: a 503, never a 200 with no items.
    let (status, body) = r.cluster.dynamo(
        node,
        "DynamoDB_20120810.Scan",
        format!(r#"{{"TableName":"{TABLE}","ConsistentRead":false}}"#).as_bytes(),
    );
    assert_eq!(
        status, 503,
        "seed={seed}: eventual Scan on a failing engine must be a retryable 503, got {status}: {body}"
    );
    r.heal_disk();
    let (status, body) = r.cluster.dynamo(
        node,
        "DynamoDB_20120810.Scan",
        format!(r#"{{"TableName":"{TABLE}","ConsistentRead":false}}"#).as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: {body}");
    assert!(body.contains("\"Count\":4"), "seed={seed}: {body}");
}

#[test]
fn forwarded_eventual_arms_charge_when_served_refund_when_not_and_never_return_an_empty_page() {
    run_forwarded_arms(env_seed(0xE7E7_0001));
}

#[test]
fn local_eventual_path_refunds_and_errors_on_a_failed_scan() {
    run_local_path(env_seed(0xE7E7_0002));
}

#[test]
fn eventual_read_contract_holds_over_seeds() {
    for i in 0..4 {
        run_forwarded_arms(0xE7E7_0100 + i);
        run_local_path(0xE7E7_0200 + i);
    }
}
