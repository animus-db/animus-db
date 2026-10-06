//! G-01 stage G-d M0: `SimWorld` smoke + determinism (see `sim_world.rs`).
//!
//! Per seed: two 3-node clusters boot; each takes DynamoDB-wire writes
//! independently (distinct data, distinct `Metadata`); a cross-cluster request
//! round-trips through the `PeerBridge` with latency and its handler performs a
//! real wire write on the destination; a partition drops it (timeout), heal
//! restores it; loss and duplication behave. `ANIMUS_SIMWORLD_SEEDS=K` sets the
//! depth (default 2), `ANIMUS_SEED=<s>` replays one seed.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::sim_world::{LinkConfig, PeerClient, PeerError, PeerHandler, SimWorld};

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return vec![s];
    }
    let k: u64 = std::env::var("ANIMUS_SIMWORLD_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    (0..k).map(|i| 0x5D00_0000 + i).collect()
}

const LAT: Duration = Duration::from_millis(40);
const TIMEOUT: Duration = Duration::from_millis(500);

fn create_table(w: &mut SimWorld, c: usize, seed: u64) {
    let body = r#"{"TableName":"tbl","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
        "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}"#;
    let (s, v) = w.dynamo(c, 0, "CreateTable", body);
    assert_eq!(s, 200, "seed={seed}: CreateTable on cluster {c}: {v}");
}

fn put(w: &mut SimWorld, c: usize, node: u64, k: &str, v: &str, seed: u64) {
    let body = format!(r#"{{"TableName":"tbl","Item":{{"pk":{{"S":"{k}"}},"v":{{"S":"{v}"}}}}}}"#);
    let (s, r) = w.dynamo(c, node, "PutItem", &body);
    assert_eq!(s, 200, "seed={seed}: PutItem {k} on cluster {c}: {r}");
}

fn get(w: &mut SimWorld, c: usize, node: u64, k: &str) -> Option<String> {
    let body =
        format!(r#"{{"TableName":"tbl","Key":{{"pk":{{"S":"{k}"}}}},"ConsistentRead":true}}"#);
    let (s, r) = w.dynamo(c, node, "GetItem", &body);
    assert_eq!(s, 200, "GetItem {k} on cluster {c}: {r}");
    let v: serde_json::Value = serde_json::from_str(&r).expect("json");
    v["Item"]["v"]["S"].as_str().map(str::to_owned)
}

/// Handler: payload `put:<k>` writes `<k>` on the destination cluster through
/// its own wire, answering `ok:<k>`; anything else echoes.
fn put_handler(hits: Arc<Mutex<u32>>) -> PeerHandler {
    Arc::new(move |handle, payload: Vec<u8>| {
        let hits = hits.clone();
        Box::pin(async move {
            *hits.lock().unwrap() += 1;
            let text = String::from_utf8_lossy(&payload).into_owned();
            if let Some(k) = text.strip_prefix("put:") {
                let body = format!(
                    r#"{{"TableName":"tbl","Item":{{"pk":{{"S":"{k}"}},"v":{{"S":"via-peer"}}}}}}"#
                );
                let (s, _) = handle
                    .dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes())
                    .await;
                format!("{}:{k}", if s == 200 { "ok" } else { "err" }).into_bytes()
            } else {
                payload
            }
        })
    })
}

/// Everything the smoke asserts, returning the world fingerprint.
fn scenario(seed: u64) -> u64 {
    let mut w = SimWorld::new(seed, 2, 3, 3);
    let hits = Arc::new(Mutex::new(0u32));
    w.set_handler(1, put_handler(hits.clone()));
    w.set_handler(0, put_handler(Arc::new(Mutex::new(0))));
    w.bridge().set_default_link(LinkConfig::new(LAT));

    // Independent clusters: same table name, disjoint data.
    for c in 0..2 {
        create_table(&mut w, c, seed);
        put(&mut w, c, 1, &format!("local{c}"), &format!("c{c}"), seed);
    }
    assert_eq!(
        get(&mut w, 0, 2, "local0").as_deref(),
        Some("c0"),
        "seed={seed}"
    );
    assert_eq!(
        get(&mut w, 1, 2, "local1").as_deref(),
        Some("c1"),
        "seed={seed}"
    );
    assert_eq!(
        get(&mut w, 0, 0, "local1"),
        None,
        "seed={seed}: clusters must be independent"
    );
    assert_eq!(
        get(&mut w, 1, 0, "local0"),
        None,
        "seed={seed}: clusters must be independent"
    );

    // Round trip with latency: >= 2 * LAT, and the handler's wire write landed.
    let (r, rtt) = w.peer_call(0, 1, b"put:from-a", TIMEOUT);
    assert_eq!(r.as_deref(), Ok(&b"ok:from-a"[..]), "seed={seed}");
    assert!(
        rtt >= 2 * LAT,
        "seed={seed}: rtt {rtt:?} must include both one-way latencies"
    );
    assert!(rtt < TIMEOUT, "seed={seed}: rtt {rtt:?}");
    assert_eq!(
        get(&mut w, 1, 2, "from-a").as_deref(),
        Some("via-peer"),
        "seed={seed}"
    );
    assert_eq!(get(&mut w, 0, 2, "from-a"), None, "seed={seed}");

    // Partition (one-way then symmetric) drops it; the handler never runs.
    let before = *hits.lock().unwrap();
    w.bridge().partition_one_way(0, 1);
    let (r, _) = w.peer_call(0, 1, b"put:cut", TIMEOUT);
    assert_eq!(r, Err(PeerError::Timeout), "seed={seed}");
    w.bridge().heal();
    w.bridge().partition(0, 1);
    let (r, _) = w.peer_call(1, 0, b"echo", TIMEOUT);
    assert_eq!(r, Err(PeerError::Timeout), "seed={seed}");
    assert_eq!(
        *hits.lock().unwrap(),
        before,
        "seed={seed}: partitioned request must not reach the handler"
    );
    assert_eq!(get(&mut w, 1, 0, "cut"), None, "seed={seed}");

    // Heal restores.
    w.bridge().heal();
    let (r, rtt) = w.peer_call(1, 0, b"echo", TIMEOUT);
    assert_eq!(r.as_deref(), Ok(&b"echo"[..]), "seed={seed}");
    assert!(rtt >= 2 * LAT, "seed={seed}");

    // A partition raised while the request is in flight drops it at delivery.
    let client = w.peer_client(0);
    let r = {
        let b = w.bridge().clone();
        let mut out = None;
        let fut = client.call(1, b"echo".to_vec(), TIMEOUT);
        let slot = Arc::new(Mutex::new(None));
        let s2 = slot.clone();
        w.clusters[0].client_env(2);
        let env = w.clusters[0].client_env(3);
        animus_env::EnvExt::spawn_task(&env, async move {
            *s2.lock().unwrap() = Some(fut.await);
        });
        w.run_for(LAT / 2);
        b.partition(0, 1);
        w.run_for(TIMEOUT * 2);
        if let Some(v) = slot.lock().unwrap().take() {
            out = Some(v);
        }
        out
    };
    assert_eq!(
        r,
        Some(Err(PeerError::Timeout)),
        "seed={seed}: in-flight request must drop at delivery"
    );
    w.bridge().heal();

    // Total loss times out; no loss + jitter + duplication still answers.
    let mut lossy = LinkConfig::new(LAT);
    lossy.loss_permille = 1000;
    w.bridge().set_link(0, 1, lossy);
    let (r, _) = w.peer_call(0, 1, b"echo", TIMEOUT);
    assert_eq!(r, Err(PeerError::Timeout), "seed={seed}");
    let mut noisy = LinkConfig::new(LAT);
    noisy.jitter = Duration::from_millis(30);
    noisy.dup_permille = 1000;
    w.bridge().set_link(0, 1, noisy);
    let (r, rtt) = w.peer_call(0, 1, b"echo", TIMEOUT);
    assert_eq!(r.as_deref(), Ok(&b"echo"[..]), "seed={seed}");
    assert!(
        rtt >= 2 * LAT && rtt <= 2 * (LAT + Duration::from_millis(30)) + Duration::from_millis(40),
        "seed={seed}: rtt {rtt:?}"
    );

    // The log shows each fault mode actually fired.
    let log = w.bridge_log().join("\n");
    for needle in [
        "dropped:partition ",
        "dropped:partition@delivery",
        "dropped:loss",
        "delivered(",
    ] {
        assert!(
            log.contains(needle) || log.contains(needle.trim_end()),
            "seed={seed}: bridge log lacks `{needle}`:\n{log}"
        );
    }

    // Both clusters still serve after all the WAN faults.
    put(&mut w, 0, 2, "after", "x", seed);
    put(&mut w, 1, 2, "after", "y", seed);
    assert_eq!(
        get(&mut w, 0, 0, "after").as_deref(),
        Some("x"),
        "seed={seed}"
    );
    w.fingerprint()
}

#[test]
fn sim_world_smoke_two_clusters_and_peer_bridge() {
    let t = std::time::Instant::now();
    for seed in seeds() {
        scenario(seed);
    }
    eprintln!(
        "sim_world smoke: {:?} for {} seeds",
        t.elapsed(),
        seeds().len()
    );
}

#[test]
fn sim_world_is_deterministic_per_seed() {
    let all = seeds();
    let mut distinct = std::collections::BTreeSet::new();
    for seed in all.iter().copied() {
        let a = scenario(seed);
        let b = scenario(seed);
        assert_eq!(a, b, "seed={seed}: two runs of one seed diverged");
        distinct.insert(a);
    }
    if all.len() > 1 {
        assert!(
            distinct.len() > 1,
            "different seeds must give different runs"
        );
    }
}
