//! Wire-shape smoke test: bring up a real 3-node in-process `animusd` cluster
//! (`ProdEnv`, real sockets, SigV4 auth ON) and drive every YCSB workload A-F
//! through the real `animus-bench` code path in both `ConsistentRead` modes,
//! then a degraded run that kills a follower.
//!
//! This asserts **correctness only** — every request is accepted by the
//! server's SigV4 verifier and parsed, no unexpected errors, every arrival
//! completes, the results JSON round-trips. It never asserts a latency or a
//! rate (that would be the flakiness the green invariant forbids). Real
//! sockets and a real cluster: its own `tests/` target, not part of a merged
//! `it` binary. All waiting is a converged-or-timeout poll inside the library
//! (`await_ready`, `create_table`'s ACTIVE poll, load retry) — no fixed sleep
//! gates correctness here.
//!
//! The second test runs the same cluster shape with **server-only TLS** on
//! every port (ADR 0064) and drives workloads through the TLS client.

use std::path::PathBuf;
use std::sync::Arc;

use animus_bench::cli::{execute, parse_args};
use animus_bench::client::{Conn, Credentials};
use animus_bench::cluster::{Cluster, LaunchTls};
use animus_bench::report::{Report, SCHEMA};
use animus_bench::tls::TlsClient;

fn args(extra: &[&str], dir: &std::path::Path, out: &std::path::Path) -> Vec<String> {
    let mut v: Vec<String> = [
        "--launch",
        "in-process",
        "--cluster-size",
        "3",
        "--records",
        "300",
        "--value-bytes",
        "64",
        "--rate",
        "150",
        "--warmup-secs",
        "0.3",
        "--connections",
        "8",
        "--seed",
        "7",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .collect();
    v.extend(extra.iter().map(|s| (*s).to_owned()));
    v.push("--data-dir".into());
    v.push(dir.display().to_string());
    v.push("--out".into());
    v.push(out.display().to_string());
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_workload_speaks_the_wire_in_both_read_modes_then_survives_a_follower_kill() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // ---- Stage 1: workloads A-F x ConsistentRead {true,false}, no fault.
    let out = tmp.path().join("all.json");
    let argv = args(
        &[
            "--workloads",
            "all",
            "--consistent-read",
            "both",
            "--steady-secs",
            "2",
            "--degraded",
            "none",
        ],
        &tmp.path().join("stage1"),
        &out,
    );
    let opts = parse_args(&argv).expect("parse");
    let report = execute(&opts, argv).await.expect("run");

    // The file on disk is the report, and it parses.
    let on_disk = Report::from_json(&std::fs::read_to_string(&out).expect("read json"))
        .expect("results JSON parses");
    // (Not `assert_eq!(on_disk, report)`: serde_json's default float parsing
    // is not bit-exact, so f64 means can differ by an ULP after a round trip.)
    assert_eq!(on_disk.schema, report.schema);
    assert_eq!(on_disk.git_sha, report.git_sha);
    assert_eq!(on_disk.args, report.args);
    assert_eq!(on_disk.runs.len(), report.runs.len());
    for (a, b) in on_disk.runs.iter().zip(&report.runs) {
        assert_eq!(a.name, b.name);
        assert_eq!(a.phases.len(), b.phases.len());
        for (pa, pb) in a.phases.iter().zip(&b.phases) {
            assert_eq!(pa.completed, pb.completed);
            assert_eq!(pa.overall.corrected.p99_us, pb.overall.corrected.p99_us);
        }
    }
    assert_eq!(report.schema, SCHEMA);
    assert!(
        !report.publishable,
        "a self-launched cluster is never publishable"
    );
    assert!(report.environment.client_and_server_colocated);
    assert!(report.environment.sigv4, "the smoke must exercise SigV4");
    assert_eq!(report.environment.node_count, 3);
    assert_eq!(report.topology_start["node_count"], 3);
    assert!(
        report.topology_start["nodes"]
            .as_array()
            .expect("nodes")
            .iter()
            .all(|n| n["auth_enabled"] == true),
        "every node must report SigV4 enforcement on: {}",
        report.topology_start["nodes"]
    );
    assert_eq!(report.runs.len(), 12, "6 workloads x 2 read modes");

    for run in &report.runs {
        let steady = run
            .phases
            .iter()
            .find(|p| p.name == "steady")
            .unwrap_or_else(|| panic!("{}: no steady phase", run.name));
        assert_eq!(steady.dispatched, 300, "{}: 150/s x 2s", run.name);
        assert_eq!(
            steady.errors.total(),
            0,
            "{}: unexpected errors {:?} {:?}",
            run.name,
            steady.errors,
            steady.error_samples
        );
        assert_eq!(steady.abandoned, 0, "{}", run.name);
        assert_eq!(
            steady.completed, steady.dispatched,
            "{}: every arrival completes",
            run.name
        );
        let warm = run
            .phases
            .iter()
            .find(|p| p.name == "warmup")
            .expect("warmup");
        assert!(
            warm.discarded && warm.overall.count == 0,
            "warm-up is discarded"
        );
        assert_eq!(warm.errors.total(), 0, "{}: warm-up errors", run.name);

        let wl = run.params["workload"].as_str().expect("workload");
        let cr = run.params["consistent_read"].as_bool().expect("cr");
        // Class coverage per workload.
        let has = |c: &str| steady.classes.contains_key(c);
        match wl {
            "A" | "B" => assert!(has("read") && has("update"), "{}", run.name),
            "C" => assert!(has("read") && !has("update"), "{}", run.name),
            "D" => assert!(has("read") && has("insert"), "{}", run.name),
            "E" => assert!(has("scan") && has("insert"), "{}", run.name),
            "F" => assert!(has("read") && has("read_modify_write"), "{}", run.name),
            other => panic!("unexpected workload {other}"),
        }
        // A linearizable read of a loaded, untouched-by-inserts key space
        // must find its record.
        if cr && matches!(wl, "A" | "B" | "C" | "F") {
            assert_eq!(
                steady.empty_reads, 0,
                "{}: consistent read missed a loaded record",
                run.name
            );
        }
        // Only F can lose a conditional-write race.
        if wl != "F" {
            assert_eq!(steady.condition_failed, 0, "{}", run.name);
        }
        // The disclosed parameters are in the file.
        assert_eq!(run.params["record_count"], 300);
        assert_eq!(run.params["value_bytes"], 64);
        assert_eq!(run.params["seed"], 7);
        assert!(
            run.params["table_topology_after_load"]["tablet_count"]
                .as_u64()
                .unwrap_or(0)
                >= 1
        );
    }
    // Each (workload, read mode) ran on its own freshly loaded table, and
    // every run reports its load.
    assert_eq!(report.runs.iter().filter(|r| r.load.is_some()).count(), 12);
    let tables: std::collections::BTreeSet<_> = report
        .runs
        .iter()
        .map(|r| r.params["table"].as_str().expect("table").to_owned())
        .collect();
    assert_eq!(tables.len(), 12, "tables must not be shared across modes");
    for r in &report.runs {
        assert_eq!(r.params["drain_secs"], 30.0);
    }
    // The text summary renders the disclosure.
    let text = report.render_text();
    assert!(text.contains("NOT PUBLISHABLE") && text.contains("ycsb-F/consistent_read=false"));

    // ---- Stage 2: a degraded run (kill a follower of the table's tablet).
    let out2 = tmp.path().join("degraded.json");
    let argv = args(
        &[
            "--workloads",
            "A",
            "--consistent-read",
            "true",
            "--degraded",
            "follower",
            "--baseline-secs",
            "1.5",
            "--degraded-secs",
            "2",
            "--recovery-secs",
            "2",
            "--steady-secs",
            "1",
        ],
        &tmp.path().join("stage2"),
        &out2,
    );
    let opts = parse_args(&argv).expect("parse");
    let report = execute(&opts, argv).await.expect("degraded run");
    let degraded = report
        .runs
        .iter()
        .find(|r| r.name.contains("/degraded/"))
        .expect("degraded run present");
    let phase = |n: &str| {
        degraded
            .phases
            .iter()
            .find(|p| p.name == n)
            .unwrap_or_else(|| panic!("no {n} phase"))
    };
    assert_eq!(
        phase("baseline").errors.total(),
        0,
        "healthy baseline has no errors"
    );
    let fault = phase("degraded").fault.clone().expect("fault recorded");
    assert!(fault.ok, "kill failed: {}", fault.detail);
    assert!(fault.node.is_some());
    // The cluster keeps serving after a follower dies (a majority remains).
    assert!(
        phase("recovery").completed > 0,
        "no op completed after the kill"
    );
    assert!(Report::from_json(&std::fs::read_to_string(&out2).unwrap()).is_ok());
}

/// A throwaway CA plus one leaf (SAN `127.0.0.1`, which is what the client
/// verifies for an IP endpoint), written as PEM files under `dir`.
fn write_pki(dir: &std::path::Path) -> (PathBuf, PathBuf, PathBuf) {
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "animus-bench test CA");
    let ca_key = KeyPair::generate().expect("ca key");
    let ca = ca_params.self_signed(&ca_key).expect("self-sign");
    let mut leaf_params = CertificateParams::new(vec!["127.0.0.1".to_owned()]).expect("leaf");
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, "127.0.0.1");
    let leaf_key = KeyPair::generate().expect("leaf key");
    let leaf = leaf_params
        .signed_by(&leaf_key, &ca, &ca_key)
        .expect("sign leaf");
    let (ca_p, cert_p, key_p) = (
        dir.join("ca.pem"),
        dir.join("node.cert.pem"),
        dir.join("node.key.pem"),
    );
    std::fs::write(&ca_p, ca.pem()).expect("ca.pem");
    std::fs::write(&cert_p, leaf.pem()).expect("cert");
    std::fs::write(&key_p, leaf_key.serialize_pem()).expect("key");
    (ca_p, cert_p, key_p)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workloads_run_over_server_only_tls_and_a_plain_or_untrusting_client_is_refused() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (ca, cert, key) = write_pki(tmp.path());
    let out = tmp.path().join("tls.json");
    let tls_args = [
        "--tls-ca",
        ca.to_str().expect("utf8"),
        "--tls-cert",
        cert.to_str().expect("utf8"),
        "--tls-key",
        key.to_str().expect("utf8"),
    ];
    // A (read/update), E (scan + insert) and F (read-modify-write) cover
    // GetItem, UpdateItem, Query, PutItem and the conditional write path.
    let mut extra = vec![
        "--workloads",
        "A,E,F",
        "--consistent-read",
        "true",
        "--steady-secs",
        "2",
        "--degraded",
        "none",
    ];
    extra.extend(tls_args);
    let argv = args(&extra, &tmp.path().join("data"), &out);
    let opts = parse_args(&argv).expect("parse");
    let report = execute(&opts, argv).await.expect("TLS run");

    assert!(report.environment.tls, "report must record tls: true");
    assert!(report.environment.tls_note.starts_with("on:"));
    assert!(report.environment.sigv4);
    assert!(
        report.topology_start["tls"]
            .as_str()
            .expect("tls")
            .contains("server-only TLS")
    );
    assert!(
        report.topology_start["nodes"]
            .as_array()
            .expect("nodes")
            .iter()
            .all(|n| n["reachable"] == true && n["auth_enabled"] == true),
        "the admin port must be reachable over TLS: {}",
        report.topology_start["nodes"]
    );
    assert_eq!(report.runs.len(), 3);
    for run in &report.runs {
        let steady = run
            .phases
            .iter()
            .find(|p| p.name == "steady")
            .unwrap_or_else(|| panic!("{}: no steady phase", run.name));
        assert_eq!(steady.dispatched, 300, "{}", run.name);
        assert_eq!(
            steady.errors.total(),
            0,
            "{}: unexpected errors {:?} {:?}",
            run.name,
            steady.errors,
            steady.error_samples
        );
        assert_eq!(steady.completed, steady.dispatched, "{}", run.name);
    }
    let on_disk = Report::from_json(&std::fs::read_to_string(&out).expect("read")).expect("parse");
    assert!(on_disk.environment.tls);

    // The negative controls need a live TLS cluster; launch one directly.
    let creds = Credentials::new("animus-bench", "animus-bench-secret");
    let client = TlsClient::from_ca_file(&ca, None).expect("client");
    let cluster = Cluster::launch_in_process(
        3,
        &tmp.path().join("neg"),
        Some(creds.clone()),
        Some(LaunchTls {
            cert_path: cert,
            key_path: key,
            ca_path: ca.clone(),
            client: client.clone(),
        }),
    )
    .await
    .expect("launch");
    cluster
        .await_ready(std::time::Duration::from_secs(60))
        .await
        .expect("ready");
    let addr = cluster.dynamo_endpoints()[0];
    // Trusted client: a signed call gets a response (any status proves the
    // wire works end to end; ListTables is a valid, authorised call).
    let mut ok = Conn::connect(addr, Some(Arc::new(creds.clone())), Some(&client))
        .await
        .expect("TLS connect");
    let r = ok.call("ListTables", "{}").await.expect("call over TLS");
    assert_eq!(r.status, 200, "{}", r.text());
    // A client trusting a different CA must fail the handshake.
    let other = tempfile::tempdir().expect("tempdir");
    let (other_ca, _, _) = write_pki(other.path());
    let stranger = TlsClient::from_ca_file(&other_ca, None).expect("client");
    assert!(
        Conn::connect(addr, Some(Arc::new(creds.clone())), Some(&stranger))
            .await
            .is_err(),
        "an untrusted server certificate must be rejected"
    );
    // A plain-TCP client is refused by the TLS listener (the exchange fails
    // rather than being served).
    let plain = Conn::connect(addr, Some(Arc::new(creds)), None).await;
    let plain_ok = match plain {
        Ok(mut c) => c
            .call("ListTables", "{}")
            .await
            .is_ok_and(|r| r.status == 200),
        Err(_) => false,
    };
    assert!(!plain_ok, "a plain client must not be served by a TLS port");
    cluster.shutdown().await;
}
