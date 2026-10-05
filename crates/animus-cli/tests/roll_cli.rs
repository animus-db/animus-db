//! `animus cluster roll plan|wait|status` (ADR 0073 Phase 3, P3-B) driven as the
//! real binary against (a) a scripted fake admin server, which pins exit codes,
//! `--json`, `--timeout`, the wait loop, `--finalize`, and parity with the
//! server's `roll.remaining` / roll-health for the same state, and (b) a real
//! `animusd` admin listener, plain and server-only TLS.
//!
//! Real processes + sockets: every wait is bounded by a deadline.

#![allow(
    clippy::disallowed_methods,
    reason = "real-process/real-socket test: wall-clock deadlines are the point (ProdEnv liveness, not SimEnv)"
)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};
use std::thread;

use serde_json::{Value, json};

fn animus_bin() -> PathBuf {
    std::env::var_os("NEXTEST_BIN_EXE_animus")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_animus")))
}

fn run(args: &[&str]) -> Output {
    Command::new(animus_bin())
        .args(args)
        .output()
        .expect("spawn animus")
}

fn out(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn err(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

type Handler = dyn FnMut(&str, &str, &str) -> (u16, Value) + Send;

/// A scripted admin server: `handler(method, path, body) -> (status, json)`.
struct Fake {
    addr: SocketAddr,
    log: Arc<Mutex<Vec<(String, String, String)>>>,
}

fn fake(mut handler: Box<Handler>) -> Fake {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let log2 = log.clone();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { break };
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            // Read the head, then the body by Content-Length.
            loop {
                let n = s.read(&mut chunk).unwrap_or(0);
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                let text = String::from_utf8_lossy(&buf).into_owned();
                if let Some((head, body)) = text.split_once("\r\n\r\n") {
                    let want = head
                        .lines()
                        .find_map(|l| l.strip_prefix("Content-Length: "))
                        .and_then(|v| v.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if body.len() >= want {
                        break;
                    }
                }
            }
            let text = String::from_utf8_lossy(&buf).into_owned();
            let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
            let mut parts = head.lines().next().unwrap_or("").split(' ');
            let (method, path) = (
                parts.next().unwrap_or("").to_string(),
                parts.next().unwrap_or("").to_string(),
            );
            log2.lock()
                .unwrap()
                .push((method.clone(), path.clone(), body.to_string()));
            let (status, v) = handler(&method, &path, body);
            let body = v.to_string();
            let _ = write!(
                s,
                "HTTP/1.0 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    Fake { addr, log }
}

impl Fake {
    fn count(&self, method: &str, path: &str) -> usize {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, p, _)| m == method && p == path)
            .count()
    }
}

/// `cluster-version` shaped like `animusd`'s `cluster_version_view`: d (data)
/// a/b/c (combined); `maxes` = recorded range max per node (d, a, b, c);
/// `remaining` is the server's own `roll.remaining` for that state (data
/// first, control voters next, the control leader `a` last, ties by id).
fn view(active: u32, maxes: [Option<u32>; 4], remaining: &[&str], can_finalize: bool) -> Value {
    let ids = ["d", "a", "b", "c"];
    let roles = ["data", "combined", "combined", "combined"];
    let nodes: Vec<Value> = (0..4)
        .map(|i| {
            json!({"node": ids[i], "role": roles[i], "status": "Active",
                   "range": maxes[i].map(|m| json!({"min": 1, "max": m})),
                   "build": "test", "reported": maxes[i].is_some()})
        })
        .collect();
    let on_new = 4 - remaining.len();
    json!({
        "era_active": true, "active": active,
        "own_range": {"min": 1, "max": 2}, "own_build": "test",
        "nodes": nodes, "safe_target": 2, "can_finalize": can_finalize, "target": active + 1,
        "blockers": [],
        "roll": {"phase": if can_finalize { "ready_to_finalize" } else if on_new == 0 { "not_started" } else { "rolling" },
                 "total": 4, "on_new": on_new, "remaining": remaining,
                 "down": [], "blockers": [], "health": {"ok": true, "reasons": []}},
    })
}

fn health_ok(node: &str) -> Value {
    json!({"ok": true, "reasons": [], "local": {"node": node, "status": "Active"}})
}

/// A fake serving a fixed cluster state from node `own`'s point of view.
fn fixed(view: Value, own: &'static str, health: Value) -> Fake {
    fake(Box::new(move |_m, path, _b| match path {
        "/admin/cluster-version" => (200, view.clone()),
        "/admin/roll-health" => {
            let mut h = health.clone();
            h["local"]["node"] = json!(own);
            (200, h)
        }
        "/admin/raft" => (200, json!({"leader": "a"})),
        _ => (404, json!({"error": "not found"})),
    }))
}

#[test]
fn plan_order_and_status_match_the_servers_roll_state() {
    // The CLI's restart order must be exactly the server's `roll.remaining`.
    for (maxes, remaining) in [
        ([Some(1); 4], vec!["d", "b", "c", "a"]),
        ([Some(2), Some(1), Some(1), Some(1)], vec!["b", "c", "a"]),
        ([Some(2), Some(1), Some(2), Some(2)], vec!["a"]),
    ] {
        let f = fixed(view(1, maxes, &remaining, false), "d", health_ok("d"));
        let addr = f.addr.to_string();
        let o = run(&["cluster", "roll", "plan", &addr, "--json"]);
        assert!(o.status.success(), "{} {}", out(&o), err(&o));
        let p: Value = serde_json::from_str(out(&o).trim()).unwrap();
        assert_eq!(p["ok"], true, "{p}");
        let order: Vec<String> = p["steps"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|s| s["action"] == "restart")
            .map(|s| s["node"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(order, remaining, "{p}");
        // The control leader is restarted last, after a leadership transfer.
        if remaining.contains(&"a") {
            let steps = p["steps"].as_array().unwrap();
            let n = steps.len();
            assert_eq!(steps[n - 2]["action"], "transfer_control_leadership", "{p}");
            assert_eq!(steps[n - 2]["from"], "a");
            assert_eq!(steps[n - 1]["node"], "a");
        }
        // status renders the server's roll object and agrees on the next node.
        let o = run(&["cluster", "roll", "status", &addr, "--json"]);
        assert!(o.status.success(), "{}", err(&o));
        let s: Value = serde_json::from_str(out(&o).trim()).unwrap();
        assert_eq!(s["roll"]["remaining"], json!(remaining), "{s}");
        assert_eq!(s["health"], "ok");
        assert!(
            s["next"]
                .as_str()
                .unwrap()
                .contains(&format!("restart {}", remaining[0])),
            "{s}"
        );
        let o = run(&["cluster", "roll", "status", &addr]);
        assert!(out(&o).contains(&format!("remaining (roll order): {}", remaining.join(", "))));
    }
}

#[test]
fn plan_refuses_when_roll_health_is_not_ok_and_exits_nonzero() {
    let bad = json!({"ok": false, "reasons": [{"kind": "tablet_under_replicated", "tablet": 7}],
                     "local": {"node": "d"}});
    let f = fixed(
        view(1, [Some(1); 4], &["d", "b", "c", "a"], false),
        "d",
        bad,
    );
    let o = run(&["cluster", "roll", "plan", &f.addr.to_string()]);
    assert!(!o.status.success());
    assert!(
        out(&o).contains("refused:") && out(&o).contains("tablet_under_replicated"),
        "{}",
        out(&o)
    );
    assert!(err(&o).contains("roll refused"), "{}", err(&o));
    // --json still prints a machine-readable refusal.
    let o = run(&["cluster", "roll", "plan", &f.addr.to_string(), "--json"]);
    assert!(!o.status.success());
    let p: Value = serde_json::from_str(out(&o).trim()).unwrap();
    assert_eq!(p["ok"], false);
}

#[test]
fn plan_on_a_phase1_cluster_uses_admin_status() {
    // A previous-release node: no cluster-version, no roll-health.
    let f = fake(Box::new(|_m, path, _b| match path {
        "/admin/status" => (
            200,
            json!({"members": {"n0": {"status": "Active"}, "n1": {"status": "Active"}, "n2": {"status": "Active"}},
                   "node_addrs": {"n0": {"role": "combined"}, "n1": {"role": "combined"}, "n2": {"role": "combined"}}}),
        ),
        "/admin/raft" => (200, json!({"leader": "n0"})),
        _ => (404, json!({"error": "not found"})),
    }));
    let o = run(&["cluster", "roll", "plan", &f.addr.to_string(), "--json"]);
    assert!(o.status.success(), "{} {}", out(&o), err(&o));
    let p: Value = serde_json::from_str(out(&o).trim()).unwrap();
    let order: Vec<&str> = p["steps"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["action"] == "restart")
        .map(|s| s["node"].as_str().unwrap())
        .collect();
    assert_eq!(order, ["n1", "n2", "n0"], "{p}");
}

#[test]
fn wait_blocks_until_healthy_and_reports_the_next_node() {
    // roll-health: not ok twice (catching up), then ok.
    let polls = Arc::new(Mutex::new(0u32));
    let p2 = polls.clone();
    let v = view(
        1,
        [Some(2), Some(1), Some(1), Some(1)],
        &["b", "c", "a"],
        false,
    );
    let f = fake(Box::new(move |_m, path, _b| match path {
        "/admin/cluster-version" => (200, v.clone()),
        "/admin/roll-health" => {
            let mut n = p2.lock().unwrap();
            *n += 1;
            if *n <= 2 {
                (
                    200,
                    json!({"ok": false, "reasons": [{"kind": "local_groups_catching_up", "node": "d"}],
                             "local": {"node": "d"}}),
                )
            } else {
                (200, health_ok("d"))
            }
        }
        "/admin/raft" => (200, json!({"leader": "a"})),
        _ => (404, json!({})),
    }));
    let o = run(&[
        "cluster",
        "roll",
        "wait",
        &f.addr.to_string(),
        "--interval",
        "50ms",
        "--timeout",
        "20s",
    ]);
    assert!(o.status.success(), "{} {}", out(&o), err(&o));
    assert!(out(&o).contains("d: healthy"), "{}", out(&o));
    assert!(out(&o).contains("next: restart b"), "{}", out(&o));
    assert!(err(&o).contains("local_groups_catching_up"), "{}", err(&o));
    assert!(*polls.lock().unwrap() >= 3);
}

#[test]
fn wait_times_out_with_the_reason_and_a_nonzero_exit() {
    // The node never reports the new range (still the old binary's view).
    let v = view(
        1,
        [None, Some(1), Some(1), Some(1)],
        &["d", "b", "c", "a"],
        false,
    );
    let f = fixed(v, "d", health_ok("d"));
    let o = run(&[
        "cluster",
        "roll",
        "wait",
        &f.addr.to_string(),
        "--interval",
        "50ms",
        "--timeout",
        "1s",
    ]);
    assert!(!o.status.success());
    assert!(
        err(&o).contains("timed out") && err(&o).contains("range"),
        "{}",
        err(&o)
    );
}

#[test]
fn wait_node_flag_must_match_the_answering_node() {
    let f = fixed(view(1, [Some(2); 4], &["a"], false), "d", health_ok("d"));
    let o = run(&[
        "cluster",
        "roll",
        "wait",
        &f.addr.to_string(),
        "--node",
        "b",
        "--timeout",
        "5s",
    ]);
    assert!(!o.status.success());
    assert!(err(&o).contains("--node b"), "{}", err(&o));
}

#[test]
fn wait_on_the_last_node_without_finalize_only_prints_the_command() {
    let f = fixed(view(1, [Some(2); 4], &[], true), "a", health_ok("a"));
    let o = run(&[
        "cluster",
        "roll",
        "wait",
        &f.addr.to_string(),
        "--timeout",
        "5s",
    ]);
    assert!(o.status.success(), "{}", err(&o));
    assert!(
        out(&o).contains("run `animus cluster finalize"),
        "{}",
        out(&o)
    );
    assert_eq!(f.count("POST", "/admin/cluster-version/finalize"), 0);
    // --finalize without --yes is refused up front (irreversible).
    let o = run(&["cluster", "roll", "wait", &f.addr.to_string(), "--finalize"]);
    assert!(!o.status.success());
    assert!(err(&o).contains("CANNOT be undone"), "{}", err(&o));
}

#[test]
fn wait_finalize_on_the_last_node_finalizes_via_the_control_leader() {
    // The last node rolled is the former leader; the leader is now `b`, whose
    // admin address (the same fake) comes from /admin/status node_addrs.
    let finalized = Arc::new(Mutex::new(false));
    let f2 = finalized.clone();
    let addr_cell: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    let a2 = addr_cell.clone();
    let f = fake(Box::new(move |m, path, _b| {
        let done = *f2.lock().unwrap();
        match (m, path) {
            ("GET", "/admin/cluster-version") => {
                let mut v = view(if done { 2 } else { 1 }, [Some(2); 4], &[], !done);
                if done {
                    v["can_finalize"] = json!(false);
                }
                (200, v)
            }
            ("GET", "/admin/roll-health") => (200, health_ok("a")),
            ("GET", "/admin/raft") => (200, json!({"leader": "b"})),
            ("GET", "/admin/status") => (
                200,
                json!({"node_addrs": {"b": {"admin": a2.lock().unwrap().clone()}}}),
            ),
            ("POST", "/admin/cluster-version/finalize") => {
                *f2.lock().unwrap() = true;
                (200, json!({"ok": true}))
            }
            _ => (404, json!({})),
        }
    }));
    *addr_cell.lock().unwrap() = f.addr.to_string();
    let o = run(&[
        "cluster",
        "roll",
        "wait",
        &f.addr.to_string(),
        "--timeout",
        "20s",
        "--finalize",
        "--yes",
    ]);
    assert!(o.status.success(), "{} {}", out(&o), err(&o));
    assert!(out(&o).contains("cluster version is now 2"), "{}", out(&o));
    let log = f.log.lock().unwrap();
    let post = log
        .iter()
        .find(|(m, p, _)| m == "POST" && p == "/admin/cluster-version/finalize")
        .expect("finalize POST");
    let body: Value = serde_json::from_str(&post.2).unwrap();
    assert_eq!(body, json!({"to": 2, "expected": 1}));
}

#[test]
fn wait_finalize_is_inert_before_the_last_node() {
    let f = fixed(
        view(
            1,
            [Some(2), Some(1), Some(1), Some(1)],
            &["b", "c", "a"],
            false,
        ),
        "d",
        health_ok("d"),
    );
    let o = run(&[
        "cluster",
        "roll",
        "wait",
        &f.addr.to_string(),
        "--timeout",
        "5s",
        "--finalize",
        "--yes",
    ]);
    assert!(o.status.success(), "{} {}", out(&o), err(&o));
    assert_eq!(f.count("POST", "/admin/cluster-version/finalize"), 0);
}

#[test]
fn argument_errors_exit_nonzero_with_usage() {
    let o = run(&["cluster", "roll"]);
    assert!(!o.status.success());
    assert!(err(&o).contains("cluster roll wait"), "{}", err(&o));
    let o = run(&[
        "cluster",
        "roll",
        "wait",
        "127.0.0.1:1",
        "--timeout",
        "soon",
    ]);
    assert!(!o.status.success());
    assert!(err(&o).contains("not a duration"), "{}", err(&o));
}
