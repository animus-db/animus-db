//! Real-binary regression for the `--ephemeral`-without-`--dir` default
//! data directory (this fix's ticket; see `crates/animusd/CLAUDE.md`'s
//! "`--cluster N`/`--cluster-control N --cluster-data M` without `--dir`"
//! entry).
//!
//! Before the fix, `--cluster N` (and `--cluster-control`/`--cluster-data`)
//! defaulted `--dir` to ONE fixed path (`$TMPDIR/animusd`) regardless of
//! `--ephemeral` — `--ephemeral` only swaps the CP-data `StorageBackend`,
//! never the control-plane `ProdEnv`'s on-disk WAL. Two back-to-back runs
//! with no `--dir` therefore rehydrated the first run's control-plane WAL.
//!
//! This crate's own investigation while writing this test confirmed the
//! downstream "stuck/never-electing" symptom is real (a `--cluster 3` run
//! followed by a reused-directory `--cluster 5` run reliably left two of the
//! five nodes permanently `control_leader_known: false` / `ok: false` in
//! most manual trials) but is **timing-sensitive**: in a handful of trials
//! the same mismatched-size sequence happened to re-elect within a second
//! instead. Per this repo's own standing rule that a flaky test is a bug
//! and never something to ship around (`CLAUDE.md`'s "Session operating
//! mode" item 4), this file does not gate on that racy election outcome.
//! Instead it asserts the actual, fully deterministic root cause directly:
//! **the second back-to-back run must land in its own brand-new directory,
//! never the first run's** — which is both necessary and sufficient to
//! prevent the stale-WAL rehydration the election symptom above was
//! downstream of.
//!
//! This spawns the actual compiled `animusd` binary (not the in-process
//! `run_in_process_cluster`/`bind_cluster` helpers the other tests in this
//! crate use — the bug is specifically in `main.rs`'s CLI-argument-to-`--dir`
//! resolution, which those helpers never go through), parsing each run's own
//! startup banner (this fix's new `animusd: data dir …` line, and every
//! node's own `admin http://…` address) from stdout.
//!
//! Real time and real sockets (a real subprocess), so generous, polled
//! timeouts throughout — never a fixed-deadline one-shot assert — and every
//! test is wrapped in an outer `tokio::time::timeout` so a hang reports as a
//! failed test rather than hanging the suite.

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const STARTUP_DEADLINE: Duration = Duration::from_secs(30);
const HEALTH_DEADLINE: Duration = Duration::from_secs(30);
const TEST_DEADLINE: Duration = Duration::from_secs(120);

struct Run {
    child: Child,
    // Kept only to keep the draining thread's sender end reachable for as
    // long as `Run` is alive; dropping it lets that thread notice the
    // receiver is gone and exit instead of leaking for the process's life.
    _stdout_rx: mpsc::Receiver<String>,
    // `None` on a binary that predates this fix's `animusd: data dir …`
    // line (deliberately NOT required to reach `admin_addrs` below — a
    // test that waited on this line too would trivially "fail" against the
    // old binary for the wrong reason, without ever exercising whether the
    // second run's cluster actually elects).
    data_dir: Option<String>,
    // In node-index order (`animusd` prints "  node {i}: … admin http://…"
    // one line per node, in order).
    admin_addrs: Vec<SocketAddr>,
}

impl Drop for Run {
    fn drop(&mut self) {
        // Best-effort: this test never exercises graceful shutdown, only
        // the startup path, so a hard kill (reaped immediately after) is
        // enough — and correct even if the process already exited.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Spawn `animusd <args>` (an `n`-node `--cluster n` invocation) with its
/// stdout piped through a dedicated draining thread (so the child never
/// blocks on a full pipe once we stop reading every line ourselves — it
/// keeps printing after startup), and block until its banner has printed
/// all `n` nodes' `admin http://…` addresses, or panic after
/// [`STARTUP_DEADLINE`]. Also opportunistically captures this fix's
/// `animusd: data dir …` line when present, but never blocks on it — see
/// [`Run::data_dir`]'s doc for why.
fn spawn_and_capture_banner(args: &[&str], n: usize) -> Run {
    let exe = env!("CARGO_BIN_EXE_animusd");
    let mut child = Command::new(exe)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn `animusd {}`: {e}", args.join(" ")));
    let stdout = child.stdout.take().expect("stdout was piped");
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            match line {
                Ok(line) => {
                    if tx.send(line).is_err() {
                        break; // receiver dropped — stop draining
                    }
                }
                Err(_) => break,
            }
        }
    });

    // Wrap in `Run` (whose `Drop` kills the child) *immediately* — before
    // any of the fallible/panicking work below — so a startup timeout or a
    // missing-banner-line panic still reaps this process instead of
    // orphaning it. A bare `std::process::Child` does NOT kill on drop
    // (only `Run`'s explicit `Drop` impl does), so leaving it unwrapped
    // across a panicking path here was observed, mid-development of this
    // very test, to leak a live `animusd` process that then silently
    // shared a later run's reused `--dir` — exactly the kind of cross-run
    // contamination this test exists to catch, so it must not introduce
    // its own copy of it.
    let mut run = Run {
        child,
        _stdout_rx: rx,
        data_dir: None,
        admin_addrs: Vec::with_capacity(n),
    };

    let deadline = Instant::now() + STARTUP_DEADLINE;
    while run.admin_addrs.len() < n && Instant::now() < deadline {
        match run._stdout_rx.recv_timeout(Duration::from_millis(200)) {
            Ok(line) => {
                if let Some(rest) = line.trim().strip_prefix("animusd: data dir ") {
                    run.data_dir = Some(rest.to_string());
                }
                if let Some(addr) = parse_admin_addr(&line) {
                    run.admin_addrs.push(addr);
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
        if let Ok(Some(status)) = run.child.try_wait() {
            panic!(
                "`animusd {}` exited early during startup (status: {status}); \
                 data_dir so far: {:?}, admin_addrs so far: {:?}",
                args.join(" "),
                run.data_dir,
                run.admin_addrs
            );
        }
    }
    assert_eq!(
        run.admin_addrs.len(),
        n,
        "`animusd {}` never printed all {n} nodes' admin addresses within \
         {STARTUP_DEADLINE:?} (got {:?})",
        args.join(" "),
        run.admin_addrs
    );
    run
}

/// Parses `"  node 0: client 1.2.3.4:1 — dynamo http 1.2.3.4:2 — admin
/// http://1.2.3.4:3 — console http://1.2.3.4:4"`'s admin address.
fn parse_admin_addr(line: &str) -> Option<SocketAddr> {
    let marker = "admin http://";
    let start = line.find(marker)? + marker.len();
    let rest = &line[start..];
    let end = rest.find(" —").unwrap_or(rest.trim_end().len());
    rest[..end].trim().parse().ok()
}

/// One raw HTTP/1.0 GET against the admin port, mirroring
/// `admin_endpoint.rs`'s own `admin` helper (self-contained here since this
/// test spawns a real subprocess rather than sharing that file's in-process
/// bring-up). Returns `None` on a connection failure (the port may not be
/// accepting yet, or may already be gone) so the caller can retry.
async fn try_admin_get(addr: SocketAddr, path: &str) -> Option<(u16, Value)> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    let request = format!("GET {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;
    stream.flush().await.ok()?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.ok()?;
    let text = String::from_utf8(raw).ok()?;
    let (head, payload) = text.split_once("\r\n\r\n")?;
    let status: u16 = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    let value: Value = serde_json::from_str(payload).ok()?;
    Some((status, value))
}

/// Polls every one of `addrs`' `/admin/health` until **all** report a
/// healthy, elected `200 {"ok": true}` at the same time, or returns an
/// error describing the last observed state of every still-unhealthy node
/// once [`HEALTH_DEADLINE`] passes — never a fixed-deadline one-shot assert.
async fn wait_for_all_healthy(addrs: &[SocketAddr]) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + HEALTH_DEADLINE;
    let mut last_seen = vec!["never got a response".to_string(); addrs.len()];
    loop {
        let mut all_ok = true;
        for (i, addr) in addrs.iter().enumerate() {
            match try_admin_get(*addr, "/admin/health").await {
                Some((200, body)) if body["ok"] == true => {}
                Some((status, body)) => {
                    all_ok = false;
                    last_seen[i] = format!("{status} {body}");
                }
                None => {
                    all_ok = false;
                    last_seen[i] = "connection failed".to_string();
                }
            }
        }
        if all_ok {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            let report: Vec<String> = addrs
                .iter()
                .zip(&last_seen)
                .map(|(addr, seen)| format!("{addr}: {seen}"))
                .collect();
            return Err(format!(
                "not every node reached `200 {{\"ok\": true}}` within \
                 {HEALTH_DEADLINE:?} — last seen per node:\n{}",
                report.join("\n")
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Names of the entries directly under `std::env::temp_dir()` whose name
/// starts with `animusd` — this crate's own data-dir naming, pre- and
/// post-fix alike (`animusd`, `animusd-cluster-<pid>`,
/// `animusd-ephemeral-<pid>`, `animusd-node-<i>`, …). Used as a
/// process-agnostic way to tell whether a run created a brand new top-level
/// directory there.
fn animusd_temp_entries() -> BTreeSet<String> {
    std::fs::read_dir(std::env::temp_dir())
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.starts_with("animusd"))
        .collect()
}

/// The deterministic regression: two back-to-back `animusd --cluster N
/// --ephemeral` runs, neither passing `--dir`, must each land in their own
/// brand-new top-level temp directory — the second run must never silently
/// reuse whatever the first run already created there.
///
/// This does not depend on election timing at all (see this file's module
/// doc for why that would be a flaky assertion) — only on which
/// directories exist on disk, which is exactly what `main.rs`'s `--dir`
/// resolution controls.
#[tokio::test(flavor = "multi_thread")]
async fn back_to_back_ephemeral_cluster_runs_each_get_a_new_temp_dir() {
    tokio::time::timeout(TEST_DEADLINE, async {
        let before_first = animusd_temp_entries();

        let first = spawn_and_capture_banner(&["--cluster", "1", "--ephemeral"], 1);
        wait_for_all_healthy(&first.admin_addrs)
            .await
            .expect("first run must become healthy");
        let after_first = animusd_temp_entries();
        drop(first); // kills + reaps it before the second spawns

        let second = spawn_and_capture_banner(&["--cluster", "1", "--ephemeral"], 1);
        wait_for_all_healthy(&second.admin_addrs)
            .await
            .expect("second run must become healthy");
        let after_second = animusd_temp_entries();
        drop(second);

        let second_run_new_entries: Vec<&String> = after_second.difference(&after_first).collect();
        assert!(
            !second_run_new_entries.is_empty(),
            "the second back-to-back `--cluster 1 --ephemeral` run (no \
             `--dir`) must create its own brand-new directory under {:?} \
             rather than silently reusing whatever the first run already \
             created there — before either run: {before_first:?}, after \
             the first run: {after_first:?}, after the second run: \
             {after_second:?}",
            std::env::temp_dir()
        );
    })
    .await
    .expect("test exceeded its overall deadline");
}

/// Companion assertion using this fix's own `animusd: data dir …` startup
/// line directly (rather than inferring it from directory listings): two
/// back-to-back runs' chosen default directories must differ. Fails on a
/// pre-fix binary simply because that line does not exist yet — a
/// deterministic, if blunt, signal that this behavior is new.
#[tokio::test(flavor = "multi_thread")]
async fn back_to_back_ephemeral_cluster_runs_get_distinct_default_dirs() {
    tokio::time::timeout(TEST_DEADLINE, async {
        let first = spawn_and_capture_banner(&["--cluster", "1", "--ephemeral"], 1);
        let first_dir = first
            .data_dir
            .clone()
            .expect("this binary must print `animusd: data dir …` at startup");
        drop(first);

        let second = spawn_and_capture_banner(&["--cluster", "1", "--ephemeral"], 1);
        let second_dir = second
            .data_dir
            .clone()
            .expect("this binary must print `animusd: data dir …` at startup");
        assert_ne!(
            first_dir, second_dir,
            "two back-to-back `--cluster N --ephemeral` runs with no `--dir` \
             must not default to the same data directory"
        );
    })
    .await
    .expect("test exceeded its overall deadline");
}
