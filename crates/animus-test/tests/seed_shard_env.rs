//! `ANIMUS_SEED_SHARD` read from the real process environment (issue #1271).
//! Its own test binary: the knob is process-global state. The workspace
//! forbids `unsafe` (so no `set_var`); the parent test re-executes this binary
//! with the variable set on the child's environment instead.

use animus_test::corpus::{SHARD_VAR, owns_unsharded, shard_range, shard_retain};
use std::process::Command;

const PROBE: &str = "ANIMUS_SHARD_PROBE";

/// Child side: prints what the helpers return under the inherited env.
#[test]
fn probe() {
    if std::env::var(PROBE).is_err() {
        return;
    }
    println!(
        "RESULT own={} range={:?} retain={:?}",
        owns_unsharded(),
        shard_range(5).collect::<Vec<_>>(),
        shard_retain(vec![7, 8, 9])
    );
}

fn run_probe(shard: Option<&str>) -> (bool, String) {
    let mut cmd = Command::new(std::env::current_exe().expect("exe"));
    cmd.args(["--exact", "probe", "--nocapture", "--test-threads=1"])
        .env(PROBE, "1")
        .env_remove(SHARD_VAR);
    if let Some(s) = shard {
        cmd.env(SHARD_VAR, s);
    }
    let out = cmd.output().expect("spawn probe");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr),
    )
}

#[test]
fn the_env_knob_is_parsed_loudly_and_applied() {
    let (ok, out) = run_probe(None);
    assert!(ok, "{out}");
    assert!(
        out.contains("RESULT own=true range=[0, 1, 2, 3, 4] retain=[7, 8, 9]"),
        "{out}"
    );

    let (ok, out) = run_probe(Some("1/2"));
    assert!(ok, "{out}");
    assert!(
        out.contains("RESULT own=false range=[1, 3] retain=[8]"),
        "{out}"
    );

    let (ok, out) = run_probe(Some("0/2"));
    assert!(ok, "{out}");
    assert!(
        out.contains("RESULT own=true range=[0, 2, 4] retain=[7, 9]"),
        "{out}"
    );

    for bad in ["2/2", "0/0", "x", "1", ""] {
        let (ok, out) = run_probe(Some(bad));
        assert!(!ok, "{bad:?} must fail loudly, not run unsharded: {out}");
    }
}
