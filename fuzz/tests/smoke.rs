//! Deterministic stable-toolchain smoke for every fuzz target (roadmap R-01
//! (c)): each target is driven over every seed it has plus N seeded mutations
//! of them, with no libFuzzer, nightly toolchain or sanitizer involved.
//!
//! Property checked: the target returns (no panic). A panic is caught, the
//! offending input is printed as hex together with the panic message and
//! location, and the test fails at the end listing *all* of them — unless the
//! panic matches a row of `fuzz/known-issues.tsv` (a documented, filed defect
//! whose fix is in flight; see that file), in which case it is reported but
//! not failed on.
//!
//! Knobs: `ANIMUS_FUZZ_SMOKE_ITERS` (mutations per target, default 3000),
//! `ANIMUS_FUZZ_SMOKE_SEED` (PRNG seed, default fixed). Replay one input with
//! `ANIMUS_FUZZ_REPLAY=<target>:<hex>`.

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Mutex;

use animus_fuzz::mutate::{Rng, mutate};
use animus_fuzz::seeds::{self, Seed};
use animus_fuzz::targets;

static LAST_PANIC: Mutex<Option<String>> = Mutex::new(None);

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).expect("hex"))
        .collect()
}

/// `Some(panic message + location)` if `f(input)` panicked.
fn run_one(f: fn(&[u8]), input: &[u8]) -> Option<String> {
    *LAST_PANIC.lock().unwrap() = None;
    match catch_unwind(AssertUnwindSafe(|| f(input))) {
        Ok(()) => None,
        Err(_) => Some(
            LAST_PANIC
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| "panic (no message captured)".into()),
        ),
    }
}

/// `target -> [(panic-message substring, note)]`.
fn known_issues() -> BTreeMap<String, Vec<(String, String)>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("known-issues.tsv");
    let mut out: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    let Ok(tsv) = std::fs::read_to_string(path) else {
        return out;
    };
    for line in tsv
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
    {
        let c: Vec<&str> = line.split('\t').collect();
        assert!(
            c.len() >= 2,
            "known-issues.tsv row: target<TAB>substring<TAB>note: {line:?}"
        );
        out.entry(c[0].to_owned())
            .or_default()
            .push((c[1].to_owned(), c.get(2).copied().unwrap_or("").to_owned()));
    }
    out
}

#[test]
fn every_target_survives_seeds_and_seeded_mutations() {
    std::panic::set_hook(Box::new(|info| {
        let loc = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_default();
        let msg = if let Some(s) = info.payload().downcast_ref::<&str>() {
            (*s).to_owned()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "non-string panic payload".into()
        };
        *LAST_PANIC.lock().unwrap() = Some(format!("{msg} @ {loc}"));
    }));

    if let Ok(replay) = std::env::var("ANIMUS_FUZZ_REPLAY") {
        let (target, h) = replay
            .split_once(':')
            .expect("ANIMUS_FUZZ_REPLAY=<target>:<hex>");
        let (_, f) = targets::ALL
            .iter()
            .find(|(n, _)| *n == target)
            .unwrap_or_else(|| panic!("unknown target {target}"));
        let _ = std::panic::take_hook();
        f(&unhex(h));
        return;
    }

    let iters: usize = std::env::var("ANIMUS_FUZZ_SMOKE_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);
    let seed: u64 = std::env::var("ANIMUS_FUZZ_SMOKE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x00A1_1105_DB00_0001);
    let known = known_issues();
    let all_seeds = seeds::all();

    let mut failures = Vec::new();
    let mut known_hits: BTreeMap<String, usize> = BTreeMap::new();
    for (idx, (name, f)) in targets::ALL.iter().enumerate() {
        let mine: Vec<&Seed> = all_seeds.iter().filter(|s| s.target == *name).collect();
        assert!(
            !mine.is_empty(),
            "target {name} has no seeds — add some (fuzz/seeds.tsv or fuzz/seeds/{name}/)"
        );
        let mut rng = Rng::new(seed ^ (idx as u64).wrapping_mul(0x9e37_79b9));

        let mut inputs: Vec<(String, Vec<u8>)> = mine
            .iter()
            .map(|s| (s.label.clone(), s.bytes.clone()))
            .collect();
        inputs.push(("empty".into(), Vec::new()));
        inputs.push(("0xff*64".into(), vec![0xff; 64]));
        for i in 0..iters {
            let base = &mine[i % mine.len()].bytes;
            let other = &mine[rng.below(mine.len())].bytes;
            inputs.push((
                format!("mutation #{i}"),
                mutate(&mut rng, base, other, 1 << 16),
            ));
        }

        for (label, input) in &inputs {
            if let Some(msg) = run_one(*f, input) {
                if let Some((_, note)) = known
                    .get(*name)
                    .and_then(|v| v.iter().find(|(sub, _)| msg.contains(sub.as_str())))
                {
                    *known_hits.entry(format!("{name}: {note}")).or_default() += 1;
                } else {
                    failures.push(format!(
                        "target `{name}` panicked on {label} ({} bytes)\n  panic: {msg}\n  replay: ANIMUS_FUZZ_REPLAY={name}:{}",
                        input.len(),
                        hex(input)
                    ));
                }
            }
        }
    }
    let _ = std::panic::take_hook();
    for (k, n) in &known_hits {
        eprintln!("known issue still reproduces ({n}x): {k}");
    }
    assert!(
        failures.is_empty(),
        "{} fuzz smoke failure(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}
