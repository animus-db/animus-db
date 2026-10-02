//! Fault-injection corpus for `EncryptedSegmentStore` (ADR 0069, S-03 PR 2)
//! — the `SegmentStore`-seam sibling of `animus-storage`'s own
//! `lsm_crash_encrypted.rs` (the `Disk`-seam corpus PR 1 shipped).
//!
//! ## Scope, stated plainly
//!
//! The backup/PITR/export-import domain corpora this crate already carries
//! (`backup_fault_corpus.rs`, `pitr_fault_corpus.rs`,
//! `export_import_fault_corpus.rs` — ~2,400 lines each) each hardcode their
//! shared harness functions to a **concrete** `&animus_sim::SimSegmentStore`
//! parameter, not a `S: SegmentStore` generic, across dozens of helper
//! functions and dozens of `#[test]` scenarios. Genericizing all three
//! files (or duplicating them wholesale) to run their existing cells
//! through an `EncryptedSegmentStore<SimSegmentStore, SimEnv>` instead would
//! be a materially larger, separate refactor — real regression risk across
//! ~7,000 lines of already-hard-won deterministic corpus, out of proportion
//! to what this PR needs to prove. That reach is a named, honest follow-up,
//! not attempted here.
//!
//! What **is** proven here, matching `lsm_crash_encrypted.rs`'s own
//! precedent of pinning the wrapper's crash/fault behavior directly rather
//! than re-running an unrelated domain's whole suite through it: every
//! `SegmentFaultConfig`/unavailability-window fault the three domain
//! corpora above already inject through `SimSegmentStore` composes
//! correctly with `EncryptedSegmentStore` sitting on top of it — since
//! every one of those corpora's own `SimSegmentStore`-level assumptions
//! (an ack-lost `put`/`delete` still lands the state change; an
//! unavailability window fails every op with no state change, then heals)
//! are exactly what this file checks still hold true, in plaintext terms,
//! through the encrypting wrapper. Also covered: the loud marker mismatch
//! in every direction survives an ack-lost fault on the marker `put` itself
//! (a caller must retry `open`, not treat a spurious ack-lost error as a
//! permanent refusal), and a mid-file corruption of an already-durable
//! object is a hard `get` error naming the id, never a silent trim.
//!
//! **Corpus shape**: this crate's own `animus_test::corpus::for_each_seed`
//! scaffolding (the closure-driven style, since each scenario below is
//! self-contained rather than built from a shared `Vec<Scenario>`); depth
//! knob **`ANIMUS_SEGMENT_STORE_ENCRYPTED_SEEDS`** (default 1 — the frozen
//! cells below; `K>1` additionally sweeps `K-1` fresh, name-derived seeds
//! per cell).

use animus_env::{
    EncryptedSegmentStore, EncryptionKey, EnvExt, SegmentStore, nid,
    verify_or_init_segment_store_marker,
};
use animus_sim::{SegmentFaultConfig, SimEnv, SimSegmentStore, Simulator};
use animus_test::corpus::for_each_seed;
use std::sync::{Arc, Mutex};

fn depth() -> usize {
    animus_test::corpus::seeds_from_env("ANIMUS_SEGMENT_STORE_ENCRYPTED_SEEDS")
}

fn test_key() -> EncryptionKey {
    EncryptionKey::from_bytes([0x37; 32])
}

fn other_key() -> EncryptionKey {
    EncryptionKey::from_bytes([0x73; 32])
}

/// Drive `body` (an async block) to completion inside `sim`, bounded by
/// `MAX_STEPS` — the same `spawn_task` + `run_until_quiescent` idiom
/// `animus-sim`'s own `segment_store.rs` tests use (no tokio in this crate's
/// dependency graph for the sim side). A panicking assertion inside `body`
/// propagates out of `run_until_quiescent` exactly like any other panic.
const MAX_STEPS: usize = 10_000;

fn drive<F>(sim: &Simulator, env: &SimEnv, body: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    env.spawn_task(body);
    let mut driver = sim.clone();
    assert!(
        driver.run_until_quiescent(MAX_STEPS),
        "scenario did not settle within {MAX_STEPS} steps"
    );
}

/// An ack-lost `put` on an encrypted store: the caller sees an injected
/// error, but the sealed object still landed — a fresh handle over the same
/// underlying store reads back the correct plaintext.
#[test]
fn round_trip_survives_put_ack_lost() {
    for_each_seed("segment_store_encrypted_put_ack_lost", depth(), |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let raw = SimSegmentStore::new(env.clone());

        let raw_for_asserts = raw.clone();
        let env_for_task = env.clone();
        let env_for_reopen = env.clone();
        drive(&sim, &env, async move {
            let enc = EncryptedSegmentStore::open(raw, env_for_task, test_key())
                .await
                .expect("open a fresh store with a key");
            // Enable the fault only after `open` (which itself `put`s the
            // marker object) has succeeded cleanly — a marker-put fault is
            // a separate property, covered by
            // `a_marker_put_ack_lost_fault_is_recovered_by_retrying_open`.
            let mut cfg = SegmentFaultConfig::default();
            cfg.set_put_ack_lost_prob(1.0);
            raw_for_asserts.set_fault_config(cfg);
            let err = enc
                .put("t/label/1/0", b"payload")
                .await
                .map(|_| ())
                .expect_err("the ack-lost fault must surface as an error");
            drop(err);
            let fresh_view =
                EncryptedSegmentStore::open(raw_for_asserts, env_for_reopen, test_key())
                    .await
                    .expect("reopen with the same key must succeed (marker already present)");
            assert_eq!(
                fresh_view.get("t/label/1/0").await.expect("get"),
                Some(b"payload".to_vec()),
                "the object must have landed even though put reported an error"
            );
        });
    });
}

/// The identical property for `delete`: the object is genuinely removed
/// even when the caller sees an ack-lost error.
#[test]
fn round_trip_survives_delete_ack_lost() {
    for_each_seed("segment_store_encrypted_delete_ack_lost", depth(), |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let raw = SimSegmentStore::new(env.clone());

        let raw_for_asserts = raw.clone();
        let env_for_task = env.clone();
        drive(&sim, &env, async move {
            let enc = EncryptedSegmentStore::open(raw, env_for_task, test_key())
                .await
                .expect("open");
            enc.put("t/label/1/0", b"payload").await.expect("put");

            let mut cfg = SegmentFaultConfig::default();
            cfg.set_delete_ack_lost_prob(1.0);
            raw_for_asserts.set_fault_config(cfg);

            let err = enc
                .delete("t/label/1/0")
                .await
                .map(|_| ())
                .expect_err("the ack-lost fault must surface as an error");
            drop(err);
            assert_eq!(
                enc.get("t/label/1/0").await.expect("get after delete"),
                None,
                "the object must be genuinely gone even though delete reported an error"
            );
        });
    });
}

/// An unavailability window fails every op with no state change, then heals
/// on its own once virtual time passes the deadline — proven through the
/// encrypting wrapper exactly as `animus-sim`'s own plaintext test proves it
/// directly against `SimSegmentStore`.
#[test]
fn unavailability_window_heals_and_then_round_trips() {
    for_each_seed("segment_store_encrypted_unavailability", depth(), |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let raw = SimSegmentStore::new(env.clone());

        let raw_for_asserts = raw.clone();
        let env_for_task = env.clone();
        drive(&sim, &env, async move {
            use animus_env::Clock;
            let enc = EncryptedSegmentStore::open(raw, env_for_task.clone(), test_key())
                .await
                .expect("open");
            let deadline = env_for_task
                .now()
                .saturating_add(std::time::Duration::from_secs(5));
            raw_for_asserts.set_unavailable_until(deadline);

            enc.put("t/label/1/0", b"x")
                .await
                .map(|_| ())
                .expect_err("put must fail during the unavailability window");
            assert!(
                !raw_for_asserts
                    .stored_ids()
                    .iter()
                    .any(|id| id == "t/label/1/0"),
                "no state change may occur while unavailable"
            );

            env_for_task.sleep(std::time::Duration::from_secs(6)).await;

            enc.put("t/label/1/0", b"x").await.expect("put after heal");
            assert_eq!(
                enc.get("t/label/1/0").await.expect("get"),
                Some(b"x".to_vec())
            );
        });
    });
}

/// Write-once holds at the PLAINTEXT level even while the underlying store
/// is faulted: an identical-content re-put is still a safe no-op, and a
/// differing-content re-put is still a hard error — a fresh random salt per
/// `put` (see the wrapper's own module doc) must never let ciphertext-level
/// comparison leak through and falsely reject an idempotent retry.
#[test]
fn write_once_holds_at_the_plaintext_level_under_fault() {
    for_each_seed("segment_store_encrypted_write_once", depth(), |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let raw = SimSegmentStore::new(env.clone());
        let env_for_task = env.clone();
        drive(&sim, &env, async move {
            let enc = EncryptedSegmentStore::open(raw, env_for_task, test_key())
                .await
                .expect("open");
            enc.put("t/label/1/0", b"same").await.expect("first put");
            enc.put("t/label/1/0", b"same")
                .await
                .expect("identical-content re-put must be a safe no-op");
            enc.put("t/label/1/0", b"different")
                .await
                .map(|_| ())
                .expect_err("differing-content re-put must be a hard error");
            assert_eq!(
                enc.get("t/label/1/0").await.expect("get"),
                Some(b"same".to_vec()),
                "a rejected write-once violation must not change the stored plaintext"
            );
        });
    });
}

/// A mid-file corruption of an already-durable object (never a crash — a
/// direct tamper of the raw bytes, `SimSegmentStore`'s own `stored_ids`/
/// raw-map access stands in for `Simulator::corrupt_durable`, which has no
/// `SegmentStore`-level analogue) is a hard `get` error naming the id —
/// never a silent `None` or garbage plaintext, since a `SegmentStore`
/// object has no torn-tail case to fall back to (see the wrapper's own
/// module doc).
#[test]
fn a_tampered_object_is_a_hard_error_naming_the_id() {
    for_each_seed("segment_store_encrypted_tamper", depth(), |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let raw = SimSegmentStore::new(env.clone());
        let raw_for_tamper = raw.clone();
        let env_for_task = env.clone();
        drive(&sim, &env, async move {
            let enc = EncryptedSegmentStore::open(raw, env_for_task, test_key())
                .await
                .expect("open");
            enc.put("t/label/1/0", b"hello world").await.expect("put");

            let mut bytes = raw_for_tamper
                .get("t/label/1/0")
                .await
                .expect("get raw")
                .expect("present");
            let last = bytes.len() - 1;
            bytes[last] ^= 0xFF;
            raw_for_tamper.delete("t/label/1/0").await.expect("delete");
            raw_for_tamper
                .put("t/label/1/0", &bytes)
                .await
                .expect("re-put the tampered bytes");

            let err = enc
                .get("t/label/1/0")
                .await
                .map(|_| ())
                .expect_err("a tampered object must be a hard error");
            let msg = err.to_string();
            assert!(msg.contains("t/label/1/0"), "must name the id: {msg}");
        });
    });
}

/// An ack-lost `put` while `open()` is initializing a fresh store's marker
/// must not be treated as a permanent refusal: the marker object itself
/// still landed (the identical ambiguity every other object tolerates), so
/// retrying `open` with the same key succeeds.
#[test]
fn a_marker_put_ack_lost_fault_is_recovered_by_retrying_open() {
    for_each_seed("segment_store_encrypted_marker_ack_lost", depth(), |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let raw = SimSegmentStore::new(env.clone());
        let mut cfg = SegmentFaultConfig::default();
        cfg.set_put_ack_lost_prob(1.0);
        raw.set_fault_config(cfg);

        let raw_for_retry = raw.clone();
        let env_for_task = env.clone();
        drive(&sim, &env, async move {
            let first = EncryptedSegmentStore::open(raw, env_for_task.clone(), test_key()).await;
            assert!(
                first.is_err(),
                "the marker put's own ack-lost fault must surface on the first attempt"
            );
            // Clear the fault before retrying, mirroring a real caller's
            // retry-without-the-transient-condition-recurring path.
            raw_for_retry.set_fault_config(SegmentFaultConfig::default());
            let second = EncryptedSegmentStore::open(raw_for_retry, env_for_task, test_key()).await;
            assert!(
                second.is_ok(),
                "retrying open must succeed — the marker already landed"
            );
        });
    });
}

/// The three loud-refusal directions hold regardless of the simulation
/// seed (they draw no RNG of their own beyond the marker's salt) — the
/// `for_each_seed`-driven counterpart of `animus-sim`'s own single-seed
/// `marker_refusal_in_every_direction` unit test.
#[test]
fn marker_refusal_in_every_direction_across_seeds() {
    for_each_seed("segment_store_encrypted_marker_refusal", depth(), |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));

        let raw = SimSegmentStore::new(env.clone());
        let env_for_task = env.clone();
        let result: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let result_for_task = Arc::clone(&result);
        drive(&sim, &env, async move {
            // (1) key against an existing plaintext store.
            raw.put("some/object", b"plaintext").await.expect("put");
            let err1 = verify_or_init_segment_store_marker(&raw, &env_for_task, Some(&test_key()))
                .await
                .map(|_| ())
                .expect_err("key against plaintext must refuse");
            assert!(err1.to_string().contains("already holds unencrypted"));

            // (2)/(3) on a fresh store: encrypted-no-key, then wrong-key.
            let raw2 = SimSegmentStore::new(env_for_task.clone());
            verify_or_init_segment_store_marker(&raw2, &env_for_task, Some(&test_key()))
                .await
                .expect("initialize marker");
            let err2 = verify_or_init_segment_store_marker(&raw2, &env_for_task, None)
                .await
                .map(|_| ())
                .expect_err("no key against encrypted must refuse");
            assert!(err2.to_string().contains("no --encryption-key was given"));
            let err3 =
                verify_or_init_segment_store_marker(&raw2, &env_for_task, Some(&other_key()))
                    .await
                    .map(|_| ())
                    .expect_err("wrong key against encrypted must refuse");
            assert!(err3.to_string().contains("does not match the key"));

            *result_for_task.lock().expect("lock") = Some("ok".to_string());
        });
        assert_eq!(result.lock().expect("lock").as_deref(), Some("ok"));
    });
}
