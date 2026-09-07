//! [`SimSegmentStore`] (ADR 0043 §A7): a deterministic, fault-injectable
//! in-memory [`SegmentStore`] — the corpus's own store.
//!
//! Every source of randomness this store draws (fault sampling) comes off
//! the sim's own seeded RNG stream — the *same* stream every other seam
//! draws from, via whichever [`SimEnv`] handle the store was built with
//! (every node in one [`crate::Simulator`] shares one underlying RNG stream,
//! see `SimState`) — and every notion of time comes off the sim's virtual
//! clock. No second entropy source, so a whole simulation run — including
//! `SegmentStore` faults — stays a pure function of its seed, mirroring the
//! discipline [`crate::DiskConfig`]/[`crate::NetConfig`] already establish
//! for disk/network fault injection.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};

use animus_env::{Clock, Nanos, Rng as RngTrait, SegmentStore};

use crate::SimEnv;

/// Fault-injection knobs for [`SimSegmentStore`], mirroring
/// [`crate::DiskConfig`]'s shape: every threshold defaults to 0, so a store
/// with no configured fault draws **no** RNG at all and behaves exactly
/// like a plain deterministic in-memory map — a fault schedule is strictly
/// opt-in per test, just as the disk fault model is.
#[derive(Clone, Copy, Default)]
pub struct SegmentFaultConfig {
    /// `put` still writes the object (so a later `get` sees it) but the
    /// caller receives an injected `io::Error`, when `rng.next_u64() <
    /// threshold` — the exact ambiguity ADR 0043 §A3's seal step must
    /// tolerate on a real network ("crash/error before the catalog commit,
    /// retry the same deterministic id"). **Unlike the disk fault model,
    /// this does not skip the state change** — that ambiguity (object
    /// landed, ack lost) is the whole point of the fault.
    put_ack_lost_threshold: u64,
    /// `delete`'s ack-lost counterpart: the id is removed, but the caller
    /// sees an injected error.
    delete_ack_lost_threshold: u64,
}

impl SegmentFaultConfig {
    /// Set the independent per-`put` ack-lost probability in `[0.0, 1.0]`.
    pub fn set_put_ack_lost_prob(&mut self, p: f64) {
        self.put_ack_lost_threshold = prob_to_threshold(p);
    }

    /// Set the independent per-`delete` ack-lost probability in `[0.0,
    /// 1.0]`.
    pub fn set_delete_ack_lost_prob(&mut self, p: f64) {
        self.delete_ack_lost_threshold = prob_to_threshold(p);
    }
}

fn prob_to_threshold(p: f64) -> u64 {
    (p.clamp(0.0, 1.0) * (u64::MAX as f64)) as u64
}

struct Inner {
    objects: BTreeMap<String, Vec<u8>>,
    fault: SegmentFaultConfig,
    /// While `Some(deadline)` and `now() < deadline`, every op errors
    /// "unavailable" with **no** state change. Cleared by
    /// [`SimSegmentStore::clear_unavailable`] or once virtual time reaches
    /// the deadline.
    unavailable_until: Option<Nanos>,
}

/// A deterministic, seeded, fault-injectable in-memory [`SegmentStore`]
/// (ADR 0043 §A7) — the corpus's own store, not the production default
/// (`ClusterSegmentStore`, a later PR). Cheap to clone: clones share state
/// via an `Arc` and draw from the same underlying RNG stream regardless of
/// which clone is used.
#[derive(Clone)]
pub struct SimSegmentStore {
    env: SimEnv,
    inner: Arc<Mutex<Inner>>,
}

impl SimSegmentStore {
    /// Build a store whose fault sampling draws off `env`'s `Rng` and whose
    /// unavailability windows are measured against `env`'s `Clock`. `env`
    /// may be any node's [`SimEnv`] handle from the same [`crate::Simulator`]
    /// — every node's `Rng` draws from that one `Simulator`'s single shared
    /// stream, so which handle is used does not change determinism.
    #[must_use]
    pub fn new(env: SimEnv) -> Self {
        SimSegmentStore {
            env,
            inner: Arc::new(Mutex::new(Inner {
                objects: BTreeMap::new(),
                fault: SegmentFaultConfig::default(),
                unavailable_until: None,
            })),
        }
    }

    /// Replace the fault-injection config (ack-lost thresholds). Takes
    /// effect immediately for every clone sharing this store's state.
    pub fn set_fault_config(&self, cfg: SegmentFaultConfig) {
        self.lock().fault = cfg;
    }

    /// Make every op error "unavailable" until virtual time reaches
    /// `deadline`, or [`clear_unavailable`](Self::clear_unavailable) is
    /// called first. No RNG draw — the deadline is caller-chosen, not
    /// sampled.
    pub fn set_unavailable_until(&self, deadline: Nanos) {
        self.lock().unavailable_until = Some(deadline);
    }

    /// Heal early: clear any unavailability window regardless of virtual
    /// time.
    pub fn clear_unavailable(&self) {
        self.lock().unavailable_until = None;
    }

    /// Every id currently stored, for test assertions that want to inspect
    /// state directly rather than through `list` (which is prefix-scoped
    /// and itself subject to the unavailability window).
    #[must_use]
    pub fn stored_ids(&self) -> Vec<String> {
        self.lock().objects.keys().cloned().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("sim segment store poisoned")
    }

    fn check_unavailable(&self) -> io::Result<()> {
        let until = self.lock().unavailable_until;
        match until {
            Some(deadline) if self.env.now() < deadline => {
                Err(io::Error::other("sim segment store: unavailability window"))
            }
            _ => Ok(()),
        }
    }

    /// Draw an ack-lost verdict for `threshold` off the shared sim RNG
    /// stream — **no draw at all** when `threshold == 0`, so a store with no
    /// fault configured perturbs neither the RNG stream nor any other
    /// test's determinism (mirrors `DiskConfig::inject_disk_fault`'s own
    /// "draw only when the rate is non-zero" rule).
    fn roll(&self, threshold: u64) -> bool {
        threshold != 0 && self.env.next_u64() < threshold
    }
}

#[async_trait::async_trait]
impl SegmentStore for SimSegmentStore {
    async fn put(&self, id: &str, bytes: &[u8]) -> io::Result<()> {
        self.check_unavailable()?;
        // Write-once (`SegmentStore::put`'s own amended contract): a
        // differing-content rewrite of an existing id is a hard error,
        // checked before any fault sampling — a write-once violation is a
        // caller bug, not a fault this store injects.
        if let Some(existing) = self.lock().objects.get(id) {
            if existing != bytes {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "sim segment store write-once violation: {id:?} already holds \
                         different content"
                    ),
                ));
            }
            return Ok(()); // identical content: safe no-op, no fault sampling
        }
        let ack_lost = self.roll(self.lock().fault.put_ack_lost_threshold);
        self.lock().objects.insert(id.to_string(), bytes.to_vec());
        if ack_lost {
            return Err(io::Error::other(
                "sim segment store: injected put ack-lost (object written, ack dropped)",
            ));
        }
        Ok(())
    }

    async fn get(&self, id: &str) -> io::Result<Option<Vec<u8>>> {
        self.check_unavailable()?;
        Ok(self.lock().objects.get(id).cloned())
    }

    async fn delete(&self, id: &str) -> io::Result<()> {
        self.check_unavailable()?;
        let ack_lost = self.roll(self.lock().fault.delete_ack_lost_threshold);
        self.lock().objects.remove(id);
        if ack_lost {
            return Err(io::Error::other(
                "sim segment store: injected delete ack-lost (object removed, ack dropped)",
            ));
        }
        Ok(())
    }

    async fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.check_unavailable()?;
        Ok(self
            .lock()
            .objects
            .keys()
            .filter(|id| id.starts_with(prefix))
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Simulator;
    use animus_env::test_support::assert_segment_store_contract;
    use animus_env::{EnvExt, nid};
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;

    /// `animus-sim` has no tokio dependency: async work is driven by the
    /// `Simulator`'s own cooperative executor, not a real runtime — the
    /// established idiom (see `tests/disk_faults.rs`) is to spawn the async
    /// workload as a task that stashes its result in a shared `Arc<Mutex<_>>`,
    /// then drive the simulator to quiescence and read the result back. None
    /// of `SimSegmentStore`'s own ops genuinely suspend (no `sleep`), so a
    /// spawned task always completes within the first drain; a panicking
    /// assertion inside the task propagates out of `run_until_quiescent`
    /// (polled synchronously on the calling thread) exactly like any other
    /// panic, so a bare `assert!`/`assert_eq!` inside the spawned block fails
    /// the test normally — no completion flag needed for those. Bounded by
    /// `MAX_STEPS` as a guard against a scenario that never settles.
    const MAX_STEPS: usize = 10_000;

    #[test]
    fn satisfies_the_segment_store_contract() {
        let mut sim = Simulator::new(1);
        let store = SimSegmentStore::new(sim.env(nid(0)));
        let store_for_task = store.clone();
        sim.env(nid(0))
            .spawn_task(async move { assert_segment_store_contract(&store_for_task).await });
        assert!(sim.run_until_quiescent(MAX_STEPS), "must settle");
    }

    // --- Encryption at rest for `SegmentStore` (ADR 0069, S-03 PR 2) -------

    use animus_env::{EncryptedSegmentStore, EncryptionKey, SEGMENT_STORE_MARKER_ID};

    fn test_key(byte: u8) -> EncryptionKey {
        EncryptionKey::from_bytes([byte; 32])
    }

    /// `EncryptedSegmentStore<SimSegmentStore, SimEnv>` satisfies the same
    /// shared contract every plaintext `SegmentStore` implementor does —
    /// round trip, write-once (checked at the plaintext level — see the
    /// wrapper's own module doc), delete idempotence, prefix-filtered
    /// `list`, none of it disturbed by sealing every object underneath.
    #[test]
    fn encrypted_store_satisfies_the_segment_store_contract() {
        let mut sim = Simulator::new(0x5EC0_0001);
        let env = sim.env(nid(0));
        let inner = SimSegmentStore::new(env.clone());
        let env_for_task = env.clone();
        env.spawn_task(async move {
            let enc = EncryptedSegmentStore::open(inner, env_for_task, test_key(1))
                .await
                .expect("open a fresh (unencrypted) store with a key");
            assert_segment_store_contract(&enc).await;
        });
        assert!(sim.run_until_quiescent(MAX_STEPS), "must settle");
    }

    /// A plain round trip through the wrapper, and proof the underlying
    /// (unwrapped) store holds ciphertext, not the plaintext value.
    #[test]
    fn round_trips_and_the_underlying_bytes_are_not_plaintext() {
        let mut sim = Simulator::new(0x5EC0_0002);
        let env = sim.env(nid(0));
        let raw = SimSegmentStore::new(env.clone());
        let raw_for_asserts = raw.clone();
        let env_for_task = env.clone();
        env.spawn_task(async move {
            let enc = EncryptedSegmentStore::open(raw, env_for_task, test_key(2))
                .await
                .expect("open");
            enc.put("table/label/1/0", b"super-secret-value")
                .await
                .expect("put");
            assert_eq!(
                enc.get("table/label/1/0").await.expect("get"),
                Some(b"super-secret-value".to_vec())
            );
            let underlying = raw_for_asserts
                .get("table/label/1/0")
                .await
                .expect("get raw")
                .expect("object present on the wrapped store");
            assert_ne!(
                underlying, b"super-secret-value",
                "the wrapped store must never see the plaintext bytes"
            );
        });
        assert!(sim.run_until_quiescent(MAX_STEPS), "must settle");
    }

    /// Opening the same (now-encrypted) store with the wrong key is a hard
    /// error at open time (the marker check) — never a silent fallthrough
    /// to plaintext.
    #[test]
    fn wrong_key_at_open_is_refused() {
        let mut sim = Simulator::new(0x5EC0_0003);
        let env = sim.env(nid(0));
        let raw = SimSegmentStore::new(env.clone());
        let env_for_task = env.clone();
        env.spawn_task(async move {
            EncryptedSegmentStore::open(raw.clone(), env_for_task.clone(), test_key(3))
                .await
                .expect("first open with key 3 initializes the marker");
            let err = EncryptedSegmentStore::open(raw, env_for_task, test_key(4))
                .await
                .map(|_| ())
                .expect_err("a different key must be refused");
            let msg = err.to_string();
            assert!(
                msg.contains("does not match the key"),
                "unexpected message: {msg}"
            );
        });
        assert!(sim.run_until_quiescent(MAX_STEPS), "must settle");
    }

    /// A `get` of an object written under one key, read back under a
    /// different key that DOES authenticate the marker (impossible in
    /// practice since `open` itself refuses first) is not reachable — but a
    /// tampered object on an already-opened, correctly-keyed store must
    /// still be a named, hard `get` error, never a silent `None` or garbage
    /// plaintext.
    #[test]
    fn a_tampered_object_fails_loudly_and_names_the_id() {
        let mut sim = Simulator::new(0x5EC0_0004);
        let env = sim.env(nid(0));
        let raw = SimSegmentStore::new(env.clone());
        let raw_for_tamper = raw.clone();
        let env_for_task = env.clone();
        env.spawn_task(async move {
            let enc = EncryptedSegmentStore::open(raw, env_for_task, test_key(5))
                .await
                .expect("open");
            enc.put("t/lbl/1/0", b"hello world").await.expect("put");
            let mut bytes = raw_for_tamper
                .get("t/lbl/1/0")
                .await
                .expect("get raw")
                .expect("present");
            let last = bytes.len() - 1;
            bytes[last] ^= 0xFF; // flip the AEAD tag's last byte
            raw_for_tamper
                .put("t/lbl/1/0", &bytes)
                .await
                .expect_err("SimSegmentStore write-once forbids overwriting with different bytes");
            // Overwrite via delete+put since SimSegmentStore enforces
            // write-once on its own raw bytes too.
            raw_for_tamper.delete("t/lbl/1/0").await.expect("delete");
            raw_for_tamper
                .put("t/lbl/1/0", &bytes)
                .await
                .expect("re-put the tampered bytes");

            let err = enc
                .get("t/lbl/1/0")
                .await
                .expect_err("a tampered object must be a hard error, never a silent None");
            let msg = err.to_string();
            assert!(msg.contains("t/lbl/1/0"), "must name the object id: {msg}");
            assert!(
                msg.contains("write-once"),
                "must explain why this can never be a torn write: {msg}"
            );
        });
        assert!(sim.run_until_quiescent(MAX_STEPS), "must settle");
    }

    /// The three loud-refusal directions (mirroring `verify_or_init_marker`'s
    /// own `Disk`-level test coverage): encrypted-no-key, wrong-key, and
    /// key-against-plaintext.
    #[test]
    fn marker_refusal_in_every_direction() {
        use animus_env::verify_or_init_segment_store_marker;

        let mut sim = Simulator::new(0x5EC0_0005);
        let env = sim.env(nid(0));

        // (1) Key against an existing plaintext store (already holding an
        // ordinary object) is refused.
        {
            let store = SimSegmentStore::new(env.clone());
            let env_for_task = env.clone();
            env.spawn_task(async move {
                store.put("some/object", b"plaintext").await.expect("put");
                let err =
                    verify_or_init_segment_store_marker(&store, &env_for_task, Some(&test_key(6)))
                        .await
                        .expect_err("a key against a plaintext store must be refused");
                assert!(err.to_string().contains("already holds unencrypted"));
            });
        }
        assert!(sim.run_until_quiescent(MAX_STEPS), "must settle (1)");

        // (2) Encrypted store, no key given: refused.
        {
            let store = SimSegmentStore::new(env.clone());
            let env_for_task = env.clone();
            env.spawn_task(async move {
                verify_or_init_segment_store_marker(&store, &env_for_task, Some(&test_key(7)))
                    .await
                    .expect("initialize the marker under key 7");
                let err = verify_or_init_segment_store_marker(&store, &env_for_task, None)
                    .await
                    .expect_err("no key against an encrypted store must be refused");
                assert!(err.to_string().contains("no --encryption-key was given"));
            });
        }
        assert!(sim.run_until_quiescent(MAX_STEPS), "must settle (2)");

        // (3) Encrypted store, wrong key: refused.
        {
            let store = SimSegmentStore::new(env.clone());
            let env_for_task = env.clone();
            env.spawn_task(async move {
                verify_or_init_segment_store_marker(&store, &env_for_task, Some(&test_key(8)))
                    .await
                    .expect("initialize the marker under key 8");
                let err =
                    verify_or_init_segment_store_marker(&store, &env_for_task, Some(&test_key(9)))
                        .await
                        .expect_err("the wrong key against an encrypted store must be refused");
                assert!(err.to_string().contains("does not match the key"));
            });
        }
        assert!(sim.run_until_quiescent(MAX_STEPS), "must settle (3)");

        // (4) A fresh store with no key stays plaintext, untouched — and the
        // marker itself is hidden from `list`.
        {
            let store = SimSegmentStore::new(env.clone());
            let env_for_task = env.clone();
            env.spawn_task(async move {
                verify_or_init_segment_store_marker(&store, &env_for_task, None)
                    .await
                    .expect("no key, fresh store: proceed plaintext");
                assert!(
                    store.list("").await.expect("list").is_empty(),
                    "a plaintext store with no key must remain untouched"
                );

                verify_or_init_segment_store_marker(&store, &env_for_task, Some(&test_key(10)))
                    .await
                    .expect("initialize the marker now");
                let enc = EncryptedSegmentStore::open(store.clone(), env_for_task, test_key(10))
                    .await
                    .expect("open");
                enc.put("x/y/1/0", b"v").await.expect("put");
                let listed = enc.list("").await.expect("list via wrapper");
                assert_eq!(listed, vec!["x/y/1/0".to_string()]);
                let raw_listed = store.list("").await.expect("raw list");
                assert!(
                    raw_listed.contains(&SEGMENT_STORE_MARKER_ID.to_string()),
                    "the marker really is present on the raw store: {raw_listed:?}"
                );
            });
        }
        assert!(sim.run_until_quiescent(MAX_STEPS), "must settle (4)");
    }

    /// The exact ambiguity a seal step must tolerate: a `put` whose caller
    /// sees an error can still have landed the object — a subsequent `get`
    /// (even from a different clone of the same store) sees it.
    #[test]
    fn ack_lost_put_leaves_the_object_visible_despite_the_error() {
        let mut sim = Simulator::new(2);
        let store = SimSegmentStore::new(sim.env(nid(0)));
        let mut cfg = SegmentFaultConfig::default();
        cfg.set_put_ack_lost_prob(1.0); // always ack-lost
        store.set_fault_config(cfg);

        let store_for_task = store.clone();
        sim.env(nid(0)).spawn_task(async move {
            let err = store_for_task
                .put("t/label/1/0", b"payload")
                .await
                .expect_err("put must surface the injected ack-lost error");
            drop(err);

            // A fresh clone of the same store (as a different
            // component/task would hold) sees the object the "failed" put
            // actually wrote.
            let other_handle = store_for_task.clone();
            assert_eq!(
                other_handle.get("t/label/1/0").await.expect("get"),
                Some(b"payload".to_vec()),
                "the object must have landed even though the caller saw an error"
            );
        });
        assert!(sim.run_until_quiescent(MAX_STEPS), "must settle");
    }

    /// The delete counterpart: the id is actually removed even though the
    /// caller is told the delete failed.
    #[test]
    fn ack_lost_delete_still_removes_the_object() {
        let mut sim = Simulator::new(3);
        let store = SimSegmentStore::new(sim.env(nid(0)));
        let mut cfg = SegmentFaultConfig::default();
        cfg.set_delete_ack_lost_prob(1.0);
        store.set_fault_config(cfg);

        let store_for_task = store.clone();
        sim.env(nid(0)).spawn_task(async move {
            store_for_task.put("t/label/1/0", b"x").await.expect("put");
            store_for_task
                .delete("t/label/1/0")
                .await
                .expect_err("delete must surface the injected ack-lost error");
            assert_eq!(
                store_for_task.get("t/label/1/0").await.expect("get"),
                None,
                "the object must actually be gone despite the caller seeing an error"
            );
        });
        assert!(sim.run_until_quiescent(MAX_STEPS), "must settle");
    }

    /// Write-once (the ledger-named-object amendment): a second `put` to the
    /// same id with different bytes is a hard error and leaves the stored
    /// content untouched; a second `put` with byte-identical content is a
    /// safe no-op (the "same-attempt retry after a lost ack" and "repair
    /// sweep copies the same bytes to a fresh replica" cases).
    #[test]
    fn put_is_write_once_except_for_identical_content() {
        let mut sim = Simulator::new(6);
        let store = SimSegmentStore::new(sim.env(nid(0)));
        let store_for_task = store.clone();
        sim.env(nid(0)).spawn_task(async move {
            store_for_task
                .put("t/label/1/0", b"first")
                .await
                .expect("first put");

            // Identical bytes: safe no-op.
            store_for_task
                .put("t/label/1/0", b"first")
                .await
                .expect("identical-content put must succeed");
            assert_eq!(
                store_for_task.get("t/label/1/0").await.expect("get"),
                Some(b"first".to_vec())
            );

            // Different bytes: hard error, no state change.
            let err = store_for_task
                .put("t/label/1/0", b"second")
                .await
                .expect_err("a write-once violation must be rejected");
            drop(err);
            assert_eq!(
                store_for_task.get("t/label/1/0").await.expect("get"),
                Some(b"first".to_vec()),
                "a rejected write-once violation must not change the stored bytes"
            );

            // A genuinely fresh id is unaffected.
            store_for_task
                .put("t/label/1/1", b"second")
                .await
                .expect("a different id is a plain first write");
        });
        assert!(sim.run_until_quiescent(MAX_STEPS), "must settle");
    }

    /// An unavailability window fails every op (no state change) until
    /// virtual time reaches the deadline, then heals on its own.
    #[test]
    fn unavailability_window_errors_every_op_then_heals_at_the_deadline() {
        let mut sim = Simulator::new(4);
        let env = sim.env(nid(0));
        let store = SimSegmentStore::new(env.clone());

        let store_for_task = store.clone();
        let env_for_task = env.clone();
        sim.env(nid(0)).spawn_task(async move {
            store_for_task
                .put("t/label/1/0", b"before")
                .await
                .expect("seed put");

            let deadline = Nanos(env_for_task.now().0 + 1_000_000_000);
            store_for_task.set_unavailable_until(deadline);

            assert!(store_for_task.put("t/label/1/1", b"x").await.is_err());
            assert!(store_for_task.get("t/label/1/0").await.is_err());
            assert!(store_for_task.delete("t/label/1/0").await.is_err());
            assert!(store_for_task.list("t/").await.is_err());
            // No state change from the failed put/delete above.
            assert_eq!(store_for_task.stored_ids(), vec!["t/label/1/0".to_string()]);

            // Advance virtual time past the deadline and it heals on its
            // own.
            env_for_task.sleep(Duration::from_secs(2)).await;
            assert!(
                env_for_task.now() >= deadline,
                "sleep must actually advance time"
            );
            assert_eq!(
                store_for_task.get("t/label/1/0").await.expect("healed get"),
                Some(b"before".to_vec())
            );
            store_for_task
                .put("t/label/1/1", b"after")
                .await
                .expect("healed put");
        });
        // Bounded by wall-clock-independent virtual time, not step count: the
        // task's own `sleep(2s)` needs the timeline actually advanced past
        // it.
        sim.run_for(Duration::from_secs(3));
    }

    /// `clear_unavailable` heals early, regardless of virtual time.
    #[test]
    fn clear_unavailable_heals_before_the_deadline() {
        let mut sim = Simulator::new(5);
        let env = sim.env(nid(0));
        let store = SimSegmentStore::new(env.clone());

        let store_for_task = store.clone();
        let env_for_task = env.clone();
        sim.env(nid(0)).spawn_task(async move {
            store_for_task.set_unavailable_until(Nanos(env_for_task.now().0 + 10_000_000_000));
            assert!(store_for_task.put("t/label/1/0", b"x").await.is_err());

            store_for_task.clear_unavailable();
            store_for_task
                .put("t/label/1/0", b"x")
                .await
                .expect("put must succeed once cleared, well before the original deadline");
        });
        assert!(sim.run_until_quiescent(MAX_STEPS), "must settle");
    }

    /// Determinism: the same seed plus the same fault schedule produces the
    /// identical sequence of outcomes (Ok/Err) across two independent runs —
    /// the guarantee every sim-testable component in this codebase relies
    /// on (ADR 0003).
    #[test]
    fn same_seed_and_schedule_reproduce_identical_outcomes() {
        fn run(seed: u64) -> Vec<bool> {
            let mut sim = Simulator::new(seed);
            let store = SimSegmentStore::new(sim.env(nid(0)));
            let mut cfg = SegmentFaultConfig::default();
            cfg.set_put_ack_lost_prob(0.5);
            cfg.set_delete_ack_lost_prob(0.5);
            store.set_fault_config(cfg);

            let outcomes = Arc::new(StdMutex::new(Vec::new()));
            let outcomes_for_task = Arc::clone(&outcomes);
            let store_for_task = store.clone();
            sim.env(nid(0)).spawn_task(async move {
                for i in 0..20u32 {
                    let id = format!("t/label/1/{i}");
                    let put_ok = store_for_task.put(&id, b"x").await.is_ok();
                    let delete_ok = store_for_task.delete(&id).await.is_ok();
                    let mut out = outcomes_for_task.lock().expect("outcomes poisoned");
                    out.push(put_ok);
                    out.push(delete_ok);
                }
            });
            assert!(sim.run_until_quiescent(MAX_STEPS), "must settle");
            Arc::try_unwrap(outcomes)
                .expect("no other Arc holders after quiescence")
                .into_inner()
                .expect("outcomes poisoned")
        }

        let a = run(42);
        let b = run(42);
        assert_eq!(
            a, b,
            "identical seed + schedule must reproduce byte-identically"
        );

        // Sanity: the fault actually fires at least once at p=0.5 over 40
        // draws, so this test would catch a config that silently no-ops.
        assert!(
            a.contains(&false),
            "expected at least one injected failure: {a:?}"
        );
        assert!(a.contains(&true), "expected at least one success: {a:?}");

        let c = run(43);
        assert_ne!(a, c, "a different seed must (almost certainly) diverge");
    }
}
