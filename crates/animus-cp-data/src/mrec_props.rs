//! ADR 0075 G-d M2: the **pure convergence oracle** for MREC last-writer-wins
//! apply, in miniature (no `Env`, no Raft, no engine).
//!
//! A *replica* here is a map `key -> stored base-row bytes`, advanced only by
//! [`crate::evaluate_kind_eval`] — the real apply decision, the real stamp rule
//! (`MrecVersion::next_local`), the real versioned encoders — fed with the
//! stored row decoded exactly as `KvCommand::KindEval`'s apply arm decodes it.
//! A seeded *history* interleaves local writes (stamped at whichever region
//! performs them, from that region's own current row) with deliveries of
//! previously produced records to arbitrary regions; a final *anti-entropy*
//! phase then delivers **every** record to **every** region in a random order
//! with random duplication. The properties:
//!
//! 1. **Convergence**: all regions hold byte-identical rows (item + version +
//!    tombstones) for every key.
//! 2. **The oracle**: that row is the maximum-version record ever produced for
//!    the key (so a delete that is the max stays deleted: **no resurrection**,
//!    and a surviving write is never lost).
//! 3. **Causality per item**: a local write made after its region observed a
//!    version is strictly greater than it, and carries the local region id.
//! 4. **Idempotence**: re-delivering a record to its own writer is a no-op.
//!
//! The **negative controls** run the same harness with a deliberately broken
//! apply rule — arrival-order LWW (the stored stamp is ignored, so the last
//! delivery wins) and a dropped region tiebreak — and the harness must catch
//! both (`negative_control_*`): a harness that cannot fail proves nothing.
//!
//! Depth: `ANIMUS_MREC_PROP_CASES=K` (default 256).

use std::collections::BTreeMap;

use animus_item::{
    AttributeValue, Item, MrecVersion, MrecWriteStamp, TableSchema, WriteSchema,
    decode_stored_item_versioned,
};
use proptest::prelude::*;

use crate::{KIND_BASE, KindEvalDecision, KindEvalOp, evaluate_kind_eval};

/// How a replica decides a replicated record. `Real` is the production rule;
/// the others are the negative controls' deliberately broken variants.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Real,
    /// Ignore the stored stamp: whatever arrives last wins.
    ArrivalOrder,
    /// Compare `(wall_ms, logical)` only; an equal pair keeps the first arrival.
    NoRegionTiebreak,
}

/// One produced record: what a local write shipped.
#[derive(Clone, Debug)]
struct Record {
    key: u8,
    item: Option<Item>,
    ver: MrecVersion,
}

struct Replica {
    region_id: u32,
    rows: BTreeMap<u8, Vec<u8>>,
}

fn pk(key: u8) -> AttributeValue {
    AttributeValue::S(format!("k{key}"))
}

fn token(key: u8) -> Vec<u8> {
    animus_tablet::partition_token(&animus_item::storage_key(&pk(key), None)).to_vec()
}

fn schema(region_id: u32, wall_ms: u64) -> WriteSchema {
    WriteSchema {
        key: TableSchema::simple("pk"),
        lsis: Vec::new(),
        change_records_carry_images: true,
        mrec: Some(MrecWriteStamp { region_id, wall_ms }),
    }
}

impl Replica {
    fn new(region_id: u32) -> Self {
        Self {
            region_id,
            rows: BTreeMap::new(),
        }
    }

    fn stored(&self, key: u8) -> (Option<Item>, Option<MrecVersion>) {
        self.rows.get(&key).map_or((None, None), |b| {
            decode_stored_item_versioned(b).expect("decodes")
        })
    }

    /// Run the real apply decision for `op` on `key` and install its base row.
    /// Returns the produced `(item, ver)` when something was written.
    fn apply(
        &mut self,
        mode: Mode,
        key: u8,
        op: &KindEvalOp,
        wall_ms: u64,
    ) -> Option<(Option<Item>, MrecVersion)> {
        let (old, mut stored_ver) = self.stored(key);
        if let KindEvalOp::Replicate { ver, .. } = op {
            match mode {
                Mode::Real => {}
                Mode::ArrivalOrder => stored_ver = None,
                Mode::NoRegionTiebreak => {
                    if let Some(st) = stored_ver
                        && (ver.wall_ms, ver.logical) <= (st.wall_ms, st.logical)
                    {
                        return None;
                    }
                }
            }
        }
        let decision = evaluate_kind_eval(
            &schema(self.region_id, wall_ms),
            &pk(key),
            None,
            &token(key),
            old,
            stored_ver,
            op,
            None,
            false,
        );
        match decision {
            KindEvalDecision::Applied {
                writes,
                new,
                new_ver,
                ..
            } => {
                let base = writes
                    .into_iter()
                    .find(|(kind, _, _)| *kind == KIND_BASE)
                    .and_then(|(_, _, v)| v)
                    .expect("a base row is always written");
                self.rows.insert(key, base);
                Some((new, new_ver.expect("an MREC write is always stamped")))
            }
            KindEvalDecision::Superseded { .. } => None,
            other => panic!("unexpected decision {other:?}"),
        }
    }
}

/// A tiny deterministic RNG for the delivery shuffles (the proptest-chosen
/// `seed` makes a failing case replayable and shrinkable).
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// One scripted step of the history phase.
#[derive(Clone, Debug)]
struct Step {
    region: u8,
    key: u8,
    /// 0 put, 1 delete, 2 TTL-style delete behind the clock, 3.. deliver a record.
    what: u8,
    wall: u8,
    pick: u16,
}

fn run(mode: Mode, regions: usize, keys: u8, steps: &[Step], seed: u64) -> Result<(), String> {
    // Distinct, deliberately unordered-looking region ids (also forces ties on
    // wall_ms across regions through the small `wall` range below).
    let ids = [0x0000_0a01_u32, 0x0000_0003, 0xffff_0002, 0x0000_0c07];
    let mut reps: Vec<Replica> = (0..regions).map(|i| Replica::new(ids[i])).collect();
    let mut records: Vec<Record> = Vec::new();
    let mut counter = 0u64;

    // History: local writes interleaved with arbitrary deliveries.
    for st in steps {
        let r = usize::from(st.region) % regions;
        let key = st.key % keys;
        if st.what < 3 {
            let wall = 1 + u64::from(st.wall % 6);
            let (_, before) = reps[r].stored(key);
            let op = if st.what == 0 {
                counter += 1;
                let mut item = Item::new();
                item.insert("pk".to_owned(), pk(key));
                item.insert("v".to_owned(), AttributeValue::N(counter.to_string()));
                KindEvalOp::Put(item)
            } else {
                KindEvalOp::Delete
            };
            let wall = if st.what == 2 { 1 } else { wall };
            let Some((item, ver)) = reps[r].apply(mode, key, &op, wall) else {
                return Err(format!("a local write was refused: {st:?}"));
            };
            // Causality per item: strictly above whatever this region had
            // observed, stamped with its own region id.
            if ver <= before.unwrap_or(MrecVersion::ZERO) || ver.region_id != reps[r].region_id {
                return Err(format!(
                    "causality violated: wrote {ver:?} over observed {before:?} ({st:?})"
                ));
            }
            // Idempotence: re-delivering the record to its own writer is a no-op
            // (equal stamp does not supersede), so a re-delivery never re-fires
            // a change record.
            let mut again = Replica::new(reps[r].region_id);
            again.rows.clone_from(&reps[r].rows);
            let replay = KindEvalOp::Replicate {
                item: item.clone(),
                ver,
            };
            if mode == Mode::Real && again.apply(mode, key, &replay, 1).is_some() {
                return Err(format!(
                    "re-applying an own record was not a no-op: {ver:?}"
                ));
            }
            records.push(Record { key, item, ver });
        } else if !records.is_empty() {
            let rec = &records[usize::from(st.pick) % records.len()];
            let op = KindEvalOp::Replicate {
                item: rec.item.clone(),
                ver: rec.ver,
            };
            reps[r].apply(mode, rec.key, &op, 1);
        }
    }

    // Anti-entropy: every record to every region, random order, random duplication.
    let mut rng = Lcg(seed);
    let mut deliveries: Vec<(usize, usize)> = Vec::new();
    for ri in 0..records.len() {
        for r in 0..regions {
            deliveries.push((ri, r));
            while rng.below(3) == 0 {
                deliveries.push((ri, r));
            }
        }
    }
    for i in (1..deliveries.len()).rev() {
        deliveries.swap(i, rng.below(i + 1));
    }
    for (ri, r) in deliveries {
        let rec = records[ri].clone();
        let op = KindEvalOp::Replicate {
            item: rec.item,
            ver: rec.ver,
        };
        reps[r].apply(mode, rec.key, &op, 1);
    }

    // Oracle.
    for key in 0..keys {
        let want = records
            .iter()
            .filter(|r| r.key == key)
            .max_by_key(|r| r.ver);
        let first = reps[0].stored(key);
        for rep in &reps {
            if rep.rows.get(&key) != reps[0].rows.get(&key) {
                return Err(format!(
                    "divergence on key {key}: region {:#x} holds {:?}, region {:#x} holds {:?}",
                    reps[0].region_id,
                    first,
                    rep.region_id,
                    rep.stored(key)
                ));
            }
        }
        match want {
            None => {
                if reps[0].rows.contains_key(&key) {
                    return Err(format!("key {key} has a row but no record was produced"));
                }
            }
            Some(w) => {
                let (item, ver) = reps[0].stored(key);
                if ver != Some(w.ver) || item != w.item {
                    return Err(format!(
                        "key {key}: converged to {ver:?}/{item:?}, but the max-version record is {:?}/{:?} (resurrection or loss)",
                        w.ver, w.item
                    ));
                }
            }
        }
    }
    Ok(())
}

fn cases() -> u32 {
    std::env::var("ANIMUS_MREC_PROP_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256)
}

fn steps_strategy() -> impl Strategy<Value = Vec<Step>> {
    proptest::collection::vec(
        (0u8..4, 0u8..3, 0u8..8, 0u8..6, any::<u16>()).prop_map(
            |(region, key, what, wall, pick)| Step {
                region,
                key,
                what,
                wall,
                pick,
            },
        ),
        1..48,
    )
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), .. ProptestConfig::default() })]

    /// Any interleaving, reordering and duplication of the same set of stamped
    /// local writes and replicated records converges to the max-version row
    /// on every region.
    #[test]
    fn mrec_lww_converges_under_any_delivery_order(
        regions in 2usize..=4,
        keys in 1u8..=3,
        steps in steps_strategy(),
        seed in any::<u64>(),
    ) {
        if let Err(e) = run(Mode::Real, regions, keys, &steps, seed) {
            prop_assert!(false, "{e}");
        }
    }
}

/// Search deterministically for a case the harness flags under a broken rule.
fn first_violation(mode: Mode) -> Option<String> {
    for seed in 0..4000u64 {
        let mut rng = Lcg(seed ^ 0x9e37_79b9_7f4a_7c15);
        let regions = 2 + rng.below(3);
        let keys = 1 + rng.below(2) as u8;
        let steps: Vec<Step> = (0..(4 + rng.below(30)))
            .map(|_| Step {
                region: rng.below(4) as u8,
                key: rng.below(3) as u8,
                what: rng.below(8) as u8,
                wall: rng.below(6) as u8,
                pick: rng.below(65536) as u16,
            })
            .collect();
        if let Err(e) = run(mode, regions, keys, &steps, seed) {
            return Some(format!("seed {seed}: {e}"));
        }
    }
    None
}

/// Negative control: LWW by **arrival order** instead of version must be
/// caught by the harness (it diverges / loses the max-version write).
#[test]
fn negative_control_arrival_order_lww_is_caught() {
    assert_eq!(first_violation(Mode::Real), None, "the real rule must pass");
    let v = first_violation(Mode::ArrivalOrder)
        .expect("the oracle must catch arrival-order last-writer-wins");
    eprintln!("negative control (arrival order) failed as required: {v}");
}

/// Negative control: dropping the `region_id` tiebreak must be caught on
/// same-millisecond ties.
#[test]
fn negative_control_missing_region_tiebreak_is_caught() {
    let v = first_violation(Mode::NoRegionTiebreak)
        .expect("the oracle must catch a dropped region tiebreak");
    eprintln!("negative control (no region tiebreak) failed as required: {v}");
}
