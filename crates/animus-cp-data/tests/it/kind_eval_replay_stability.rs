//! A `KindEval` / `KindEvalBatch` the live apply REJECTED (condition failed,
//! or blocked by a foreign intent) must also be a no-op when WAL recovery
//! re-applies it over an engine that already holds later entries' effects
//! (issue #1247; the single-key sibling of #1242's `TxnStage`).
//!
//! The base row is protected by per-key last-writer-wins, but the derived rows
//! `materialize_derived` writes are not: the change-log record sits on a unique
//! `prefix || ts || ordinal` key and LSI rows are keyed by item attributes. On
//! replay the decision reads *future* state (the item was deleted since, the
//! foreign intent resolved since), flips to "applied", and lands an orphan
//! stream record / index row on the restarted replica alone.
//!
//! The harness is `txn_stage_replay_stability`'s: a fresh-process restart over
//! a retained engine, `assert_identical` over every replica's raw rows
//! (tombstones, change-log, LSI, markers all included).
//!
//! Deterministic and seed-reproducible (ADR 0003): `ANIMUS_SEED=<seed>` replays
//! one corpus schedule, `ANIMUS_KINDEVAL_REPLAY_SEEDS=K` widens it.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{KindBatchOutcome, KindEvalEntry, KindEvalOp, TxnOutcome};
use animus_env::nid;
use animus_item::{
    AttributeValue, ConditionExpression, Item, LsiDef, PathSegment, Projection, TableSchema,
    UpdateAction, WriteSchema, encode_stored_item,
};
use animus_tablet::partition_token;

use super::txn_stage_replay_stability::{BASE_SEED, Cluster, SETTLE, txn_id};

const SEED: u64 = BASE_SEED + 0x1247_0000;

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_owned())
}

fn n(v: u64) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

fn item(pk: &str, age: u64) -> Item {
    [("pk".to_owned(), s(pk)), ("age".to_owned(), n(age))]
        .into_iter()
        .collect()
}

fn schema() -> WriteSchema {
    WriteSchema {
        key: TableSchema::simple("pk"),
        lsis: vec![LsiDef {
            name: "byAge".to_owned(),
            sort_attribute: "age".to_owned(),
            projection: Projection::All,
        }],
        change_records_carry_images: true,
        mrec: None,
    }
}

fn base_key(pk: &str) -> Vec<u8> {
    let pk = s(pk);
    let mut key = partition_token(&animus_item::storage_key(&pk, None)).to_vec();
    key.extend_from_slice(&animus_item::storage_key(&pk, None));
    key
}

fn not_exists() -> Option<ConditionExpression> {
    Some(ConditionExpression::AttributeNotExists("pk".to_owned()))
}

impl Cluster {
    /// Propose one `KindEval` on the leader and return its recorded outcome.
    fn eval(
        &mut self,
        pk: &str,
        op: KindEvalOp,
        condition: Option<ConditionExpression>,
    ) -> KindBatchOutcome {
        let l = self.leader().expect("leader");
        let ProposeResult::Accepted { index, .. } =
            self.nodes[l].propose_kind_eval(schema(), s(pk), None, op, condition, false)
        else {
            panic!("KindEval refused (seed={})", self.seed);
        };
        self.sim.run_for(SETTLE);
        self.nodes[l]
            .kind_batch_outcome(index)
            .unwrap_or_else(|| panic!("no outcome (seed={})", self.seed))
            .1
    }

    fn eval_batch(&mut self, entries: Vec<KindEvalEntry>) {
        let l = self.leader().expect("leader");
        let ProposeResult::Accepted { .. } = self.nodes[l].propose_kind_eval_batch(entries) else {
            panic!("KindEvalBatch refused (seed={})", self.seed);
        };
        self.sim.run_for(SETTLE);
    }

    fn restart_a_follower(&mut self) {
        let follower = (0..3).find(|&i| i != self.leader().unwrap()).unwrap();
        self.restart_fresh(follower);
        self.sim.run_for(SETTLE);
    }
}

fn entry(pk: &str, op: KindEvalOp, condition: Option<ConditionExpression>) -> KindEvalEntry {
    KindEvalEntry {
        schema: schema(),
        pk: s(pk),
        sk: None,
        op,
        condition,
        ttl_expired: false,
    }
}

/// Live: `attribute_not_exists` fails because the item exists. Then the item is
/// deleted. Replay reads the key as absent, the condition passes, and the
/// change-log record + LSI row of the *rejected* put land on the restarted
/// replica only.
#[test]
fn condition_failed_eval_is_not_resurrected_by_replay_after_a_delete() {
    let mut c = Cluster::new(SEED);
    let first = c.eval("a", KindEvalOp::Put(item("a", 30)), None);
    assert!(matches!(first, KindBatchOutcome::Applied), "{first:?}");
    let rejected = c.eval("a", KindEvalOp::Put(item("a", 99)), not_exists());
    assert!(
        matches!(rejected, KindBatchOutcome::ConditionFailed { .. }),
        "live: condition must fail, got {rejected:?}"
    );
    let del = c.eval("a", KindEvalOp::Delete, None);
    assert!(matches!(del, KindBatchOutcome::Applied), "{del:?}");
    c.assert_identical(&[&base_key("a")], "before restart");
    c.restart_a_follower();
    c.assert_identical(&[&base_key("a")], "after follower replay");
}

/// The same shape through `UpdateItem` (a conditional `Update` whose replayed
/// evaluation would also derive a fresh LSI row from the re-read state).
#[test]
fn condition_failed_update_is_not_resurrected_by_replay() {
    let mut c = Cluster::new(SEED + 1);
    c.eval("a", KindEvalOp::Put(item("a", 30)), None);
    let rejected = c.eval(
        "a",
        KindEvalOp::Update {
            key_item: item("a", 0),
            actions: vec![UpdateAction::Set(
                vec![PathSegment::Field("age".to_owned())],
                animus_item::UpdateExpr::value(n(77)),
            )],
        },
        not_exists(),
    );
    assert!(matches!(rejected, KindBatchOutcome::ConditionFailed { .. }));
    c.eval("a", KindEvalOp::Delete, None);
    c.assert_identical(&[&base_key("a")], "before restart");
    c.restart_a_follower();
    c.assert_identical(&[&base_key("a")], "after follower replay");
}

/// Live: a foreign transaction's unresolved intent makes the `KindEval`
/// `ConditionFailed`. The intent is then resolved (committed). Replay sees a
/// plain committed item, no intent, and applies the put's derived rows.
#[test]
fn intent_blocked_eval_is_not_resurrected_by_replay_after_resolve() {
    let mut c = Cluster::new(SEED + 2);
    let k = base_key("a");
    let rk = base_key("anchor-elsewhere");
    let l = c.leader().unwrap();
    let writes = vec![animus_cp_data::TxnWrite::plain(
        k.clone(),
        Some(encode_stored_item(&item("a", 10))),
    )];
    let (n_, id, rk2) = (c.nodes[l].clone(), txn_id(1), rk.clone());
    let (ts, _) = super::txn_stage_replay_stability::drive(
        &mut c.sim,
        c.nodes[l].env(),
        SETTLE,
        async move {
            n_.txn_stage_participant(id, rk2, "t".into(), writes, Vec::new())
                .await
        },
    )
    .flatten()
    .expect("stage completes");
    let blocked = c.eval("a", KindEvalOp::Put(item("a", 55)), None);
    assert!(
        matches!(blocked, KindBatchOutcome::ConditionFailed { .. }),
        "live: blocked by the intent, got {blocked:?}"
    );
    c.resolve(
        l,
        &txn_id(1),
        &rk,
        std::slice::from_ref(&k),
        TxnOutcome::Committed { commit_ts: ts },
    );
    c.sim.run_for(SETTLE);
    c.assert_identical(&[&k], "before restart");
    c.restart_a_follower();
    c.assert_identical(&[&k], "after follower replay");
}

/// `BatchWriteItem`'s `KindEvalBatch`: one item rejected live, deleted after.
#[test]
fn condition_failed_batch_item_is_not_resurrected_by_replay() {
    let mut c = Cluster::new(SEED + 3);
    c.eval("a", KindEvalOp::Put(item("a", 30)), None);
    c.eval_batch(vec![
        entry("a", KindEvalOp::Put(item("a", 99)), not_exists()),
        entry("b", KindEvalOp::Put(item("b", 5)), not_exists()),
    ]);
    c.eval("a", KindEvalOp::Delete, None);
    c.assert_identical(&[&base_key("a"), &base_key("b")], "before restart");
    c.restart_a_follower();
    c.assert_identical(&[&base_key("a"), &base_key("b")], "after follower replay");
}

/// Equal-version case: an applied item's own write is visible on replay. Item 1
/// (`not_exists`) passes live and, re-evaluated over its own post-state, would
/// fail on replay, so item 2's change record would shift from ordinal 1 to
/// ordinal 0 — an orphan row on the restarted replica.
#[test]
fn batch_replay_does_not_shift_change_record_ordinals() {
    let mut c = Cluster::new(SEED + 4);
    c.eval_batch(vec![
        entry("a", KindEvalOp::Put(item("a", 10)), not_exists()),
        entry("b", KindEvalOp::Delete, not_exists()),
    ]);
    c.assert_identical(&[&base_key("a"), &base_key("b")], "before restart");
    c.restart_a_follower();
    c.assert_identical(&[&base_key("a"), &base_key("b")], "after follower replay");
}

/// Equal-version case for `UpdateItem`: `ADD age 1` replayed over its own
/// post-state would derive a second LSI row (age 32) on the restarted replica.
#[test]
fn update_add_is_not_applied_twice_by_replay() {
    let mut c = Cluster::new(SEED + 5);
    c.eval("a", KindEvalOp::Put(item("a", 30)), None);
    let out = c.eval(
        "a",
        KindEvalOp::Update {
            key_item: item("a", 0),
            actions: vec![UpdateAction::Add(
                vec![PathSegment::Field("age".to_owned())],
                n(1),
            )],
        },
        None,
    );
    assert!(matches!(out, KindBatchOutcome::Applied), "{out:?}");
    c.assert_identical(&[&base_key("a")], "before restart");
    c.restart_a_follower();
    c.assert_identical(&[&base_key("a")], "after follower replay");
}

// ---- seeded corpus ---------------------------------------------------------

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn run_schedule(seed: u64) {
    let mut c = Cluster::new(seed);
    let pks = ["k0", "k1", "k2"];
    let keys: Vec<Vec<u8>> = pks.iter().map(|p| base_key(p)).collect();
    let mut rng = Rng(seed | 1);
    let mut pad = 0u64;
    let mut txn = 0u64;
    let rk = base_key("anchor-elsewhere");
    for _ in 0..80 {
        let Some(l) = c.leader() else {
            c.sim.run_for(Duration::from_secs(1));
            continue;
        };
        let pk = pks[rng.below(3) as usize];
        let age = 1 + rng.below(50);
        let cond = match rng.below(4) {
            0 => not_exists(),
            1 => Some(ConditionExpression::AttributeExists("pk".to_owned())),
            _ => None,
        };
        match rng.below(14) {
            0..=2 => {
                let _ = c.nodes[l].propose_kind_eval(
                    schema(),
                    s(pk),
                    None,
                    KindEvalOp::Put(item(pk, age)),
                    cond,
                    false,
                );
                c.sim.run_for(Duration::from_millis(300));
            }
            3 | 4 => {
                let _ = c.nodes[l].propose_kind_eval(
                    schema(),
                    s(pk),
                    None,
                    KindEvalOp::Update {
                        key_item: item(pk, 0),
                        actions: vec![UpdateAction::Set(
                            vec![PathSegment::Field("age".to_owned())],
                            animus_item::UpdateExpr::value(n(age)),
                        )],
                    },
                    cond,
                    false,
                );
                c.sim.run_for(Duration::from_millis(300));
            }
            5 | 6 => {
                let _ = c.nodes[l].propose_kind_eval(
                    schema(),
                    s(pk),
                    None,
                    KindEvalOp::Delete,
                    cond,
                    false,
                );
                c.sim.run_for(Duration::from_millis(300));
            }
            7 => {
                let other = pks[rng.below(3) as usize];
                let _ = c.nodes[l].propose_kind_eval_batch(vec![
                    entry(pk, KindEvalOp::Put(item(pk, age)), cond),
                    entry(other, KindEvalOp::Delete, not_exists()),
                ]);
                c.sim.run_for(Duration::from_millis(300));
            }
            8 => {
                // A foreign transaction's intent on one key, resolved later
                // by whichever step comes next (or left blocking).
                txn += 1;
                let k = keys[rng.below(3) as usize].clone();
                let id = txn_id(txn);
                let w = vec![animus_cp_data::TxnWrite::plain(
                    k.clone(),
                    Some(encode_stored_item(&item(pk, age))),
                )];
                let (n_, id2, rk2) = (c.nodes[l].clone(), id.clone(), rk.clone());
                let staged = super::txn_stage_replay_stability::drive(
                    &mut c.sim,
                    c.nodes[l].env(),
                    SETTLE,
                    async move {
                        n_.txn_stage_participant(id2, rk2, "t".into(), w, Vec::new())
                            .await
                    },
                )
                .flatten();
                if let Some((ts, _)) = staged {
                    if rng.below(2) == 0 {
                        let outcome = TxnOutcome::Committed { commit_ts: ts };
                        c.resolve(l, &id, &rk, std::slice::from_ref(&k), outcome);
                    } else {
                        // Leave it unresolved for a while; the eval below is
                        // blocked, then a later round resolves it.
                        let _ = c.nodes[l].propose_kind_eval(
                            schema(),
                            s(pk),
                            None,
                            KindEvalOp::Put(item(pk, age)),
                            None,
                            false,
                        );
                        c.sim.run_for(Duration::from_millis(300));
                        c.resolve(l, &id, &rk, std::slice::from_ref(&k), TxnOutcome::Aborted);
                    }
                }
            }
            9 => {
                let i = rng.below(3) as usize;
                if c.up[i] {
                    if c.up.iter().filter(|u| **u).count() > 2 {
                        c.sim.crash(nid(i as u64));
                        c.up[i] = false;
                    }
                } else {
                    c.sim.restart(nid(i as u64));
                    c.up[i] = true;
                }
                c.sim.run_for(Duration::from_secs(2));
            }
            10 | 11 => {
                let i = rng.below(3) as usize;
                if c.up[i] {
                    c.restart_fresh(i);
                }
            }
            12 => {
                for _ in 0..rng.below(5000) {
                    pad += 1;
                    let _ = c.nodes[l].put(format!("pad{pad:06}").into_bytes(), b"x".to_vec());
                }
                c.sim.run_for(Duration::from_secs(3));
            }
            _ => c.sim.run_for(Duration::from_millis(500)),
        }
    }
    for i in 0..3 {
        if !c.up[i] {
            c.sim.restart(nid(i as u64));
            c.up[i] = true;
        }
    }
    c.sim.run_for(Duration::from_secs(20));
    let refs: Vec<&Vec<u8>> = keys.iter().collect();
    c.assert_identical(&refs, "end of schedule");
}

#[test]
fn kind_eval_replay_corpus() {
    let seeds: Vec<u64> = if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        vec![s]
    } else {
        let k = std::env::var("ANIMUS_KINDEVAL_REPLAY_SEEDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(4)
            .max(1);
        (0..k).map(|i| SEED + 100 + i * 7919).collect()
    };
    for seed in seeds {
        run_schedule(seed);
    }
}
