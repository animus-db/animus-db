//! Seed-reproducible fault-injection corpus for the ADR 0073 Phase 2 (P2-A)
//! version commands: `ReportNodeVersion` / `FinalizeClusterVersion` mixed with
//! `UpsertMember` / `RemoveMember` / `RegisterNode`.
//!
//! Each seed generates a random command stream (biased by an oracle replica so
//! the interesting paths — era start, finalize, removal of a blocker — are
//! actually hit), then damages it the way a lossy proposer path would: drops,
//! duplicates and adjacent reorders. The damaged stream is the "committed
//! log"; it is applied to N replicas, one of them through
//! `mirror::apply_and_derive_mirror` whose writes are folded onto a plain
//! `Metadata` with `apply_key_write` (the data-only node's read path).
//!
//! Checked after every command: all replicas agree; the mirror replay equals
//! the replica (lossless incl. `node_versions`/`cluster_version`); a
//! non-`Applied` command changes nothing; `cluster_version` only ever rises,
//! by exactly one; a `Finalize` succeeds only if no registered node blocks it
//! (checked against the pre-state independently of `apply`, including the
//! member-status half, issue #1168); once finalized no
//! entry's `max` is below the version; `UpsertMember` never touches
//! `node_versions`; a removed node leaves no record and never blocks; the era
//! never turns off once on (sticky marker, even if every reporter is removed).
//!
//! Replay one seed with `ANIMUS_SEED=<seed>`; depth with `ANIMUS_CONTROL_SEEDS`.

use std::collections::BTreeMap;

use animus_control::mirror::{apply_and_derive_mirror, apply_key_write};
use animus_control::version::VersionRange;
use animus_control::{ApplyOutcome, MetaCommand, Metadata, NodeStatus};
use animus_env::{NodeId, nid};
use animus_test::corpus;

/// splitmix64: a tiny pure PRNG, so the corpus needs no `Env`.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

const NODES: u64 = 5;

fn register(n: u64, role: &str) -> MetaCommand {
    MetaCommand::RegisterNode {
        node: nid(n),
        addrs: animus_control::meta::NodeAddrs {
            internal: format!("127.0.0.1:{}", 9300 + n),
            client: format!("127.0.0.1:{}", 9000 + n),
            admin: format!("127.0.0.1:{}", 9500 + n),
            intra: format!("127.0.0.1:{}", 9600 + n),
            role: role.to_string(),
        },
        labels: BTreeMap::new(),
    }
}

fn gen_command(rng: &mut Rng, oracle: &Metadata) -> MetaCommand {
    let node = 1 + rng.below(NODES);
    match rng.below(100) {
        0..=17 => register(
            node,
            if rng.chance(50) {
                "control"
            } else {
                "combined"
            },
        ),
        18..=33 => MetaCommand::UpsertMember {
            node: nid(node),
            labels: BTreeMap::new(),
            status: if rng.chance(50) {
                NodeStatus::Active
            } else {
                NodeStatus::Down
            },
        },
        34..=45 => MetaCommand::RemoveMember { node: nid(node) },
        46..=77 => {
            // Mostly well-formed ranges around the current version; sometimes
            // inverted/zero/excluding.
            let cur = u64::from(oracle.cluster_version());
            let (min, max) = match rng.below(10) {
                0 => (3, 2),
                1 => (0, 1),
                2 => (cur + 1, cur + 2),
                _ => {
                    let min = 1 + rng.below(cur);
                    (min, cur + rng.below(3))
                }
            };
            MetaCommand::ReportNodeVersion {
                node: nid(node),
                range: VersionRange::new(
                    u32::try_from(min).unwrap_or(u32::MAX),
                    u32::try_from(max).unwrap_or(u32::MAX),
                ),
                build: format!("b{}", rng.below(3)),
            }
        }
        _ => {
            let cur = oracle.cluster_version();
            let expected = if rng.chance(85) { cur } else { cur + 1 };
            let target = if rng.chance(90) {
                expected + 1
            } else {
                expected + 2
            };
            MetaCommand::FinalizeClusterVersion { expected, target }
        }
    }
}

/// The independent "no blocker" check, computed from the pre-state only.
fn finalize_has_no_blocker(pre: &Metadata, expected: u32, target: u32) -> bool {
    pre.versioning_active()
        && expected == pre.cluster_version()
        && expected.checked_add(1) == Some(target)
        // Decision 6 (issue #1168), spelled out independently of
        // `Member::finalize_block_reason`.
        && pre.members.values().all(|m| match m.status {
            NodeStatus::Active => true,
            NodeStatus::Joining => m.has_activated,
            NodeStatus::Down | NodeStatus::Leaving => false,
        })
        && pre.members.keys().chain(pre.node_addrs.keys()).all(|n| {
            pre.node_versions
                .get(n)
                .is_some_and(|v| v.range.contains(target))
        })
}

#[derive(Default)]
struct Coverage {
    era_started: usize,
    finalized: usize,
    finalize_blocked: usize,
    removed_with_version: usize,
}

fn run_seed(seed: u64, cov: &mut Coverage) {
    let mut rng = Rng(seed);
    // Generate against an oracle so the stream reaches deep states.
    let mut oracle = Metadata::default();
    let mut stream = Vec::new();
    for _ in 0..120 {
        let cmd = gen_command(&mut rng, &oracle);
        oracle.apply(&cmd);
        stream.push(cmd);
    }
    // Damage: drop / duplicate / adjacent-reorder.
    let mut log = Vec::new();
    for cmd in stream {
        if rng.chance(8) {
            continue;
        }
        if rng.chance(10) {
            log.push(cmd.clone());
        }
        log.push(cmd);
    }
    for i in 1..log.len() {
        if rng.chance(8) {
            log.swap(i - 1, i);
        }
    }

    const REPLICAS: usize = 3;
    let mut replicas: Vec<Metadata> = (0..REPLICAS).map(|_| Metadata::default()).collect();
    let mut mirrored = Metadata::default();
    let mut mirror_view = Metadata::default();

    for (step, cmd) in log.iter().enumerate() {
        let ctx = format!("seed={seed:#x} step={step} cmd={cmd:?}");
        let pre = replicas[0].clone();

        let mut outcomes = Vec::new();
        for r in &mut replicas {
            outcomes.push(r.apply(cmd));
        }
        assert!(
            outcomes.windows(2).all(|w| w[0] == w[1]),
            "{ctx}: outcomes diverged"
        );
        assert!(
            replicas.windows(2).all(|w| w[0] == w[1]),
            "{ctx}: replicas diverged"
        );
        let outcome = outcomes[0];
        let post = &replicas[0];

        // Mirror path: same outcome, writes fold to the same state.
        let (m_outcome, writes) = apply_and_derive_mirror(&mut mirrored, cmd);
        assert_eq!(m_outcome, outcome, "{ctx}: mirror path outcome");
        assert_eq!(&mirrored, post, "{ctx}: mirror path state");
        if outcome != ApplyOutcome::Applied {
            assert!(writes.is_empty(), "{ctx}: non-Applied produced writes");
            assert_eq!(post, &pre, "{ctx}: non-Applied changed state");
        }
        for w in &writes {
            apply_key_write(&mut mirror_view, w);
        }
        assert_eq!(&mirror_view, post, "{ctx}: mirror replay lost state");

        // Version invariants.
        assert!(
            post.cluster_version() >= pre.cluster_version(),
            "{ctx}: version fell"
        );
        assert!(
            post.cluster_version() - pre.cluster_version() <= 1,
            "{ctx}: version jumped"
        );
        assert!(
            !pre.versioning_active() || post.versioning_active(),
            "{ctx}: the era turned off (it is sticky)"
        );
        if !pre.versioning_active() && post.versioning_active() {
            cov.era_started += 1;
        }

        match cmd {
            MetaCommand::FinalizeClusterVersion { expected, target } => {
                let clean = finalize_has_no_blocker(&pre, *expected, *target);
                assert_eq!(
                    outcome == ApplyOutcome::Applied,
                    clean,
                    "{ctx}: finalize outcome disagrees with the independent blocker check"
                );
                if outcome == ApplyOutcome::Applied {
                    cov.finalized += 1;
                    for n in post.members.keys().chain(post.node_addrs.keys()) {
                        assert!(
                            post.node_versions[n].range.contains(post.cluster_version()),
                            "{ctx}: finalized past {n:?}'s range"
                        );
                    }
                } else if matches!(outcome, ApplyOutcome::Rejected(r) if r.starts_with("blocked")) {
                    cov.finalize_blocked += 1;
                }
            }
            MetaCommand::UpsertMember { .. } => {
                assert_eq!(
                    post.node_versions, pre.node_versions,
                    "{ctx}: UpsertMember touched node_versions"
                );
            }
            MetaCommand::RemoveMember { node } if outcome == ApplyOutcome::Applied => {
                if pre.node_versions.contains_key(node) {
                    cov.removed_with_version += 1;
                }
                assert!(
                    !post.node_versions.contains_key(node),
                    "{ctx}: removed node kept a record"
                );
                assert!(!post.members.contains_key(node) && !post.node_addrs.contains_key(node));
            }
            MetaCommand::ReportNodeVersion { node, .. } if outcome == ApplyOutcome::Applied => {
                assert!(
                    post.members.contains_key(node) || post.node_addrs.contains_key(node),
                    "{ctx}: recorded a version for an unregistered node"
                );
            }
            _ => {}
        }
        // Records only ever exist for registered nodes.
        for n in post.node_versions.keys() {
            assert!(
                post.members.contains_key(n) || post.node_addrs.contains_key(n),
                "{ctx}: orphan version record for {n:?}"
            );
        }
        // No record ever excludes the current cluster version.
        for (n, v) in &post.node_versions {
            assert!(
                v.range.contains(post.cluster_version()),
                "{ctx}: {n:?} range excludes the cluster version"
            );
        }
        // `required_version_set` is exactly members ∪ node_addrs.
        let want: std::collections::BTreeSet<NodeId> = post
            .members
            .keys()
            .chain(post.node_addrs.keys())
            .cloned()
            .collect();
        assert_eq!(post.required_version_set(), want, "{ctx}");
    }
}

fn seeds() -> Vec<u64> {
    if let Some(seed) = std::env::var("ANIMUS_SEED").ok().and_then(|v| {
        v.parse::<u64>()
            .ok()
            .or_else(|| u64::from_str_radix(v.trim_start_matches("0x"), 16).ok())
    }) {
        return vec![seed];
    }
    let k = corpus::seeds_from_env("ANIMUS_CONTROL_SEEDS");
    let mut out = Vec::new();
    corpus::for_each_seed("version_apply_corpus", 16 * k, |s| out.push(s));
    out
}

#[test]
fn version_commands_converge_and_hold_invariants_under_damaged_logs() {
    let mut cov = Coverage::default();
    let seeds = seeds();
    let replay = seeds.len() == 1 && std::env::var("ANIMUS_SEED").is_ok();
    for seed in &seeds {
        run_seed(*seed, &mut cov);
    }
    if !replay {
        assert!(cov.era_started > 0, "corpus never started an era");
        assert!(cov.finalized > 0, "corpus never finalized a version");
        assert!(
            cov.finalize_blocked > 0,
            "corpus never saw a blocked finalize"
        );
        assert!(
            cov.removed_with_version > 0,
            "corpus never removed a node with a version record"
        );
    }
}
