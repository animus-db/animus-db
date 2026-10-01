//! Issue #1132: the control-plane Raft WAL (`CWL1`) must tell a crash-torn
//! tail from corruption of already-durable history.
//!
//! Before: a CRC failure on *any* line stopped decoding and silently returned
//! the earlier records, so a rotted first line decoded to zero records and
//! dropped acked term/vote/log history. A plain "a valid line follows the bad
//! one" proof is unsound here, because a persist round appends N lines and
//! syncs once, and `corrupt_on_crash` flips a byte anywhere in the kept part
//! of that un-synced region (measured: 72 of 300 crash seeds with a correct
//! writer). The fix is the version-2 sync marker (`format::decode_lines_extent`
//! documents the algorithm): the real writer appends `!sync:<offset>` after
//! every successful fsync, and only a bad line that starts before a valid
//! marker is mid-file corruption.
//!
//! Every test writes through the **real writer path** (`RaftNode`'s
//! `persist_wal`, with real syncs, on a `SimEnv` disk), prints its seed in
//! every assertion message, and replays one seed via `ANIMUS_SEED=<seed>`.
//! Depth: `max(8, ANIMUS_CONTROL_SEEDS)` seeds per test.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::format::{self, FormatError, FormatTag};
use animus_control::persist::PersistedState;
use animus_control::{MetaCommand, NodeStatus, RaftNode};
use animus_env::{Disk, EnvExt, NodeId, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;

const WAL: &str = "raft.wal";
const ME: u64 = 0;

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        return vec![s];
    }
    let depth = std::env::var("ANIMUS_CONTROL_SEEDS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1)
        .max(8);
    (0..depth).map(|i| 0x1132_0000 + i).collect()
}

/// A small deterministic stream of choices derived from the seed (the sim's
/// own RNG is not exposed to tests; this keeps every choice replayable).
struct Choices(u64);
impl Choices {
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
}

fn upsert(i: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(100 + i),
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

fn start(sim: &Simulator, engine: &MemoryEngine) -> RaftNode<SimEnv> {
    RaftNode::start(sim.env(nid(ME)), vec![nid(ME)], engine.clone())
}

fn members(node: &RaftNode<SimEnv>) -> BTreeSet<NodeId> {
    node.metadata()
        .members
        .keys()
        .filter(|n| **n != nid(ME))
        .cloned()
        .collect()
}

fn read_wal(sim: &mut Simulator) -> Vec<u8> {
    let out = Arc::new(Mutex::new(None));
    let (env, o) = (sim.env(nid(ME)), out.clone());
    env.clone().spawn_task(async move {
        *o.lock().unwrap() = Some(env.read(WAL).await.unwrap_or_default());
    });
    sim.run_for(Duration::from_millis(1));
    out.lock().unwrap().take().expect("read task ran")
}

fn write_wal(sim: &mut Simulator, bytes: Vec<u8>) {
    let env = sim.env(nid(ME));
    env.clone().spawn_task(async move {
        env.replace(WAL, &bytes).await.expect("replace");
    });
    sim.run_for(Duration::from_millis(1));
}

/// Byte offset of every line start and whether it is a sync marker.
fn line_map(bytes: &[u8]) -> Vec<(usize, usize, bool)> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        let end = bytes[pos..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(bytes.len(), |r| pos + r);
        let is_marker = bytes[pos..end].windows(6).any(|w| w == b"!sync:");
        out.push((pos, end, is_marker));
        pos = end + 1;
    }
    out
}

/// Elect a single node and persist `n` proposals one at a time, a real
/// fsynced round (plus marker) each. Returns the node.
fn run_history(sim: &mut Simulator, engine: &MemoryEngine, n: u64, base: u64) -> RaftNode<SimEnv> {
    let node = start(sim, engine);
    sim.run_for(Duration::from_secs(2));
    assert!(node.is_leader(), "single node must self-elect");
    for i in 0..n {
        node.propose(upsert(base + i));
        sim.run_for(Duration::from_millis(200));
    }
    node
}

/// (a) and (c): flip one seed-chosen byte in a record line that has a valid
/// durable marker after it, then decode: the exact named error, with the
/// exact offsets, never a silent truncation. Case (c) forces the victim to be
/// the very first record line (the one the old rule dropped everything for).
fn corrupt_before_marker(first_line_only: bool) {
    for seed in seeds() {
        eprintln!("wal_midfile_corruption (first_line_only={first_line_only}): seed={seed}");
        let mut ch = Choices(seed);
        let mut sim = Simulator::new(seed);
        let engine = MemoryEngine::new();
        let n = 3 + ch.below(4);
        let node = run_history(&mut sim, &engine, n, 0);
        drop(node);
        sim.stop(nid(ME));
        let clean = read_wal(&mut sim);

        let lines = line_map(&clean);
        let last_marker =
            lines.iter().rev().find(|l| l.2).unwrap_or_else(|| {
                panic!("seed={seed}: the real writer must have emitted markers")
            });
        let records: Vec<_> = lines
            .iter()
            .filter(|l| !l.2 && l.0 < last_marker.0)
            .collect();
        assert!(records.len() >= 3, "seed={seed}: history too short");
        let victim = if first_line_only {
            records[0]
        } else {
            records[ch.below(records.len() as u64) as usize]
        };
        let mut bytes = clean.clone();
        let at = victim.0 + ch.below((victim.1 - victim.0) as u64) as usize;
        bytes[at] ^= 0xFF;

        assert_eq!(
            PersistedState::<MetaCommand, animus_control::Metadata>::decode(&bytes).unwrap_err(),
            FormatError::MidFileCorruption {
                format: "control-wal",
                offset: victim.0 as u64,
                durable_to: last_marker.0 as u64,
            },
            "seed={seed}: flipped byte {at} of the line at {}",
            victim.0
        );
        // And the pristine file still decodes (to something non-empty).
        assert!(
            !PersistedState::<MetaCommand, animus_control::Metadata>::decode(&clean)
                .expect("clean file decodes")
                .is_empty(),
            "seed={seed}"
        );

        // A node restarted on the corrupted disk refuses to recover as an
        // empty log: it halts (the existing loud-failure path), it does not
        // silently come up with truncated history.
        write_wal(&mut sim, bytes);
        let reborn = start(&sim, &engine);
        sim.run_for(Duration::from_secs(1));
        assert!(
            reborn.is_halted(),
            "seed={seed}: a node must halt on mid-file corruption, not recover truncated"
        );
    }
}

#[test]
fn a_flipped_byte_in_a_record_with_a_durable_marker_after_it_is_a_named_error() {
    corrupt_before_marker(false);
}

#[test]
fn a_flipped_first_record_line_is_a_named_error_not_zero_records() {
    corrupt_before_marker(true);
}

/// (b) Damage confined to the un-synced tail is a tolerated tear: flip a byte
/// in the final record's round (only lines after the last durable marker) and
/// recovery yields exactly the prior records.
#[test]
fn damage_to_the_unsynced_tail_recovers_exactly_the_prior_records() {
    for seed in seeds() {
        eprintln!("wal_midfile_corruption (tail): seed={seed}");
        let mut ch = Choices(seed);
        let mut sim = Simulator::new(seed);
        let engine = MemoryEngine::new();
        let node = run_history(&mut sim, &engine, 3 + ch.below(3), 0);
        drop(node);
        sim.stop(nid(ME));
        let clean = read_wal(&mut sim);
        let want = PersistedState::<MetaCommand, animus_control::Metadata>::decode(&clean)
            .expect("clean decodes");

        // Append a never-synced round: several complete lines, as a crash
        // would leave in the kept prefix, then rot one of them while the
        // later ones stay intact (the `corrupt_on_crash` shape).
        let mut bytes = clean.clone();
        let tail_start = bytes.len();
        let mut tail_lines = Vec::new();
        for i in 0..(2 + ch.below(3)) {
            let line = PersistedState::<MetaCommand, animus_control::Metadata>::encode_record(
                &animus_control::persist::WalRecord::Hard {
                    term: 1000 + i,
                    voted_for: None,
                },
            );
            tail_lines.push((bytes.len(), bytes.len() + line.len() - 1));
            bytes.extend(line);
        }
        let victim_idx = ch.below(tail_lines.len() as u64) as usize;
        let victim = tail_lines[victim_idx];
        let at = victim.0 + ch.below((victim.1 - victim.0) as u64) as usize;
        bytes[at] ^= 0xFF;

        let (got, valid_len) =
            PersistedState::<MetaCommand, animus_control::Metadata>::decode_with_extent(&bytes)
                .unwrap_or_else(|e| panic!("seed={seed}: a torn tail is never an Err: {e}"));
        // Exactly the synced records, plus any intact un-synced lines that
        // precede the rotted one (they are valid lines before the first bad
        // one; nothing after the bad line is trusted).
        let mut expect = want.clone();
        for i in 0..victim_idx {
            expect.push(animus_control::persist::WalRecord::Hard {
                term: 1000 + i as u64,
                voted_for: None,
            });
        }
        assert_eq!(got, expect, "seed={seed}: exactly the prior records");
        assert_eq!(
            valid_len, victim.0,
            "seed={seed}: clean prefix ends at the bad line"
        );
        assert!(valid_len >= tail_start, "seed={seed}");

        // Reopening repairs the tail on disk and the node recovers.
        write_wal(&mut sim, bytes);
        let reborn = start(&sim, &engine);
        sim.run_for(Duration::from_secs(2));
        assert!(!reborn.is_halted(), "seed={seed}");
        sim.stop(nid(ME));
        let repaired = read_wal(&mut sim);
        assert!(
            PersistedState::<MetaCommand, animus_control::Metadata>::decode(&repaired).is_ok(),
            "seed={seed}"
        );
    }
}

/// (b) The property that rules out the naive resync rule: with
/// `torn_tail_on_crash` + `corrupt_on_crash` armed and **no** `fsync_lie`, a
/// real crash at a seed-chosen moment ALWAYS recovers (never the named
/// error, never a halt), keeps every write that was acknowledged before the
/// crash, and — after more writes and a second crash — still does (the tail
/// repair on open is what makes the second recovery safe).
#[test]
fn crash_with_torn_and_corrupt_tail_always_recovers_every_synced_write() {
    for seed in seeds() {
        eprintln!("wal_midfile_corruption (crash property): seed={seed}");
        let mut ch = Choices(seed);
        let mut sim = Simulator::new(seed);
        let engine = MemoryEngine::new();
        let mut acked: BTreeSet<NodeId> = BTreeSet::new();
        let mut next = 0u64;

        let mut node = start(&sim, &engine);
        sim.run_for(Duration::from_secs(2));
        for cycle in 0..3 {
            for _ in 0..(2 + ch.below(5)) {
                // A burst of proposals so one persist round holds several
                // lines, then crash at a seed-chosen point inside it.
                for _ in 0..(1 + ch.below(4)) {
                    node.propose(upsert(next));
                    next += 1;
                }
                sim.run_for(Duration::from_millis(1 + ch.below(120)));
                acked.extend(members(&node));
            }
            // Crash exactly like a power loss that tears and garbles the
            // un-synced tail, then restart a fresh process on the same disk.
            let mut disk = DiskConfig::default();
            disk.torn_tail_on_crash = true;
            disk.corrupt_on_crash = true;
            sim.set_disk_config_for(nid(ME), disk);
            sim.crash(nid(ME));
            sim.restart(nid(ME));
            sim.stop(nid(ME));
            sim.set_disk_config_for(nid(ME), DiskConfig::default());

            let after = read_wal(&mut sim);
            PersistedState::<MetaCommand, animus_control::Metadata>::decode(&after).unwrap_or_else(
                |e| panic!("seed={seed} cycle={cycle}: crash left an undecodable WAL: {e}"),
            );

            node = start(&sim, &engine);
            sim.run_for(Duration::from_secs(2));
            assert!(
                !node.is_halted(),
                "seed={seed} cycle={cycle}: recovery halted after a legitimate crash"
            );
            let have = members(&node);
            assert!(
                acked.is_subset(&have),
                "seed={seed} cycle={cycle}: lost acked writes {:?}",
                acked.difference(&have).collect::<Vec<_>>()
            );
        }
    }
}

/// (d) A v1 file reopened by this build keeps working, appends are v2 lines +
/// markers (version is per line, so the mix decodes), and the new marker
/// protects the v1 prefix.
#[test]
fn a_v1_wal_reopened_by_this_build_keeps_working_and_gains_markers() {
    const V1: FormatTag = FormatTag {
        magic: *b"CWL1",
        version: 1,
        name: "control-wal",
    };
    for seed in seeds() {
        eprintln!("wal_midfile_corruption (v1 reopen): seed={seed}");
        let mut ch = Choices(seed);
        let n = 3 + ch.below(3);

        // Build a real history, then transcode the file to what a v1 writer
        // left behind: v1-tagged lines, no markers.
        let mut sim = Simulator::new(seed);
        let engine = MemoryEngine::new();
        let node = run_history(&mut sim, &engine, n, 0);
        let before = members(&node);
        drop(node);
        sim.stop(nid(ME));
        let v2 = read_wal(&mut sim);
        let records = PersistedState::<MetaCommand, animus_control::Metadata>::decode(&v2).unwrap();
        let v1_bytes: Vec<u8> = records
            .iter()
            .flat_map(|r| format::encode_line(&V1, &serde_json::to_vec(r).unwrap()))
            .collect();
        assert!(!v1_bytes.windows(6).any(|w| w == b"!sync:"));
        write_wal(&mut sim, v1_bytes.clone());

        // Reopen on the v1 file: recovers, accepts new writes.
        let node = start(&sim, &engine);
        sim.run_for(Duration::from_secs(2));
        assert!(!node.is_halted(), "seed={seed}: v1 file must open");
        assert_eq!(members(&node), before, "seed={seed}: v1 history intact");
        for i in 0..3 {
            node.propose(upsert(500 + i));
            sim.run_for(Duration::from_millis(200));
        }
        let after_writes = members(&node);
        assert_eq!(after_writes.len(), before.len() + 3, "seed={seed}");
        drop(node);
        sim.stop(nid(ME));

        // The file is now v1 prefix + v2 lines + a marker, and decodes whole.
        let mixed = read_wal(&mut sim);
        assert_eq!(&mixed[..v1_bytes.len()], &v1_bytes[..], "seed={seed}");
        assert!(mixed.windows(6).any(|w| w == b"!sync:"), "seed={seed}");
        let all = PersistedState::<MetaCommand, animus_control::Metadata>::decode(&mixed)
            .unwrap_or_else(|e| panic!("seed={seed}: mixed file must decode: {e}"));
        assert!(all.len() > records.len(), "seed={seed}");

        // ...and the marker now protects the v1 prefix: rot its first line.
        let marker = line_map(&mixed).into_iter().rev().find(|l| l.2).unwrap();
        let mut rotted = mixed.clone();
        rotted[10 + ch.below(8) as usize] ^= 0xFF;
        assert_eq!(
            PersistedState::<MetaCommand, animus_control::Metadata>::decode(&rotted).unwrap_err(),
            FormatError::MidFileCorruption {
                format: "control-wal",
                offset: 0,
                durable_to: marker.0 as u64,
            },
            "seed={seed}"
        );

        // A restart on the mixed file recovers every write.
        let node = start(&sim, &engine);
        sim.run_for(Duration::from_secs(2));
        assert!(!node.is_halted(), "seed={seed}");
        assert_eq!(members(&node), after_writes, "seed={seed}");
    }
}
