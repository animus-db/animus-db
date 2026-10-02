//! Issue #1132 for `SharedWal` / `SWL1` (the `raftkv.wal.shared` file):
//! mid-file corruption of durable history is a named error, a crash-torn tail
//! is not.
//!
//! Same design and same reasons as `wal_midfile_corruption.rs` (read its module
//! doc): a flush appends N tagged lines from several tablets and syncs once, so
//! "a valid line follows the bad one" is not proof of corruption under
//! `corrupt_on_crash`; the v2 writer appends a `!sync:<offset>` marker after each
//! successful sync and the decoder trusts only that. Every test writes through
//! the real `SharedWal` (concurrent `append_tagged` callers, real syncs) on a
//! `SimEnv` disk, prints its seed, and replays one seed via `ANIMUS_SEED=<seed>`.
//! Depth: `max(8, ANIMUS_CONTROL_SEEDS)` seeds per test.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::format::{self, FormatError, FormatTag};
use animus_control::persist::{PersistedState, WalRecord};
use animus_control::{MetaCommand, Metadata, SharedWal};
use animus_env::{Clock, Disk, EnvExt, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_tablet::TabletId;

const FILE: &str = "raftkv.wal.shared";
const TABLETS: [u64; 3] = [1, 2, 3];

type Wal = SharedWal<MetaCommand, Metadata>;
type Rec = WalRecord<MetaCommand, Metadata>;

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
    (0..depth).map(|i| 0x5113_0000 + i).collect()
}

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

fn hard(term: u64) -> Rec {
    WalRecord::Hard {
        term,
        voted_for: None,
    }
}

fn term_of(r: &Rec) -> u64 {
    match r {
        WalRecord::Hard { term, .. } => *term,
        other => panic!("unexpected record {other:?}"),
    }
}

/// Acked terms per tablet, in ack order (pushed only after `append_tagged`
/// returned `Ok`, i.e. after the round's real sync).
type Acked = Arc<Mutex<BTreeMap<u64, Vec<u64>>>>;

/// Start one writer task per tablet on `env`; each loops appending a round of
/// 1-3 records (`Hard{term}` with a per-tablet strictly increasing term) and
/// sleeping a little, forever (until the process is stopped).
fn spawn_writers(
    env: &SimEnv,
    wal: &Arc<Wal>,
    next_term: &Arc<Mutex<BTreeMap<u64, u64>>>,
    acked: &Acked,
    salt: u64,
) {
    for t in TABLETS {
        let (env, wal, next_term, acked) =
            (env.clone(), wal.clone(), next_term.clone(), acked.clone());
        env.clone().spawn_task(async move {
            let mut ch = Choices(salt ^ (t << 32));
            loop {
                let n = 1 + ch.below(3);
                let first = {
                    let mut g = next_term.lock().unwrap();
                    let e = g.entry(t).or_insert(1);
                    let first = *e;
                    *e += n;
                    first
                };
                let recs: Vec<Rec> = (first..first + n).map(hard).collect();
                if wal
                    .append_tagged(&env, FILE, TabletId(t), &recs)
                    .await
                    .is_ok()
                {
                    acked
                        .lock()
                        .unwrap()
                        .entry(t)
                        .or_default()
                        .extend(first..first + n);
                }
                env.sleep(Duration::from_millis(1 + ch.below(15))).await;
            }
        });
    }
}

fn run<T: Send + 'static>(
    sim: &mut Simulator,
    f: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    let out = Arc::new(Mutex::new(None));
    let o = out.clone();
    sim.env(nid(0)).spawn_task(async move {
        *o.lock().unwrap() = Some(f.await);
    });
    sim.run_for(Duration::from_millis(5));
    out.lock().unwrap().take().expect("task finished")
}

fn read_file(sim: &mut Simulator) -> Vec<u8> {
    let env = sim.env(nid(0));
    run(sim, async move { env.read(FILE).await.unwrap_or_default() })
}

fn write_file(sim: &mut Simulator, bytes: Vec<u8>) {
    let env = sim.env(nid(0));
    run(sim, async move {
        env.replace(FILE, &bytes).await.expect("replace")
    });
}

fn open(sim: &mut Simulator) -> std::io::Result<Arc<Wal>> {
    let env = sim.env(nid(0));
    run(sim, async move { Wal::open(&env, FILE).await })
}

/// Byte offset of every line (start, end-exclusive-of-newline, is_marker).
fn line_map(bytes: &[u8]) -> Vec<(usize, usize, bool)> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        let end = bytes[pos..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(bytes.len(), |r| pos + r);
        out.push((pos, end, bytes[pos..end].windows(6).any(|w| w == b"!sync:")));
        pos = end + 1;
    }
    out
}

fn decode(bytes: &[u8]) -> Result<Vec<(TabletId, Rec)>, FormatError> {
    PersistedState::<MetaCommand, Metadata>::decode_tagged(bytes)
}

/// Run the real writers for `ms` of virtual time, then drop the process
/// (clean stop: everything flushed so far is whole on disk).
fn real_history(sim: &mut Simulator, ms: u64, salt: u64) -> Acked {
    let wal = open(sim).expect("fresh open");
    let acked: Acked = Arc::default();
    spawn_writers(&sim.env(nid(0)), &wal, &Arc::default(), &acked, salt);
    sim.run_for(Duration::from_millis(ms));
    sim.stop(nid(0));
    acked
}

/// (a) and (c): rot one seed-chosen byte of a record line that has a durable
/// marker after it (case (c): the very first line of the file) and assert the
/// exact named error and offsets, both from the decoder and from `open`.
fn corrupt_before_marker(first_line_only: bool) {
    for seed in seeds() {
        eprintln!("shared_wal_midfile_corruption (first_line_only={first_line_only}): seed={seed}");
        let mut ch = Choices(seed);
        let mut sim = Simulator::new(seed);
        real_history(&mut sim, 150 + ch.below(150), seed);
        let clean = read_file(&mut sim);
        assert!(decode(&clean).is_ok(), "seed={seed}");

        let lines = line_map(&clean);
        let last_marker = *lines
            .iter()
            .rev()
            .find(|l| l.2)
            .unwrap_or_else(|| panic!("seed={seed}: the real writer must emit markers"));
        let records: Vec<_> = lines
            .iter()
            .filter(|l| !l.2 && l.0 < last_marker.0)
            .collect();
        assert!(records.len() >= 4, "seed={seed}: history too short");
        let victim = if first_line_only {
            *records[0]
        } else {
            *records[ch.below(records.len() as u64) as usize]
        };
        let mut bytes = clean.clone();
        let at = victim.0 + ch.below((victim.1 - victim.0) as u64) as usize;
        bytes[at] ^= 0xFF;

        let expect = FormatError::MidFileCorruption {
            format: "shared-wal",
            offset: victim.0 as u64,
            durable_to: last_marker.0 as u64,
        };
        assert_eq!(
            decode(&bytes).unwrap_err(),
            expect,
            "seed={seed}: flipped byte {at} of the line at {}",
            victim.0
        );

        // `SharedWal::open` surfaces it as InvalidData naming the offset,
        // never an empty or truncated recovery.
        write_file(&mut sim, bytes);
        let err = open(&mut sim).err().expect("open must refuse");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "seed={seed}");
        assert!(
            err.to_string()
                .contains(&format!("corrupt record at byte offset {}", victim.0)),
            "seed={seed}: {err}"
        );
    }
}

#[test]
fn a_flipped_byte_before_a_durable_marker_is_a_named_error() {
    corrupt_before_marker(false);
}

#[test]
fn a_flipped_first_line_is_a_named_error_not_zero_records() {
    corrupt_before_marker(true);
}

/// (b) Damage after the last durable marker is a tolerated tear: drop the final
/// marker (a crash before it became durable) and rot the final record; recovery
/// yields exactly the prior records and `open` repairs the file.
#[test]
fn damage_after_the_last_marker_recovers_exactly_the_prior_records() {
    for seed in seeds() {
        eprintln!("shared_wal_midfile_corruption (tail): seed={seed}");
        let mut ch = Choices(seed);
        let mut sim = Simulator::new(seed);
        real_history(&mut sim, 150 + ch.below(100), seed);
        let clean = read_file(&mut sim);
        let lines = line_map(&clean);
        let last_marker = *lines.iter().rev().find(|l| l.2).expect("marker");
        let tail_record = *lines
            .iter()
            .rev()
            .find(|l| !l.2 && l.0 < last_marker.0)
            .expect("record");

        // File as a crash before the final marker became durable leaves it,
        // with the final record's bytes garbled.
        let mut bytes = clean[..last_marker.0].to_vec();
        let at = tail_record.0 + ch.below((tail_record.1 - tail_record.0) as u64) as usize;
        bytes[at] ^= 0xFF;

        // (A stop can land after a round synced but before its marker, so
        // records may follow the last marker in `clean`; they are cut above.)
        let want: Vec<_> = {
            let upto_marker = decode(&clean[..last_marker.0]).unwrap();
            upto_marker[..upto_marker.len() - 1].to_vec()
        };
        let (got, valid_len) =
            PersistedState::<MetaCommand, Metadata>::decode_tagged_with_extent(&bytes)
                .unwrap_or_else(|e| panic!("seed={seed}: a torn tail is never an Err: {e}"));
        assert_eq!(got, want, "seed={seed}: exactly the prior records");
        assert_eq!(valid_len, tail_record.0, "seed={seed}");

        write_file(&mut sim, bytes);
        open(&mut sim).unwrap_or_else(|e| panic!("seed={seed}: open must tolerate it: {e}"));
        let repaired = read_file(&mut sim);
        assert_eq!(repaired.len(), tail_record.0, "seed={seed}: tail cut back");
        assert_eq!(decode(&repaired).unwrap(), want, "seed={seed}");
    }
}

/// (b) The property that rules out the naive resync rule: concurrent writers,
/// a crash at a seed-chosen moment with `torn_tail_on_crash` +
/// `corrupt_on_crash` armed and no `fsync_lie`, over three crash/recover
/// cycles. Recovery always succeeds (never the named error) and every record
/// acked before each crash is on disk, per tablet, in order.
#[test]
fn crash_with_torn_and_corrupt_tail_always_recovers_every_synced_record() {
    for seed in seeds() {
        eprintln!("shared_wal_midfile_corruption (crash property): seed={seed}");
        let mut ch = Choices(seed);
        let mut sim = Simulator::new(seed);
        let next_term: Arc<Mutex<BTreeMap<u64, u64>>> = Arc::default();
        let acked: Acked = Arc::default();

        for cycle in 0..3u64 {
            let wal = open(&mut sim).unwrap_or_else(|e| {
                panic!("seed={seed} cycle={cycle}: recovery refused a legitimate crash: {e}")
            });
            spawn_writers(&sim.env(nid(0)), &wal, &next_term, &acked, seed ^ cycle);
            sim.run_for(Duration::from_millis(20 + ch.below(120)));

            let mut disk = DiskConfig::default();
            disk.torn_tail_on_crash = true;
            disk.corrupt_on_crash = true;
            sim.set_disk_config_for(nid(0), disk);
            sim.crash(nid(0));
            sim.restart(nid(0));
            sim.stop(nid(0));
            sim.set_disk_config_for(nid(0), DiskConfig::default());

            let bytes = read_file(&mut sim);
            let decoded = decode(&bytes).unwrap_or_else(|e| {
                panic!("seed={seed} cycle={cycle}: crash left an undecodable file: {e}")
            });
            let mut on_disk: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
            for (t, r) in &decoded {
                on_disk.entry(t.0).or_default().push(term_of(r));
            }
            for (t, terms) in acked.lock().unwrap().iter() {
                let have = on_disk.get(t).cloned().unwrap_or_default();
                assert!(
                    have.len() >= terms.len() && have[..terms.len()] == terms[..],
                    "seed={seed} cycle={cycle} tablet {t}: lost acked records \
                     (acked {terms:?}, on disk {have:?})"
                );
            }
            // Terms the next cycle hands out must not collide with whatever
            // un-acked survivors the crash left behind.
            let mut g = next_term.lock().unwrap();
            for (t, terms) in &on_disk {
                let e = g.entry(*t).or_insert(1);
                *e = (*e).max(terms.iter().max().copied().unwrap_or(0) + 1);
            }
            drop(g);
            // Anything on disk that was not acked still counts as "acked" for
            // the next cycle's prefix check (it is now durable history).
            let mut a = acked.lock().unwrap();
            for (t, terms) in on_disk {
                a.insert(t, terms);
            }
        }
    }
}

/// (d) A v1 file reopened by this build keeps working: it opens, new rounds are
/// v2 lines + markers appended after it (version is per line), the mixed file
/// decodes whole, and a marker now protects the v1 prefix.
#[test]
fn a_v1_shared_wal_reopened_by_this_build_keeps_working_and_gains_markers() {
    const V1: FormatTag = FormatTag {
        magic: *b"SWL1",
        version: 1,
        name: "shared-wal",
    };
    for seed in seeds() {
        eprintln!("shared_wal_midfile_corruption (v1 reopen): seed={seed}");
        let mut ch = Choices(seed);
        let mut sim = Simulator::new(seed);
        real_history(&mut sim, 100 + ch.below(100), seed);
        let v2 = read_file(&mut sim);
        let records = decode(&v2).unwrap();
        assert!(records.len() >= 4, "seed={seed}");

        // What a v1 writer left: v1-tagged lines, no markers.
        let v1: Vec<u8> = records
            .iter()
            .flat_map(|(t, r)| {
                #[derive(serde::Serialize)]
                struct Line<'a> {
                    tablet: TabletId,
                    record: &'a Rec,
                }
                format::encode_line(
                    &V1,
                    &serde_json::to_vec(&Line {
                        tablet: *t,
                        record: r,
                    })
                    .unwrap(),
                )
            })
            .collect();
        write_file(&mut sim, v1.clone());

        // Reopen: v1 history intact; append more through the real writer.
        let wal = open(&mut sim).unwrap_or_else(|e| panic!("seed={seed}: v1 must open: {e}"));
        let env = sim.env(nid(0));
        let w2 = wal.clone();
        let appended = run(&mut sim, async move {
            for i in 0..3u64 {
                w2.append_tagged(&env, FILE, TabletId(9), &[hard(9000 + i)])
                    .await
                    .unwrap();
            }
            3usize
        });
        assert_eq!(appended, 3);
        sim.stop(nid(0));

        let mixed = read_file(&mut sim);
        assert_eq!(
            &mixed[..v1.len()],
            &v1[..],
            "seed={seed}: v1 prefix untouched"
        );
        let all = decode(&mixed).unwrap_or_else(|e| panic!("seed={seed}: mixed decodes: {e}"));
        assert_eq!(all.len(), records.len() + 3, "seed={seed}");
        let marker = *line_map(&mixed).iter().rev().find(|l| l.2).expect("marker");

        // The v1 prefix is now protected by the new marker.
        let mut rotted = mixed.clone();
        rotted[10 + ch.below(8) as usize] ^= 0xFF;
        assert_eq!(
            decode(&rotted).unwrap_err(),
            FormatError::MidFileCorruption {
                format: "shared-wal",
                offset: 0,
                durable_to: marker.0 as u64,
            },
            "seed={seed}"
        );
    }
}
