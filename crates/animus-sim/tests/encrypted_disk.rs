//! Direct unit tests for `animus_env::{EncryptedDisk, EncryptedEnv,
//! EncryptionKey, verify_or_init_marker}` (ADR 0069, S-03 PR 1) over
//! `SimEnv` — the crash/fault properties live in
//! `animus-storage/tests/lsm_crash_encrypted.rs`; this file pins the
//! wrapper's own `Disk` contract in isolation.

use animus_env::{Disk, EncryptedEnv, EncryptionKey, Rng, nid, verify_or_init_marker};
use animus_sim::{SimEnv, Simulator};
use futures::executor::block_on;

fn key(byte: u8) -> EncryptionKey {
    EncryptionKey::from_bytes([byte; 32])
}

fn encrypted(sim: &Simulator, k: EncryptionKey) -> EncryptedEnv<SimEnv> {
    block_on(EncryptedEnv::open(sim.env(nid(0)), k)).expect("open")
}

#[test]
fn append_read_round_trips() {
    let sim = Simulator::new(1);
    let e = encrypted(&sim, key(1));
    block_on(async {
        e.append("f", b"hello ").await.unwrap();
        e.append("f", b"world").await.unwrap();
        e.sync("f").await.unwrap();
        assert_eq!(e.read("f").await.unwrap(), b"hello world");
    });
}

#[test]
fn read_at_slices_across_frame_boundaries() {
    let sim = Simulator::new(2);
    let e = encrypted(&sim, key(2));
    block_on(async {
        e.append("f", b"0123456789").await.unwrap(); // frame 0
        e.append("f", b"abcdefghij").await.unwrap(); // frame 1
        e.sync("f").await.unwrap();
        assert_eq!(e.read_at("f", 0, 5).await.unwrap(), b"01234");
        assert_eq!(e.read_at("f", 8, 5).await.unwrap(), b"89abc");
        assert_eq!(e.read_at("f", 15, 5).await.unwrap(), b"fghij");
        assert_eq!(e.read_at("f", 18, 10).await.unwrap(), b"ij");
        assert_eq!(e.read_at("f", 100, 5).await.unwrap(), Vec::<u8>::new());
    });
}

#[test]
fn size_tracks_plaintext_length() {
    let sim = Simulator::new(3);
    let e = encrypted(&sim, key(3));
    block_on(async {
        assert_eq!(e.size("nope").await.unwrap(), 0);
        e.append("f", b"12345").await.unwrap();
        assert_eq!(e.size("f").await.unwrap(), 5);
        e.append("f", b"67").await.unwrap();
        assert_eq!(e.size("f").await.unwrap(), 7);
    });
}

#[test]
fn replace_overwrites_and_reads_back() {
    let sim = Simulator::new(4);
    let e = encrypted(&sim, key(4));
    block_on(async {
        e.append("f", b"old-old-old").await.unwrap();
        e.replace("f", b"new").await.unwrap();
        assert_eq!(e.read("f").await.unwrap(), b"new");
        assert_eq!(e.size("f").await.unwrap(), 3);
    });
}

#[test]
fn remove_then_list_reflects_it() {
    let sim = Simulator::new(5);
    let e = encrypted(&sim, key(5));
    block_on(async {
        e.append("a", b"1").await.unwrap();
        e.append("b", b"2").await.unwrap();
        e.remove("a").await.unwrap();
        assert_eq!(e.list().await.unwrap(), vec!["b".to_string()]);
        assert_eq!(e.read("a").await.unwrap(), Vec::<u8>::new());
        assert_eq!(e.size("a").await.unwrap(), 0);
    });
}

#[test]
fn list_never_shows_the_marker_file() {
    let sim = Simulator::new(6);
    let e = encrypted(&sim, key(6));
    block_on(async {
        e.append("visible", b"x").await.unwrap();
    });
    let listed = block_on(e.list()).unwrap();
    assert_eq!(listed, vec!["visible".to_string()]);

    // The marker really is on the raw disk underneath.
    let raw = sim.env(nid(0));
    let raw_listed = block_on(Disk::list(&raw)).unwrap();
    assert!(raw_listed.iter().any(|n| n == animus_env::MARKER_FILE));
}

#[test]
fn link_shares_bytes_and_dst_reads_like_src() {
    let sim = Simulator::new(7);
    let e = encrypted(&sim, key(7));
    block_on(async {
        e.append("src", b"payload").await.unwrap();
        e.link("src", "dst").await.unwrap();
        assert_eq!(e.read("dst").await.unwrap(), b"payload");
        // A further append to src must not affect the already-linked dst
        // (a hard link is a snapshot of src's bytes *at link time*).
        e.append("src", b"-more").await.unwrap();
        assert_eq!(e.read("dst").await.unwrap(), b"payload");
        assert_eq!(e.read("src").await.unwrap(), b"payload-more");
    });
}

/// The frame index is rebuilt from a fresh scan on a brand-new
/// `EncryptedEnv` handle over the *same* underlying disk (simulating a
/// process restart) — not carried over via any shared in-process cache.
#[test]
fn frame_index_rebuilds_after_reopen() {
    let sim = Simulator::new(8);
    {
        let e = encrypted(&sim, key(8));
        block_on(async {
            for i in 0..20u32 {
                e.append("f", format!("chunk-{i:02}-").as_bytes())
                    .await
                    .unwrap();
            }
            e.sync("f").await.unwrap();
        });
    }
    // Fresh EncryptedEnv, fresh cache.
    let e2 = encrypted(&sim, key(8));
    block_on(async {
        let mut expected = String::new();
        for i in 0..20u32 {
            expected.push_str(&format!("chunk-{i:02}-"));
        }
        assert_eq!(e2.read("f").await.unwrap(), expected.as_bytes());
        assert_eq!(e2.size("f").await.unwrap(), expected.len() as u64);
        // Random-access read after a rebuild must also be correct, not
        // just the full linear read.
        assert_eq!(&e2.read_at("f", 9, 9).await.unwrap(), b"chunk-01-");
    });
}

/// A wrong key against an already-encrypted, marker-bearing directory is
/// refused at `EncryptedEnv::open` with the documented exact text.
#[test]
fn wrong_key_is_refused_with_exact_text() {
    let sim = Simulator::new(9);
    {
        let e = encrypted(&sim, key(9));
        block_on(async { e.append("f", b"x").await.unwrap() });
    }
    let err = match block_on(EncryptedEnv::open(sim.env(nid(0)), key(200))) {
        Ok(_) => panic!("wrong key must be refused"),
        Err(e) => e,
    };
    assert_eq!(
        err.to_string(),
        "--encryption-key does not match the key this data directory was encrypted with — \
         refusing to start. Use the original key, or point at a fresh data directory."
    );
}

/// No key at all against an already-encrypted directory is refused too.
#[test]
fn missing_key_is_refused_with_exact_text() {
    let sim = Simulator::new(10);
    {
        let e = encrypted(&sim, key(10));
        block_on(async { e.append("f", b"x").await.unwrap() });
    }
    let raw = sim.env(nid(0));
    let err = block_on(verify_or_init_marker(&raw, &raw, None)).unwrap_err();
    assert_eq!(
        err.to_string(),
        format!(
            "data directory is encrypted (found {}) but no --encryption-key was given — \
             refusing to start. Pass --encryption-key PATH with the same key this directory \
             was created with, or point at a fresh data directory.",
            animus_env::MARKER_FILE
        )
    );
}

/// A key against an existing *plaintext* directory (files present, no
/// marker) is refused too — the reverse mismatch.
#[test]
fn key_against_existing_plaintext_directory_is_refused() {
    let sim = Simulator::new(11);
    let raw = sim.env(nid(0));
    block_on(async {
        raw.append("plain-file", b"already here, unencrypted")
            .await
            .unwrap();
    });
    let err = match block_on(EncryptedEnv::open(sim.env(nid(0)), key(11))) {
        Ok(_) => panic!("key against a plaintext directory must be refused"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("already holds unencrypted files"),
        "unexpected message: {err}"
    );
}

/// A single tampered byte anywhere in an encrypted file's ciphertext or tag
/// makes that frame fail to authenticate.
#[test]
fn tampered_byte_fails_authentication() {
    let sim = Simulator::new(12);
    {
        let e = encrypted(&sim, key(12));
        block_on(async { e.append("f", b"authenticate-me").await.unwrap() });
    }
    let raw = sim.env(nid(0));
    let mut bytes = block_on(raw.read("f")).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF; // flip the last byte (inside the AEAD tag)
    block_on(raw.replace("f", &bytes)).unwrap();

    // Reopening with the same (correct) key must not resurrect the
    // tampered value as if it were valid data — either the read comes back
    // empty (torn-tail recovery, since this is now the only frame and the
    // whole thing fails to parse) or an explicit corruption error, but
    // never the tampered plaintext.
    let e2 = encrypted(&sim, key(12));
    if let Ok(v) = block_on(e2.read("f")) {
        assert!(
            v.is_empty(),
            "tampered single frame must not yield data, got {v:?}"
        );
    }
}

/// Nonce uniqueness: two `append`s to the same file, and a `replace`
/// afterward, must never reuse a `(salt, counter)` pair — verified
/// indirectly (this module has no way to inspect nonces directly) by
/// confirming every produced ciphertext is bit-for-bit distinct even for
/// identical plaintext, which XChaCha20-Poly1305 guarantees only when the
/// nonce differs.
#[test]
fn nonce_uniqueness_across_appends_and_replace() {
    let sim = Simulator::new(13);
    let raw = sim.env(nid(0));
    let e = encrypted(&sim, key(13));
    block_on(async {
        e.append("f", b"same-plaintext--").await.unwrap();
        e.append("f", b"same-plaintext--").await.unwrap();
        e.append("f", b"same-plaintext--").await.unwrap();
    });
    let after_appends = block_on(raw.read("f")).unwrap();
    // Three frames of identical plaintext must not produce identical
    // ciphertext bytes (would indicate nonce reuse).
    let frame_len = 4 + b"same-plaintext--".len() + 16; // len-prefix + ct + tag
    let header_len = 21;
    let f0 = &after_appends[header_len..header_len + frame_len];
    let f1 = &after_appends[header_len + frame_len..header_len + 2 * frame_len];
    let f2 = &after_appends[header_len + 2 * frame_len..header_len + 3 * frame_len];
    assert_ne!(f0, f1, "frame 0 and frame 1 ciphertext collided");
    assert_ne!(f1, f2, "frame 1 and frame 2 ciphertext collided");
    assert_ne!(f0, f2, "frame 0 and frame 2 ciphertext collided");

    // `replace` mints a fresh salt, so even identical plaintext content
    // produces a different header (salt) than what append had.
    block_on(e.replace("f", b"same-plaintext--")).unwrap();
    let after_replace = block_on(raw.read("f")).unwrap();
    let salt_before = &after_appends[5..21];
    let salt_after = &after_replace[5..21];
    assert_ne!(
        salt_before, salt_after,
        "replace must mint a fresh per-generation salt"
    );
}

/// The salt really is drawn from the env's seeded `Rng` (not, say, always
/// zero) — two different seeds produce two different salts for the very
/// first frame of a file, so the nonce space genuinely varies per node/run.
#[test]
fn salt_is_seed_derived_and_varies() {
    let sim_a = Simulator::new(100);
    let sim_b = Simulator::new(101);
    let ea = encrypted(&sim_a, key(50));
    let eb = encrypted(&sim_b, key(50));
    block_on(async {
        ea.append("f", b"x").await.unwrap();
        eb.append("f", b"x").await.unwrap();
    });
    let ra = sim_a.env(nid(0));
    let rb = sim_b.env(nid(0));
    let bytes_a = block_on(ra.read("f")).unwrap();
    let bytes_b = block_on(rb.read("f")).unwrap();
    assert_ne!(
        &bytes_a[5..21],
        &bytes_b[5..21],
        "salts for two different seeds must differ"
    );
    // Sanity: env RNG is exercised, not bypassed.
    let _ = Rng::next_u64(&sim_a.env(nid(1)));
}
