# A fuzz target for a checksummed format must re-stamp the checksum, or it fuzzes only the checksum

**Context.** R-01 (c) added cargo-fuzz targets for every durable-format decoder (`fuzz/`). The
control/shared/RaftKV WALs are `<crc32 hex>:<magic><ver><payload>` lines, the LSM WAL is
`len | crc32 | payload` frames, and the SSTable block index ends in a CRC32.

**Lessons.**
- libFuzzer cannot solve a CRC. Fed a mutated line, the decoder rejects it at the checksum and
  the payload decoder (the code that actually parses attacker-shaped bytes) never runs, so
  coverage plateaus at the framing layer while the target looks "busy". Give the target a
  fix-up pass (`fix_line_crcs`, `fix_tail_crc_le`, `wal_file_with_frame` in `fuzz/src/targets.rs`)
  and run the decoder on both the raw and the re-stamped bytes, so the checksum path *and* the
  payload path are fuzzed.
- Expose an inner decoder directly (the bare WAL record, a CRC-stripped SSTable block) when the
  framing cannot be re-stamped cheaply, and derive seeds for it from the golden fixtures.
- A private parser (the expression grammars in `animus-dynamo`) is only reachable through a
  JSON request, so a raw-bytes target wastes its budget on JSON syntax. Add a structure-aware
  target that embeds the fuzzed strings (JSON-escaped) into valid request shapes.
- Keep a stable-toolchain deterministic smoke (seeds + `SplitMix64` mutations, fixed seed) next
  to the libFuzzer targets: it runs everywhere, replays from a printed hex input, and is the
  per-push guard when nightly cargo-fuzz is unavailable.
- First real find, inside 60 s of libFuzzer: an LZ4 SSTable block's 4-byte size prefix is passed
  to `lz4_flex::decompress_size_prepended`, which allocates that many bytes up front (a
  CRC-valid hostile block asks for ~4 GiB). It is the same class as the "untrusted length-prefix
  pre-sizing a `Vec`" entry in `docs/engineering-lessons.md`: every length a decoder takes from
  the bytes needs a cap, including the one a *dependency* reads for you. Run libFuzzer with
  `-rss_limit_mb=2048` so a huge `calloc` is reported instead of silently succeeding on an
  overcommitting kernel.
