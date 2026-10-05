# `decompress_size_prepended` allocates the untrusted size prefix

Found by libFuzzer (R-01): `lz4_flex::decompress_size_prepended` does
`vec![0; declared]` from the 4-byte prefix before decoding a byte, so a
CRC-valid corrupt/hostile SSTable block declaring ~3.8 GB aborts the process
(`handle_alloc_error`, not a catchable error). A CRC only proves the bytes are
what was written, not that the writer was sane.

Rule: never use a size-prepended decompress API on durable/untrusted input.
Read the prefix yourself, bound it, then call `lz4_flex::decompress(data, n)`.

Choosing the bound: the LSM layer has no cap on a single record (u32 length
prefixes), so there is no fixed "largest legal block" to compare against and
any constant would risk rejecting a legitimately written block (ADR 0073).
Bound by the input instead: LZ4 cannot expand more than 255x, so
`declared <= 255 * compressed_len` rejects only impossible blocks and keeps
the allocation proportional to the real on-disk length. Test both sides: the
fuzzer's crash block, and a max-ratio legitimately-written block.

Same class, not fixed here: `animusd::import::gunzip_bytes` reads a gzip
stream with `read_to_string` and no output cap (a gzip bomb in an S3 import
object). Not a durable-read path; tracked separately.
