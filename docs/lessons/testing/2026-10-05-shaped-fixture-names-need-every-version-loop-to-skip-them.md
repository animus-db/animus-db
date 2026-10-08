# A `vN-<shape>` fixture breaks every "iterate all vN files" loop that parses the stem

Adding an additive variant inside an existing format version needs a new fixture file
(the old one is never edited), conventionally `vN-<shape>.ext`. Each crate's loader
parsed the whole stem as a number (`v1-versioned` fails `parse`, a hard panic), and
the cp-data loader walks the directory for `raftkv-wire`/`raftkv-wal` too. Make the
per-version loops skip stems containing `-`, read shaped files in their own test with a
hand-written expected value, and add the old-input test (the existing unshaped fixtures
still decode unchanged with no stamp, and `required_gate` of every old frame is `Base`).
Also: generating the fixture through the `#[ignore]`d generators is the safe path, and
rerunning an existing generator is refused by name, which is the guard working.
