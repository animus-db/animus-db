# Golden-fixture tests: version from the file name, panic on an unknown one

A decode test that iterates `tests/fixtures/formats/<fmt>/` but compares
every file to one expected value, or opens it with the *current* format
constant, silently stops testing old versions the day `v2` lands: the
retained `v1.bin` either fails against the wrong expectation or is decoded
as if it were current. A test that skips (or only weakly checks) a file with
no known expectation lets a newly added `v2.bin` pass unverified.

Rule (ADR 0073 Phase 1, P1-B): derive the version from the `v<N>` file name,
`match` it to a per-version expected value, and `panic!` on any version with
no arm, so adding a fixture fails loudly until its expectation is written.
Also assert a fixture for the current version exists. Prove the panic once by
temporarily dropping a copy named `v99.bin` into the directory (never
committed; `scripts/check-format-fixtures.sh` only guards edits/deletes of
existing files).
