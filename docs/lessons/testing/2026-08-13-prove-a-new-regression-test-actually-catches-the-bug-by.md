# Prove a new regression test actually catches the bug by running it against the pre-fix code, not just against the fix.

**Prove a new regression test actually catches the bug by running it
against the pre-fix code, not just against the fix.** A test that passes
post-fix is consistent with "the test works" *and* with "the test asserts
the wrong thing and would pass either way" — the two are indistinguishable
from a single green run. The cheap check: `git stash push -- <fixed
file(s)>`, re-run the new test, confirm it fails with the expected
symptom, `git stash pop`. For the ADR 0041 drop-table-cascade fix
(`ClientCtx::drop_table` in `animusd/src/lib.rs`), this caught nothing
wrong — but it's the difference between "I wrote an assertion" and "I
verified the assertion is load-bearing," and it costs one extra
`cargo test` invocation. Worth doing for any fix landing with exactly one
new regression test, especially when the bug is an *omission* (a cascade
step that never ran) rather than a wrong-value computation, since an
omission bug is the shape most likely to also be missing from a
carelessly-written test.
