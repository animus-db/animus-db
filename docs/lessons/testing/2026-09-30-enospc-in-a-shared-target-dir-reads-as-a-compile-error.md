# A full disk in a shared sandbox reads as a bogus compile error

During P1-C a workspace build reported `could not compile animusd (test
"encryption_at_rest_segment_store_e2e") due to 1 previous error` with no
diagnostic, and a background task's output file was silently emptied. Cause:
the sandbox disk (shared by concurrent agent builds; `target/` alone was
25G, 6G of it `incremental/`) hit ENOSPC. The same test compiled fine once
space was freed, so it was not a code defect.

- An `error: could not compile` with "1 previous error" and no rustc
  diagnostic, or an empty tool-output file, means check `df -h /` before
  debugging the code.
- Reclaim space with `rm -rf target/debug/incremental` and by deleting the
  extensionless test executables under `target/debug/deps` (regenerated on
  demand); build with `CARGO_INCREMENTAL=0` (and `CARGO_PROFILE_DEV_DEBUG=0`
  when tight) so the next build does not refill it.
- Do not treat a green rerun as "flaky": confirm the cause (ENOSPC) first.
