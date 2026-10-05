# Agents must redirect build, test and soak output to files

**Context (R-01 (a) soak harness).** The first agent on the soak branch ran
out of context before committing: it let full `cargo` build logs, clippy
output and multi-minute `--nocapture` soak logs stream into its own context,
and a long-running harness prints a line per epoch, per sample and per trend
series. The work was lost to context, not to difficulty.

**Rule.** For any command whose output can exceed a screen (cargo build/test/
clippy, soak and chaos runs, corpora), redirect to a file (`> log 2>&1`) and
inspect with `tail -n 40`, `grep -m 20` or `wc -l`; read source in
`sed -n 'A,Bp'` chunks of at most ~200 lines. Start multi-minute runs detached
(`setsid nohup ... &`) and poll the log, never block a tool call on them: a
foreground wait both stalls the session and risks the tool's own timeout.

**Related soak lessons.**
- Do not `pkill -f <name>` with a pattern that also occurs in your own shell
  command line (`animusd`): it kills the shell. Kill by pid.
- A soak that keeps its whole history grows without bound; cut it into epochs
  with disjoint key ranges and verify-then-drop each (docs/soak.md), and
  delete old keys so any remaining resource growth is a leak, not workload.
- Sizing trend-detector unit tests: assert the *magnitude* of the synthetic
  leak against the detector's relative tolerance first; three of the first
  tests failed because the leak was below tolerance, not because the detector
  was wrong.
- Shared build hosts run out of disk (several `target` dirs of 10G+): check
  `df` before a long run and delete `incremental/` first.
