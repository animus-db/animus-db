# Huge crate guides exhaust subagent context

**Subagents that edit files in a crate with a very large `CLAUDE.md` die of
autocompact thrash.** `crates/animusd/CLAUDE.md` is about 830 KB (other crate
guides about 220 KB). The harness may auto-inject a crate's guide when an agent
uses the Read/Edit/Write tools on a file under that crate, so a single edit can
pull hundreds of KB into the context, and a few edits in a row exhaust it:
several G-d M4 agents died this way before finishing.

What worked: do not use Read/Edit/Write on files under `crates/` (or on any
`CLAUDE.md`). Read with `grep -n` and `sed -n 'A,Bp'` (at most about 120 lines),
edit with a `python3` heredoc doing an exact replacement guarded by an `assert`
that the old text exists, create files with `cat > f <<'EOF'`, and redirect all
cargo output to a file, printing only `grep -E "^error|test result|FAILED|panicked"`.
Watch for the silent failure of that edit style: a `replace` without the assert
quietly does nothing when `cargo fmt` has reflowed the text (it cost a debugging
round once).

Follow-up worth doing: trim the largest guides (move history into `docs/lessons/`
and the ADRs, keep only entry points and gotchas) so the harness's auto-injection
is cheap again. The thin-entry-point rule at the top of the root `CLAUDE.md`
already says that is the intent.
