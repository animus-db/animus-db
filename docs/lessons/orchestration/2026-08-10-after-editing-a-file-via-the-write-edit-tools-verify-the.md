# After editing a file via the Write/Edit tools, verify the change actually reached the filesystem the Bash tool (and thus `cargo`) sees — don't trust a "success" result alone.

**After editing a file via the Write/Edit tools, verify the change actually
reached the filesystem the Bash tool (and thus `cargo`) sees — don't trust
a "success" result alone.** Debugging this PR's own heartbeat-liveness
regression test (ADR 0037 hardening PR1, PR #134) burned well over an
hour chasing a phantom distributed-systems bug — a control-only leader's
`FailureDetector` losing `believes_alive` ~500ms after a runtime-added
voter took over leadership, permanently, never self-healing — that was
entirely explained once `git diff --stat`/`grep` **run through Bash**
showed the fix (`heartbeat_loop_live`, the `peer_sync_loop` address-book
merge) had *never actually landed* in `crates/animusd/src/lib.rs`: the
Write/Edit tool session had a stale/cached view of that file, diverged
from what a fresh `cat`/`Write` heredoc through Bash produced for a
*different* file in the same debugging session (a test file created via
`Write`, invisible to `ls`/`grep` through Bash until the same content was
independently written through Bash itself). Every `cargo build` in
between kept succeeding — which felt like confirmation the edits had
landed, but a clean build of *unchanged* code is indistinguishable from a
clean build of the intended change; a passing build proves nothing about
which source it built. **Concrete mitigation**: immediately after any
Write/Edit-tool change to a file a build depends on, run `grep`/`git diff
--stat` on that exact path **through the Bash tool** for a string unique
to the new content, before writing a single line of test/debug code
against the assumption it landed — this costs one command and would have
caught the divergence at the first edit instead of an hour into a wild
goose chase. If Bash's view and the Edit tool's view ever disagree on a
file's content (one tool sees a change the other doesn't, or a
freshly-`Write`-created file is invisible to `ls` through Bash), treat it
as a real tooling desync, not a typo — stop trusting that tool's Read
cache for the affected file and do all further edits to it through Bash
(`cat > file <<'EOF' ... EOF` / `python3` in-place patch) until a fresh
`Read` of the file (forced by an external touch, which this session's
harness surfaces as a "file was modified externally" notice) demonstrably
matches Bash's own view again.
