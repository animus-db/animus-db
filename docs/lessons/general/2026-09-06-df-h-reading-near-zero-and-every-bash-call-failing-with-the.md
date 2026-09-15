# `df -h /` reading near-zero and every Bash call failing with "the temp filesystem is full" can be the SAME root cause, not two problems

Mid-session, every `Bash` tool call started failing with "Command output
was lost: the temp filesystem at /tmp/claude-0/.../tasks is full (0MB
free)" — including a bare `echo done > file`, which made it look like the
harness's own tmpfs (unrelated to the repo) had filled up independently of
anything this session was doing. It hadn't: `df -h /` (once a command
finally got through) showed the **root filesystem itself** at 140K free —
the harness's task-output tmpfs and `/home/user/animus-db/target` share the
same underlying disk, so a 28GB `target/debug/deps` (accumulated across a
long session's worth of `cargo build`/`test` invocations, many of them
recompiling the same crates under slightly different feature combinations
and leaving old-hash duplicate `.rlib`/test-binary artifacts behind) had
quietly starved the whole filesystem, harness tmp included.

**The fix**: exactly the prune this repo's own task instructions already
describe for this situation — group `target/debug/deps/*` by basename
(strip the trailing `-<16 hex>` build-hash suffix) and delete every file
in a group except the newest by mtime, which recovered 17GB here. **The
lesson worth generalizing**: when a sandboxed harness's own bookkeeping
(temp files, output capture, anything **not** the actual task) starts
failing with a space/quota-shaped error, check the *whole* filesystem's
free space before assuming the harness's own storage is a separate,
unrelated resource — on a single-disk sandbox it usually isn't, and the
real fix lives in the repo's build output, not in anything the harness
itself controls.
