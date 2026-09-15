# In a worktree session, an absolute-path tool call (Read/Edit/Write) is not scoped by the shell's `cd` — pin every path under the worktree root explicitly, every time.

**In a worktree session, an absolute-path tool call (Read/Edit/Write) is not
scoped by the shell's `cd` — pin every path under the worktree root
explicitly, every time.** A `Bash` `cd /path/to/main/repo && ...` changes the
*shell's* cwd for subsequent Bash calls, but Read/Edit/Write take literal
absolute paths and don't care what the shell's cwd is — so it is easy to
`cd` into the main checkout for one command (e.g. to run cargo from a
familiar path) and then keep handing Read/Edit/Write paths that *look*
worktree-rooted but are actually bare `/repo/...` paths resolving into the
main checkout, silently editing a different working tree than intended.
The tell was a `git status` on what should have been the worktree suddenly
reporting the *main repo's* branch name, and a test binary not picking up an
edit that Read/Edit had just reported succeeding — both mean the tool and the
build are looking at two different files. Recovery: `git diff` the
suspect-wrong checkout, confirm which hunks are genuinely new (not
pre-existing unrelated dirty state) before touching anything, revert only
those, and re-apply them (a filtered `git apply --include=<path>` off a saved
patch is faster and safer than re-doing every edit by hand) in the correct
location. Never `git checkout --`/reset a dirty file without first diffing
it to confirm every hunk is yours. (PR #34.)
