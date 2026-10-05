# Merging main into a branch that added a struct field: new files still break

When a branch adds a field to a widely-literal'd struct (e.g. `RoleAddrs.labels`)
and `main` concurrently adds a different field (`overload`), the ~60 textual
conflicts are all "keep both lines" and can be resolved mechanically. The
trap is the files `main` *added* (e.g. `tests/overload.rs`): they merge cleanly
yet lack the branch's new field, so the merge compiles nowhere until
`cargo clippy --all-targets` is run. Always run clippy/build over
`--all-targets` after a mechanical resolve, never just `git diff --check`.

Also: `pkill -f '<pattern>'` from a Bash tool call matches its own command line
and kills the call; wait on a background job with a file-marker poll instead.
