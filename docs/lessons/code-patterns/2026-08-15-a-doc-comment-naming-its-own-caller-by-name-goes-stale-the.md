# A doc comment naming its own caller by name goes stale the moment that caller is deleted — even in a completely different crate, in a PR that never touches the file carrying the comment.

**A doc comment naming its own caller by name goes stale the moment that
caller is deleted — even in a completely different crate, in a PR that
never touches the file carrying the comment.** Deleting `MergeTablets`
and its wire surface (PR2, `animusd::index_drain::
cleanup_merge_residue_cursor_rows`) silently orphaned doc comments one
crate away, in `animus-cp-data` — a crate PR2's own diff never touched —
naming that exact function as "the caller" of `cursor::token_of` and
`RaftKvNode::cursor_rows_with_token` (`cursor.rs`/`lib.rs`). Both
primitives still compiled, still had a unit-test caller
(`tests/cursor_scope.rs`), and their own crate's build/clippy/test gates
all stayed green — nothing about deleting a caller in one crate makes a
*different* crate's doc comment describing that caller fail any gate.
**General check for any deletion PR: grep every file the deletion
touches for "no longer exists" callers of its own, but also grep the
*deleted symbol's own name* workspace-wide one more time after the PR
— a hit outside the files you touched is a doc comment describing a
caller that is now fiction, in a crate the deletion diff never had a
reason to open.**
