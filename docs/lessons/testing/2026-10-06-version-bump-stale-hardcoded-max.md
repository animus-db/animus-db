# A test that hard-codes "the newest version" goes stale on a merge, not on its own PR

`animus-cli`'s `roll_cli::plan_after_finalize_is_empty_not_a_roll_toward_an_unsupported_version`
hard-coded 2 as "the newest version this build speaks". Phase 3 was green on
its own branch; `main` then bumped `MAX_SUPPORTED` to 3 (G-01 G-d M1) and the
merge commit failed the shard deterministically (the CLI build's `cli_max`
became 3, so `settle_goal` legitimately planned a roll to 3).

Rule: any fixture standing for "the top of this build's range" must read
`animus_control::version::MAX_SUPPORTED` (and build the faked view's
`own_range`/`safe_target` from it), never a literal. Two branches that each pass
alone can still collide through a constant one of them bumps; grep tests for the
literal when a PR touches the version constants.
