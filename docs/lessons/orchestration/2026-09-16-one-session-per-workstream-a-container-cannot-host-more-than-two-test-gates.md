# One session per workstream: a container cannot host more than two test gates

## What happened

A single coordinating session ran four implementation agents at once on a
4-core web container. Each reached its validation gate (a full crate test
suite plus a `prod-heavy` run) at about the same time, the 1-minute load
sat at 10–12 for an hour, and three independent things went wrong:

- `ProdEnv` liveness tests timed out spuriously (`large_metadata_catch_up_
  stays_live`, `forward_hop_timeout_tests`, a bootstrap wait), each looking
  like a real failure until re-run on a quiet box.
- Every agent's gate took two to three times longer, so the gates overlapped
  even more.
- Two agents built the same `animusd` test binary into the shared
  `CARGO_TARGET_DIR` from different worktrees, and one of them was served
  the other's binary by a `cargo test --no-run` that reported "up to date";
  it only noticed through a `--list | grep <test>` canary.

Meanwhile the work itself was largely independent: a stacked PR series with
the defects gating it, and a backlog of unrelated issues.

## The lesson

CPU and the target directory are per container, so a session's throughput
is capped at about two concurrent heavy agents no matter how many it
launches, and past that point the extra agents make every result less
trustworthy. Split by workstream instead: one session keeps the entangled
work (a stacked series and the defects that gate it), and each independent
stream gets its own session in the same environment, with a brief that
carries its scope, an explicit do-not-touch list for what the parent owns,
the mechanics it needs, and reporting to the maintainer in its own chat.
This is now Session operating mode item 5 in `CLAUDE.md`.
