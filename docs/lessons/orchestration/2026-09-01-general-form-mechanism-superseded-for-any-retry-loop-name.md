# General form (mechanism superseded): for any retry loop, name the input that CHANGES on a failed attempt — if the answer is "none," it is a spin, not a retry, and the fix is feeding the failure's own payload back into the next attempt, done once at a shared choke point. And: the existing "test through a follower-connected node" rule is not enough for a tablet-addressed forward — the caller must host *no replica at all* of the target, which requires a cluster LARGER than the replication factor to prove at all.

**General form (mechanism superseded): for any retry loop, name the
input that CHANGES on a failed attempt — if the answer is "none," it is
a spin, not a retry, and the fix is feeding the failure's own payload
back into the next attempt, done once at a shared choke point. And: the
existing "test through a follower-connected node" rule is not enough for
a tablet-addressed forward — the caller must host *no replica at all* of
the target, which requires a cluster LARGER than the replication factor
to prove at all.** Originally learned from the now-deleted copy-based
split driver's own fork-F5 seeding hint-chase (`SeedRows` spinning
forever against a freshly-placed, off-node child); the fix — feeding a
refusal's own leader hint back into the next attempt at one shared choke
point — is still exactly what `forward_to_tablet_leader` does today for
every tablet-addressed RPC. See `docs/engineering-lessons-archive.md`'s
"The copy-based split-build driver" section for the full incident.
