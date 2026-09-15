# Commit the election no-op in `become_leader` itself (`maybe_advance_commit` after the append)

**Commit the election no-op in `become_leader` itself (`maybe_advance_commit`
after the append)** — a leader that only advances commit on propose/ack strands
a sole voter's recovered WAL tail (nothing re-drives commit until the next
propose), and any gate on "current-term entry committed" (ReadIndex §6.4, the
membership-change gate) would deadlock a single-node group. (PR #25.)
