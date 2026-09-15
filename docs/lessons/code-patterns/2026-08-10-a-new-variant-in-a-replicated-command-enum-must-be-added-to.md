# A new variant in a replicated command enum must be added to every *gating* match, not just `apply` — a missed relay allowlist is a bimodal per-process flake.

**A new variant in a replicated command enum must be added to every *gating*
match, not just `apply` — a missed relay allowlist is a bimodal per-process
flake.** `animusd`'s cross-process proposal path gates on `is_relayable_command`;
a `MetaCommand` variant missing there **works whenever the connected node happens
to be the control leader** (proposed locally) and silently times out ("did not
commit") when it must relay to another node's leader. The compiler can't catch a
`matches!` allowlist, and single-node tests never exercise the relay. When adding
a variant, grep the enum's name for gating `matches!`/match sites (allowlists,
admin filters) and update them in the same change; regression-test the new
command through a **follower-connected** node in a per-process cluster.
(`DropTableTablets`; caught by `drop_table_gc.rs`'s 3-node test going bimodal.)
