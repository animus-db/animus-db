# A margin that a design silently depends on must be enforced in code, or it is just the original bug one layer down.

**A margin that a design silently depends on must be enforced in code, or it
is just the original bug one layer down.** The quiesce veto's safety in
production rested on an unstated 25x ratio between `--quiesce-after` (5s
default) and the hard-coded 200ms sweep interval; nothing enforced it, and
the test that exposed #302 used a 1.5x ratio. The fix pairs the correctness
change with an enforced floor (`animusd::MIN_QUIESCE_AFTER`, validated on the
CLI flag and `debug_assert`ed at node start), which also turns the test's
tight knob into a genuine regression guard rather than a restatement of the
bug's own "usually fine" margin. When a mechanism's safety argument contains
the words "much larger than", make the comparison executable. (#302,
`crates/animusd/src/{lib,main}.rs`, 2026-08-20.)
