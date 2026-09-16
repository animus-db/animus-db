# For a *true black-box* Elle check, store the datatype and observe it — don't reconstruct it from the ordering layer's log.

**For a *true black-box* Elle check, store the datatype and observe it — don't
reconstruct it from the ordering layer's log.** Reconstructing each read's list
from `AccordNode::applied_order` (the old register modelling) limits the
checker's teeth to cross-replica *divergence*: a single globally-agreed but
non-serializable order can't show as a cycle, because the lists are derived from
the very order under test. With **arbitrary write values** (ADR 0011) each key
now stores a real list and reads observe stored bytes
(`AccordNode::read_value_result`), so `check_cycles` is genuinely black-box
(`animus-test/tests/support/mod.rs`). Read "final state" straight from stored
values on **two distinct replicas** (a real cross-replica agreement check), and
use **single-writer-per-key** so per-key LWW doesn't lose appends — and build
each append on the client's own authoritative list, not a begin-time quorum read
(the apply flips `is_applied` before its fire-and-forget data-plane write lands,
so a begin-time read can be stale and lose the client's own earlier appends).
