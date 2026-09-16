# A fault-schedule runner that heals immediately after the last fault gives single-fault scenarios a zero-length outage — give scenarios an explicit fault window.

**A fault-schedule runner that heals immediately after the last fault gives
single-fault scenarios a zero-length outage — give scenarios an explicit fault
window.** The raftkv corpus healed partitions the instant the last fault landed,
so its partition cells were near-vacuous (nothing was ever asked of the cluster
*while* partitioned). New cells carry `Scenario::window` (outage duration with
traffic spanning it); old cells keep window 0 for byte-identity. Check any new
fault harness for this: "did traffic actually run during the fault?" (PR #23.)
