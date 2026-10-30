# A wire-level negative control must keep driving until its trigger fires

`sim_cluster_lsm_stall_control_old_gate_takes_the_apply_task_down` (issue
#1271) failed at nightly depth on seed 0x812003: "the old gate must violate
no-panic, got []". The control needs one writer parked at L0 stop for 10 s of
virtual time, but over the DynamoDB wire the test cannot see L0, and how fast
flushes pile up L0 files is seed-dependent (this seed added about one file per
12 s burst round; 5 fixed rounds reached 6 of the 10 needed). The product was
right; the control's fixed drive length was a guess.

Rule: a control whose oracle depends on a threshold being crossed drives the
fault in a bounded loop until the failure it is waiting for happens (here the
sim panics, caught by `catch_unwind`), and only then falls through to its loud
assertion. Never widen a fixed constant to fit one seed, and keep the healthy
run's depth unchanged. Same class as the storage-tier fix in 4c2bf477 (read the
level off the engine, drive until reached).
