# An LSM stall test must withhold compaction; a slow disk alone never reaches the stall

ADR 0081 characterisation (SimEnv, `DiskConfig` `sync_delay` 5 to 30 ms, up to 32
engines, a pool of 2 compaction permits): with everything off, level-0 never
climbed past twice the compaction trigger (high-water mark 8 at trigger 4). The
engine's own urgent-bypass level (an L0 count at 2T is compacted even when the
permit pool is empty) and the memtable cap (4x the flush threshold) hold the
line first; the active cause under a slow disk is the **memtable**, not L0.

Rules:

- To test the L0 slowdown/stop levels, use a gate that withholds *every*
  compaction including the urgent one (`Wedge` in `lsm_write_stall.rs` and
  `lsm_stall_admission.rs`). It models "compaction cannot keep up" (a starved
  CPU, a wedged disk), which is outside the gate's contract; say so in the file
  doc, or the corpus looks like it tests a state the engine never reaches.
- A negative control must be given a fault the oracle can actually see: the
  old bounded gate only fails after 10 s of continuous stall, so a control run
  shorter than that (or with data too small to reach the level) passes vacuously.
  Print the stall counters once while writing the control to prove it engaged.
- Report the measured high-water mark next to the threshold in the ADR: it tells
  the reader which levels are active safeguards and which are a backstop.
