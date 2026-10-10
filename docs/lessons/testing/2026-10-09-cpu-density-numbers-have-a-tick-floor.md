# Read a "nonzero CPU" threshold against the measurement's tick resolution

The C-17 quiesced-density harness reads process CPU from `/proc` in `CLK_TCK`
(100 Hz) ticks over a 10 s window, so its resolution is 1 ms/s, and the
zero-group baseline itself fluctuates 2-3 ms/s. A ratified "any nonzero steady
CPU" rule cannot be checked literally below that. After #1207 the RF3 row read
1.0 ms/s: one tick, 0.001 ms/s per group.

How to tell a floor from a residual wake: run the same cell at a different
group count. A real per-group periodic wake scales with G (the removed apply
poll gave 20 ms/s at 1k and 167 ms/s at 10k); a floor does not (RF1 read 0.0 at
both). State the resolution next to the numbers, and when ratifying such a
threshold write it as "net CPU within N ticks of the baseline, not scaling
with G" rather than "zero". Re-run each row at least twice before calling a
1-tick difference.
