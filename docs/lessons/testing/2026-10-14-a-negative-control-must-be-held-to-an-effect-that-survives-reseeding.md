# A negative control must be held to an effect that survives reseeding

**Context.** Controls (c) and (f) of `election_backpressure_corpus` passed on their canonical seed and failed on `_s01` at `ANIMUS_ELECTION_BP_SEEDS>=2` (issue #1313). The oracle was asserting the fault breaks an absolute bound (c: recovery above 5 s, which only 2 of 16 seeds hit, when the re-arm deadlock forms) or breaks a maximum (f: `since_restart_s`, which ties or dips by up to 0.2 s).

**Lesson.** A control proves the oracle can see the fault, so hold it to the statistic where the fault's effect is robust, and say what the measured margin is. Compare against the clean mechanism on the same seed (c: faulty recovery never faster than the real governor's, slower by 0.2..0.9 s on all 15 reseeds), and prefer an integral or average over a maximum (f: leaderless group-seconds 196..208 against 138..142). Keep the strict absolute assertion on the frozen canonical seed. Probe-step noise gets a named constant, not a silently widened threshold.

**Also.** `density_admission_corpus` cell 5 counted attempts with the node-wide `CpCampaignAdmitted` (every hosted group and the ghosts), so its `rounds` multiplier was hundreds and the bound could not fail. Use the per-group `campaign_stats().rounds_started`, print the multiplier, and add the ghosts' re-ask term.
