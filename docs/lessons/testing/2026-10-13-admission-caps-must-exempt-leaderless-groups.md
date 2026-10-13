# A work-admission cap must exempt work whose absence is an outage

ADR 0079 step 4 capped bulk wakes of quiesced Raft groups. After a node kill the
admitted groups could not quiesce (a voter was dead), so the slots never turned
over and the queued groups, including those whose leader had died, stayed
leaderless until a client touched them. The corpus hid this: two cells ended with
a "client wake" that rescued exactly those groups.

Lessons: (1) a cap that defers recovery work needs an exemption for the class that
nothing else will recover (here: leader lost), decided from information the
deferred party really has locally (`leader()`), not from global state; (2) a
"clients arrive afterwards" step in a fault cell is a smell: assert that the system
recovers with NO client touch, and give that oracle a negative control; (3) when a
bypass changes the cost model (over-cap population is now `G/N`, not `A`), re-read
the comparative assertions (lag, high-water) rather than loosening them blindly.
