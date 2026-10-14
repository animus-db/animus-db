# A refusal by silence needs a bound on the sender's permit

When a full receiver refuses by not acknowledging (no wire shape for "no"), the
sender's permit for that transfer is held until an ack that may never come. A
pool of N such senders then starves every transfer that could make progress.
Bound the unacknowledged time from the existing retry constants (here twice the
heartbeat backoff cap, so a live peer has been offered the chunk several times),
release the permit, and keep that peer out of the pool for one more bound so
the refused retry does not immediately retake the slot. Test it with a receiver
that never has room and a second transfer that must still get served, plus a
negative control with the bound off.
