# A negative control needs the state that makes the bug reachable

ADR 0078 control (c) gates the *responder* side of election admission (a
deferred follower re-arms its timer and refuses pre-votes). On a cold start it
changes nothing: no follower believes in a leader yet, so every voter grants
pre-votes anyway and the bug is unreachable. It only bites on the leader-node
loss shape, where the surviving followers do believe in the dead leader and
refuse pre-votes until their deadline lapses.

Before trusting a control, run it and confirm the oracle fails for the reason
you claim (here: groups never recovered, not merely "slower"). If the control
passes, the cell does not reach the bug; change the cell, not the assertion.
A related trap: the ADR bound for the lost-leader delay stops at admission, so
a literal assertion fails by one election round trip; state the slack in the
test rather than loosening it silently.
