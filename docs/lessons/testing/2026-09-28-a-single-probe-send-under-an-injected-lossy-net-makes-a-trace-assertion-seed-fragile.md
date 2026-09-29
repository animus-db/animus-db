# A single probe send under an injected lossy net makes a delivery-time trace assertion a function of where the seed's RNG lands

`crates/animus-cp-data/tests/demux_stream_teardown.rs` sends one "late" frame
at a closed stream and asserts the trace carries a `stream-closed` drop. The
file's network runs `set_drop_prob(0.05)`, and a `lossy` drop happens at
**send** time, before a `Deliver` event exists, so the delivery-time
closed-stream check never sees that frame. When unrelated changes on `main`
(raft fixes, handshake modelling) shifted the RNG draw sequence, the one
probe landed on a lossy roll: the test failed with the seed unchanged and
nothing about closed streams broken.

The rule: a test that must observe a **delivery-time** behaviour through a
lossy or otherwise fault-injected network sends enough copies that missing
all of them is negligible (8 copies at 5% is about 4e-11), or drops the
loss for that step. Do not assume "the seed passes today" pins the outcome
across merges: any change that consumes RNG earlier in the run reshuffles
every later draw.
