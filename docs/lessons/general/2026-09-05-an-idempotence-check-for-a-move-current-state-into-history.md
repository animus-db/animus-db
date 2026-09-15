# An idempotence check for a "move current state into history" command must compare against the row's post-apply effect, not re-derive the history entry from the row's own already-mutated fields (ADR 0066, `RotateCredential`)

`MetaCommand::RotateCredential` (the credential catalog's rotation-grace
primitive) moves a row's current `secret` into `previous` and installs a
new current secret — the identical "no-op on an identical retried
payload" idempotence every other catalog command in this state machine
promises (`SetTableTtl`, `BeginBackup`, …). The first implementation's
idempotence check computed `new_previous = PreviousSecret { secret:
existing.secret.clone(), valid_until: now + grace_secs }` from `self`'s
**current** row and compared it against what was already on file. That
comparison is correct only for a command whose effect doesn't depend on
what it itself already did — the moment a `RotateCredential` command is
*replayed* against a row it already mutated, `existing.secret` is no
longer the pre-rotation secret the retried command's own `new_previous`
should be compared against: it's the post-rotation one. Concretely, replaying
`Rotate{new_secret: "s1", grace_secs, now}` against a row where `secret`
is already `"s1"` computes `new_previous.secret = "s1"` (today's current
secret) instead of recognizing that the row's *actual* `previous.secret`
(the real pre-rotation value, correctly preserved from the first apply) is
what a genuine retry must leave untouched — the check's own freshly
recomputed value disagreed with the already-correct stored one and the
"identical repeat" test failed by re-rotating a second time, moving the
already-rotated secret into `previous` a second time and silently losing
the real original.

Found immediately by the very idempotence unit test the ADR's own Testing
section asked for (`rotate_credential_is_idempotent_on_identical_repeat`)
— not by inspection. The fix inverts what the check compares: recognize a
retry by three facts already sitting on the **existing, unmodified** row —
its current secret already equals `new_secret`, its `updated_at` already
equals this command's own `now`, and its `previous.valid_until` already
equals what `(now, grace_secs)` would produce — and only build/install a
*fresh* `PreviousSecret` (from `existing.secret` as it stands *before* this
apply mutates anything) once that three-fact check has ruled out a retry.
**General form**: an idempotence check for a command whose effect is
defined in terms of "the current value becomes the new history entry"
must never recompute what that history entry *would be* from the row's
already-current-at-check-time fields and compare that recomputation
against storage — on a genuine replay, "current" has already moved past
what the original apply started from, so the recomputation silently
describes a different (wrong) transition than the one already recorded.
Detect a replay by facts that don't require re-deriving the transition at
all (a target value already installed, a timestamp already stamped, a
derived deadline already matching), then perform the real mutation from
whatever `existing` looks like *at that point* — never build the
would-be-new value first and use it as the idempotence oracle.
