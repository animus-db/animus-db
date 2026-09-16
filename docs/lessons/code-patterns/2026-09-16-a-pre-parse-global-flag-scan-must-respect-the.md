# A pre-parse global-flag scan must respect the positional boundary

When a global flag (e.g. a CLI's `--tls-ca PATH`) is pulled out of `argv`
*before* the subcommand's own parser runs, it is tempting to implement the
extraction as a whole-argv `position(|a| a == "--the-flag")` scan, because
that's the simplest code that handles "the flag can go anywhere before the
subcommand's own args start." But once any subcommand accepts free-form
string positionals (a key, a value, a table name), "anywhere" is a lie: a
positional argument that happens to equal the flag's literal spelling is
indistinguishable from the flag itself to that scan, gets silently stripped,
and — because the scan also eats the token after it as the flag's own
value — every argument after it silently shifts by two. The failure is not
a crash; it is a *different, valid-looking command running instead of the
one given*, sometimes with no error at all (`animus-cli`, issue #840:
`put <addr> <table> --tls-ca myvalue` treated `--tls-ca` as the global TLS
flag and `myvalue` as its PATH, silently dropping the intended `<key>`
`<value>` pair).

The fix is to make the extraction's eligible window exactly as wide as the
places the flag can legitimately appear — normally just the prefix before
the first non-flag token (the subcommand name) — never the whole argument
list, and to give the boundary an explicit escape (`--`) so an operator can
still pass positional data that happens to collide with a flag's spelling.
Before writing (or reviewing) a "pull this flag out first, in place" helper
for any CLI in this repo, grep for wherever it claims to search "anywhere"
in its own doc comment — that phrase is the tell.
