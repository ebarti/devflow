#!/bin/sh
# Synthetic editorial acceptance check, independent of the prose fixture's Python check.
set -eu
grep -Fxq '# Changelog' CHANGELOG.md
grep -Fxq -- '- Clarify the local handoff.' CHANGELOG.md
test "$(wc -l < CHANGELOG.md | tr -d ' ')" = 2
test "$(cat unrelated.txt)" = 'preserved unrelated content'
printf '%s\n' 'NOTES_INVARIANT_OBSERVED'
