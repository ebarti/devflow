#!/bin/sh
# The installation smoke check runs entirely in a temporary directory.
set -eu

source_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
install_fixture=$(mktemp -d)
trap 'rm -rf "$install_fixture"' EXIT HUP INT TERM
destination="$install_fixture/skills"

sh "$source_root/scripts/install.sh" "$destination"
for name in devflow using-devflow devflow-defining-work devflow-planning \
    devflow-coordinating devflow-implementing devflow-reviewing devflow-verifying \
    devflow-delivering; do
    test -f "$destination/$name/SKILL.md"
done
for name in state.py legacy.py schema.sql; do
    test -r "$destination/devflow/scripts/$name"
done
python3 -B "$destination/devflow/scripts/state.py" --help > /dev/null
printf 'Installation check passed.\n'
