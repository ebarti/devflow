#!/bin/sh
# Link the bundled skills into the host's discovery directory.
set -eu

if [ "$#" -gt 2 ]; then
    printf 'Usage: %s [skills-directory] [codex-home]\n' "$0" >&2
    exit 2
fi

source_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
destination=${1:-"${HOME}/.agents/skills"}
codex_directory=${2:-"${CODEX_HOME:-${HOME}/.codex}"}

# Preserve existing files and skills from other installations.
for skill in "$source_root"/skills/*; do
    target="$destination/${skill##*/}"
    if [ -e "$target" ] || [ -L "$target" ]; then
        if [ -L "$target" ] && [ "$(readlink "$target")" = "$skill" ]; then
            continue
        fi
        printf 'Existing path preserved: %s\nChoose a different skills directory or relocate that path.\n' "$target" >&2
        exit 1
    fi
done

mkdir -p "$destination"
for skill in "$source_root"/skills/*; do
    target="$destination/${skill##*/}"
    if [ ! -L "$target" ]; then
        ln -s "$skill" "$target"
    fi
done
python3 -B "$destination/devflow/scripts/telemetry.py" install --codex-home "$codex_directory"
printf 'Skills installed in %s\nKeep this checkout at %s.\n' "$destination" "$source_root"
