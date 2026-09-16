#!/bin/sh
# Link the bundled skills into the host's discovery directory.
set -eu

force=false
if [ "${1:-}" = "--force" ]; then
    force=true
    shift
fi
if [ "$#" -gt 2 ]; then
    printf 'Usage: %s [--force] [skills-directory] [codex-home]\n' "$0" >&2
    exit 2
fi

source_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
destination=${1:-"${HOME}/.agents/skills"}
codex_directory=${2:-"${CODEX_HOME:-${HOME}/.codex}"}

# Check all conflicts before changing any links; force only replaces symlinks.
for skill in "$source_root"/skills/*; do
    target="$destination/${skill##*/}"
    if [ -e "$target" ] || [ -L "$target" ]; then
        if [ -L "$target" ]; then
            if [ "$force" = true ] || [ "$(readlink "$target")" = "$skill" ]; then
                continue
            fi
        fi
        printf 'Existing path preserved: %s\nUse --force for symlinks; relocate other conflicting paths.\n' "$target" >&2
        exit 1
    fi
done

mkdir -p "$destination"
for skill in "$source_root"/skills/*; do
    target="$destination/${skill##*/}"
    if [ "$force" = true ] && [ -L "$target" ]; then
        ln -sfn "$skill" "$target"
    elif [ ! -L "$target" ]; then
        ln -s "$skill" "$target"
    fi
done
python3 -B "$destination/devflow/scripts/telemetry.py" install --codex-home "$codex_directory"
printf 'Skills installed in %s\nKeep this checkout at %s.\n' "$destination" "$source_root"
