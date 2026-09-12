#!/bin/sh
# Link the bundled skills into the host's discovery directory.
set -eu

if [ "$#" -gt 1 ]; then
    printf 'Usage: %s [skills-directory]\n' "$0" >&2
    exit 2
fi

source_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
destination=${1:-"${HOME}/.agents/skills"}

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
printf 'Skills installed in %s\nKeep this checkout at %s.\n' "$destination" "$source_root"
