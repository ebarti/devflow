#!/bin/sh
# Link the bundled skills and agent definitions into the host's directories.
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
devflow_python=${DEVFLOW_PYTHON:-python3.12}

# Stop on any conflict before changing a link; force replaces only symlinks.
check_links() {
    for source in "$1"/*; do
        [ -e "$source" ] || continue
        target="$2/${source##*/}"
        if [ -e "$target" ] || [ -L "$target" ]; then
            if [ -L "$target" ]; then
                if [ "$force" = true ] || [ "$(readlink "$target")" = "$source" ]; then
                    continue
                fi
            fi
            printf 'Existing path preserved: %s\nUse --force for symlinks; relocate other conflicting paths.\n' "$target" >&2
            exit 1
        fi
    done
}

make_links() {
    mkdir -p "$2"
    for source in "$1"/*; do
        [ -e "$source" ] || continue
        target="$2/${source##*/}"
        if [ "$force" = true ] && [ -L "$target" ]; then
            ln -sfn "$source" "$target"
        elif [ ! -L "$target" ]; then
            ln -s "$source" "$target"
        fi
    done
}

# Remove only obsolete links owned by this checkout.
prune_links() {
    for target in "$2"/*; do
        [ -L "$target" ] || continue
        previous="$1/${target##*/}"
        if [ "$(readlink "$target")" = "$previous" ] && [ ! -e "$previous" ]; then
            unlink "$target"
        fi
    done
}

check_links "$source_root/skills" "$destination"
check_links "$source_root/agents" "$codex_directory/agents"
make_links "$source_root/skills" "$destination"
make_links "$source_root/agents" "$codex_directory/agents"
"$devflow_python" -B "$destination/devflow/scripts/telemetry.py" install --codex-home "$codex_directory"
prune_links "$source_root/skills" "$destination"
prune_links "$source_root/agents" "$codex_directory/agents"
printf 'Skills installed in %s\nAgent definitions installed in %s\nKeep this checkout at %s.\n' \
    "$destination" "$codex_directory/agents" "$source_root"
