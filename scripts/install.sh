#!/bin/sh
# Link bundled skills and copy loadable agent definitions into the host directories.
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
default_destination="${HOME}/.agents/skills"
default_codex="${CODEX_HOME:-${HOME}/.codex}"
if [ "$(uname -s)" = Darwin ] && [ "$destination" = "$default_destination" ] && [ "$codex_directory" = "$default_codex" ]; then
    reconcile_agents="${HOME}/Library/LaunchAgents"
    reconcile_mode=active
else
    # Isolated candidate/custom installs receive a reviewable plist without
    # starting a background job against an unintended home or database.
    reconcile_agents="$codex_directory/LaunchAgents"
    reconcile_mode=staged
fi

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

rollback_backup=$("$devflow_python" -B "$source_root/scripts/install-rollback.py" capture \
    "$source_root" "$destination" "$codex_directory")
restore_on_failure() {
    install_status=$?
    trap - 0
    if [ "$install_status" -ne 0 ]; then
        if "$devflow_python" -B "$source_root/scripts/install-rollback.py" restore "$rollback_backup"; then
            "$devflow_python" -B "$source_root/scripts/install-rollback.py" discard "$rollback_backup" || true
        else
            printf 'Install rollback failed; preserved snapshot at %s\n' "$rollback_backup" >&2
        fi
    fi
    exit "$install_status"
}
trap restore_on_failure 0
check_links "$source_root/skills" "$destination"
"$devflow_python" -B "$source_root/scripts/reconcile-service.py" \
    --launch-agents "$reconcile_agents" inspect > /dev/null
command -v gh > /dev/null || { printf 'gh is required for issue reconciliation.\n' >&2; exit 1; }
"$devflow_python" -B "$source_root/scripts/install-agents.py" preflight \
    "$source_root" "$destination" "$codex_directory" "$force"
"$devflow_python" -B "$source_root/scripts/install-agents.py" apply \
    "$source_root" "$destination" "$codex_directory" "$force"
make_links "$source_root/skills" "$destination"
"$devflow_python" -B "$destination/devflow/scripts/telemetry.py" install --codex-home "$codex_directory" \
    --guard-path "$codex_directory/.devflow-hook.py"
prune_links "$source_root/skills" "$destination"
"$devflow_python" -B "$source_root/scripts/install-guard.py" snapshot "$source_root" "$destination" "$codex_directory"
if [ "$reconcile_mode" = active ]; then
    "$devflow_python" -B "$source_root/scripts/reconcile-service.py" \
        --launch-agents "$reconcile_agents" install --codex-home "$codex_directory"
else
    "$devflow_python" -B "$source_root/scripts/reconcile-service.py" \
        --launch-agents "$reconcile_agents" install --codex-home "$codex_directory" --no-start
fi
trap - 0
"$devflow_python" -B "$source_root/scripts/install-rollback.py" discard "$rollback_backup" || true
# Service activation is the final fallible step. A broken informational stdout
# pipe must not make update.sh roll back after the daemon may have migrated DB.
printf 'Skills linked in %s\nAgent definitions copied into %s\nKeep this checkout at %s.\n' \
    "$destination" "$codex_directory/agents" "$source_root" || true
