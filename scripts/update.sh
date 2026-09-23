#!/bin/sh
# Select a published release in the existing installation checkout.
set -eu

upgrade() {
    if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
        printf 'Usage: %s TAG [skills-directory] [codex-home]\n' "$0" >&2
        exit 2
    fi
    release_tag=$1
    shift
    source_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
    git check-ref-format "refs/tags/$release_tag"
    if ! git -C "$source_root" diff --quiet HEAD --; then
        printf 'Preserve or commit tracked changes before upgrading.\n' >&2
        exit 1
    fi
    git -C "$source_root" fetch --no-tags origin "refs/tags/$release_tag:refs/tags/$release_tag"
    release_commit=$(git -C "$source_root" rev-parse --verify "refs/tags/$release_tag^{commit}")
    previous_commit=$(git -C "$source_root" rev-parse HEAD)
    previous_branch=$(git -C "$source_root" symbolic-ref -q --short HEAD || true)
    git -C "$source_root" checkout --detach "$release_commit"
    if sh "$source_root/scripts/install.sh" "$@"; then
        return 0
    else
        result=$?
    fi
    if [ -n "$previous_branch" ]; then
        git -C "$source_root" checkout "$previous_branch"
    else
        git -C "$source_root" checkout --detach "$previous_commit"
    fi
    return "$result"
}

upgrade "$@"
