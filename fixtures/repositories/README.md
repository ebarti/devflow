# Synthetic repository reuse fixtures

These two repositories exercise A18: the same workflow engine adopts different
repository profiles and executes their own checks through the registered check
runner. They contain no JobCtrl data or engine-specific branches.

- `prose`: a Python check validates one exact README sentence and an unrelated-file sentinel.
- `release-notes`: a shell check validates the exact two-line changelog and the same preservation sentinel.

`fixture-case.json` supplies test-only accepted edits. The acceptance test copies
each fixture into a temporary Git repository, resolves its local repository
identity, commits the baseline, and makes the editorial edit on a dedicated branch.
Both run the real check command and reach independently observed local delivery.

The `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa` workflow revisions are explicitly
synthetic placeholder pins used to test profile loading. They do not identify an
installable release and must be replaced with a verified full release commit when
adopting a real repository. Repository IDs are likewise synthetic placeholders;
these directories are fixtures, not enrolled workspaces or installation targets.
