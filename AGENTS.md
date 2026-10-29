# Repository contribution rules

## Testing

- Add tests only for high-value user or workflow regressions, data integrity,
  security boundaries, or integration behavior. Exercise a real entry point or
  meaningful contract, and state the failure or risk the test catches.
- Do not add small isolated unit tests, tests that mirror implementation details,
  trivial parameter explosions, or tests solely to increase counts or coverage.
  Extend an existing meaningful scenario where possible.
- Keep the runtime suite curated. Remove redundant or low-value cases instead of
  hiding them with deselection, skip markers, or collection configuration. CI must
  continue to run every retained collected case exactly once across its partitions.
- Run the checks appropriate to the change. Use disposable homes, workspaces and
  fixtures; do not mutate active installations, settings or deliveries for tests.

## Pull requests

Use conventional PR titles: `type(scope): concise description` (scope optional),
for example `fix(runtime): preserve cleanup ownership` or
`ci: simplify runtime test scheduling`. Choose a type that describes the change,
such as `fix`, `feat`, `refactor`, `test`, `docs`, `ci`, `build`, `perf`, or `chore`.
