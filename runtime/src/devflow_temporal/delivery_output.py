"""Normalize terminal log decoration for configured check result parsing.

The original output stays in its immutable, hashed log. Only count and reject
patterns see this text, so colored and timestamped test summaries remain visible.
"""

from __future__ import annotations

import hashlib
import re

_TERMINAL_ESCAPE = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07\x1b]*(?:\x07|\x1b\\))")
_DOCKER_TIMESTAMP = re.compile(
    r"(?m)^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2}) "
)


def visible_output(output: str) -> str:
    return _DOCKER_TIMESTAMP.sub("", _TERMINAL_ESCAPE.sub("", output))


def observed_test_count(output: str, pattern: str) -> int:
    """Read one runner's largest summary, excluding duplicated CI annotations."""

    summaries = "\n".join(
        line for line in visible_output(output).splitlines() if not line.lstrip().startswith("::")
    )
    numbers = re.findall(pattern, summaries)
    return max(int(number) for number in numbers) if numbers else 0


def rejection_causes(
    output: str, patterns: list[str], *, test_results: bool = False,
) -> list[dict]:
    """Bounded matched evidence, independent of a diagnostic's tail truncation."""
    causes = []
    for pattern in patterns:
        match = next((m for m in re.finditer(pattern, output)
                      if not (test_results and _passing_test_title(output, m))), None)
        if match is None:
            continue
        start = max(0, match.start() - 120)
        end = min(len(output), match.end() + 120, start + 768)
        causes.append({
            "pattern": pattern[:512], "pattern_length": len(pattern),
            "pattern_sha256": hashlib.sha256(pattern.encode()).hexdigest(),
            "span": [match.start(), match.end()],
            "match": match.group()[:512], "match_length": match.end() - match.start(),
            "context": output[start:end], "context_start": start,
            "output_sha256": hashlib.sha256(output.encode()).hexdigest(),
        })
    return causes


def _passing_test_title(output: str, match: re.Match[str]) -> bool:
    """Outcome words in an explicitly passing test description are not outcomes.

    Keep matching on the original normalized log so genuine subsequent failures
    still produce their original spans and hashes. Other configured prohibitions
    remain effective even on a passing test line.
    """
    if match.group().lower() not in {"failed", "skipped", "flaky", "did not run"}:
        return False
    line_start = output.rfind("\n", 0, match.start()) + 1
    prefix = output[line_start:match.start()]
    return bool(re.match(r"^\s*(?:[✓✔√]\s+|ok\s+\d+\s+-\s+)", prefix))
