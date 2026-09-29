"""Normalize Docker log decoration for configured check result parsing.

The original output stays in its immutable, hashed log. Only count and reject
patterns see this text, so colored and timestamped test summaries remain visible.
"""

from __future__ import annotations

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
