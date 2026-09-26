"""Normalize terminal display escapes for configured check result parsing.

The original output stays in its immutable, hashed log. Only count and reject
patterns see this visible-text view, so colored test summaries do not vanish.
"""

from __future__ import annotations

import re

_TERMINAL_ESCAPE = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07\x1b]*(?:\x07|\x1b\\))")


def visible_output(output: str) -> str:
    return _TERMINAL_ESCAPE.sub("", output)
