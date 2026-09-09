"""Synthetic editorial acceptance check; no external data or dependencies."""
from pathlib import Path

assert Path("README.md").read_text() == "Reviewed local handoff.\n"
assert Path("unrelated.txt").read_text() == "preserved unrelated content\n"
print("PROSE_INVARIANT_OBSERVED")
