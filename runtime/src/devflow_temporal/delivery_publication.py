"""A public change summary is separate from immutable execution instructions."""

from __future__ import annotations

import re
import unicodedata

# Periods inside ordinary abbreviations/initialisms and ellipses are not boundaries.
_NON_BOUNDARY = re.compile(
    r"\b(?:[a-z]\.){2,}|\b(?:vs|etc|mr|mrs|ms|dr|prof|sr|jr|st|no)\.|\.{2,}", re.I
)
_BIDI_CONTROLS = {"LRE", "RLE", "LRO", "RLO", "PDF", "LRI", "RLI", "FSI", "PDI"}
_BIDI_MARKS = {"\u061c", "\u200e", "\u200f"}


def conventional(subject: str) -> bool:
    return bool(re.fullmatch(r"[a-z][a-z0-9-]*(?:\([^()\r\n]+\))?!?: \S.*", subject))


def publication_summary(goal: str, supplied: str | None = None) -> str:
    """Admit one concise subject, never shorten a detailed execution prompt."""
    value = goal if supplied is None else supplied
    source = "goal used for publication_summary" if supplied is None else "publication_summary"

    def reject(rule: str) -> None:
        hint = ("; provide publication_summary separately for a detailed goal"
                if supplied is None else "")
        raise ValueError(f"{source} {rule}{hint}")

    if not isinstance(value, str) or not value.strip():
        reject("must be a non-empty string")
    # Refuse hidden controls before stripping: str.strip() also removes C1 NEL.
    if any(
        (unicodedata.category(ch) == "Cc" and ch not in " \t\r\n\v\f")
        or unicodedata.bidirectional(ch) in _BIDI_CONTROLS
        or ch in _BIDI_MARKS
        for ch in value
    ):
        reject("must not contain Unicode control or bidi formatting characters")
    summary = value.strip()
    if len(summary.splitlines()) != 1:
        reject("must be a single line after trimming surrounding whitespace")
    if any(unicodedata.category(ch) == "Cc" for ch in summary):
        reject("must not contain control characters within the summary")
    if re.search(r"[.!?]\s+\S", _NON_BOUNDARY.sub("abbreviation", summary)):
        reject("must not contain multiple sentences")
    if supplied is not None and not conventional(summary):
        reject("must use Conventional Commit syntax")
    if not conventional(summary):
        summary = "chore: " + summary
    if len(summary) > 120:
        reject("must be at most 120 characters including its type")
    return summary
