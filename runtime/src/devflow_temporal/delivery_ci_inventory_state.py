"""Display CI evidence without conferring another owner's gate authority."""

from __future__ import annotations


def assessment(state, head, *, source, complete, required, present=None, execution_only=False):
    """Report the four independent questions; unknown never means eligible."""
    return {
        "execution": {
            "state": "observed" if present is True else "absent" if present is False else "unknown",
            "source": source,
            "head": head,
            "read_complete": complete,
            "required_checks": sorted(required),
        },
        "delivery_policy": {"state": state, "required_checks": sorted(required)},
        "pr_eligibility": {
            "state": "not_established" if execution_only else "unverified",
            "reason": (
                "Dispatched execution evidence does not establish required PR checks."
                if execution_only else
                "Check observations do not evaluate the current GitHub ruleset."
            ),
        },
        "merge_readiness": {
            "state": "unverified",
            "reason": "The merge owner must verify the current candidate and all merge gates.",
        },
    }
