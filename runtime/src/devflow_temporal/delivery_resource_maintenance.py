"""Prune disposable caches and already-settled retained worktrees.

Uses recorded run ownership, preserves the original cleanup receipts and never
dispatches a role, changes a run outcome or deletes sessions/evidence.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .contracts import canonical_json
from .delivery_config import DeliveryConfig
from .delivery_resources import RunResources
from .delivery_store import DeliveryStore


def prune(store: DeliveryStore, run_id: str) -> dict:
    with store._connect() as db:
        row = db.execute("SELECT execution_state,cleanup,checks_json "
                         "FROM delivery_runs WHERE run_id=?",
                         (run_id,)).fetchone()
        if (not row or row["execution_state"] not in {"terminal", "blocked", "cancelled"}
                or row["cleanup"] != "confirmed"):
            raise ValueError("pruning requires a settled terminal run with confirmed cleanup")
    spec = store.effective_spec(run_id)
    receipt = json.loads(row["checks_json"] or "{}").get("resource_cleanup", {})
    expected = str(Path(spec["state_dir"]) / "resources/finalization.json")
    if (receipt.get("receipt") != expected or not receipt.get("receipt_sha256")
            or receipt.get("state") != "confirmed"):
        raise ValueError("pruning requires the run's recorded confirmed finalization receipt")
    return RunResources(spec, read_only=True).prune_retained(
        expected_receipt_sha256=receipt["receipt_sha256"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--run", required=True, action="append", dest="runs")
    args = parser.parse_args()
    store = DeliveryStore(DeliveryConfig.load(args.config))
    for run_id in args.runs:
        print(canonical_json(prune(store, run_id)), flush=True)


if __name__ == "__main__":
    main()
