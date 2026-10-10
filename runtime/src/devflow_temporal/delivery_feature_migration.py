"""Import legacy execution custody before enabling GitHub feature delivery.

Run against every participating runtime while admission is stopped. This does
not adopt, close, merge, or pick between historical PRs. It preserves their exact
receipts as admission fences and leaves all old execution inputs untouched.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from .contracts import canonical_json
from .delivery_config import DeliveryConfig
from .delivery_execution_registry import ExecutionRegistry, OwnershipConflict, now
from .delivery_feature_execution import registry_path


def migrate(configs):
    if not configs:
        raise ValueError("at least one runtime configuration is required")
    paths = {registry_path(config) for config in configs}
    if len(paths) != 1:
        raise ValueError("all runtimes must use the same execution registry")
    records, stores = {}, set()
    for config in configs:
        path = config.tracking_db.resolve()
        stores.add(str(path))
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as db:
            db.row_factory = sqlite3.Row
            for row in db.execute("SELECT * FROM delivery_runs"):
                spec = json.loads(row["request_json"])
                if spec.get("feature_delivery"):
                    continue
                if row["outcome"] is None:
                    raise OwnershipConflict("legacy execution is still active: " + row["run_id"])
                if row["pr_json"]:
                    key = row["issue_url"].rstrip("/").casefold()
                    records.setdefault(key, []).append(
                        {
                            "run_id": row["run_id"],
                            "store_path": str(path),
                            "publication": json.loads(row["pr_json"]),
                        }
                    )
    shared = ExecutionRegistry(paths.pop())
    with shared.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        for issue, receipts in records.items():
            prior = db.execute(
                "SELECT receipts_json FROM execution_legacy_custody WHERE issue_url=?", (issue,)
            ).fetchone()
            by_execution = {
                (item["store_path"], item["run_id"]): item
                for item in (json.loads(prior[0]) if prior else [])
            }
            for item in receipts:
                by_execution[(item["store_path"], item["run_id"])] = item
            db.execute(
                "INSERT OR REPLACE INTO execution_legacy_custody VALUES (?,?)",
                (issue, canonical_json([by_execution[key] for key in sorted(by_execution)])),
            )
        for path in sorted(stores):
            db.execute(
                "INSERT OR REPLACE INTO execution_migrated_stores VALUES (?,?)", (path, now())
            )
    return {"stores": len(stores), "issues_with_legacy_custody": len(records)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", action="append", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(migrate([DeliveryConfig.load(path) for path in args.config])))


if __name__ == "__main__":
    main()
