"""Repository-level permission for new admissions, separate from frozen run policy."""

from __future__ import annotations

import json


def initialize(db) -> None:
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_repository_access (
        scope TEXT PRIMARY KEY, revision INTEGER NOT NULL, allowed_json TEXT NOT NULL
    )""")


def repository_access(store, db) -> dict:
    # Config overlays of one service share this setting; other service owners
    # using the same tracking database retain their own admission permissions.
    scope = str(store.config.state_root.resolve())
    registered = {
        item["github_repo"].casefold(): item["github_repo"]
        for item in store.config.raw["repositories"].values()
    }
    row = db.execute(
        "SELECT revision,allowed_json FROM delivery_repository_access WHERE scope=?", (scope,)
    ).fetchone()
    allowed = set(json.loads(row[1])) if row else set(registered)
    return {
        "revision": row[0] if row else 0,
        "repositories": [
            {"name": name, "allowed": key in allowed}
            for key, name in sorted(registered.items())
        ],
    }


def read_repository_access(store) -> dict:
    with store._connect() as db:
        return repository_access(store, db)


def save_repository_access(store, payload: dict) -> dict:
    if (not isinstance(payload, dict)
            or set(payload) != {"expected_revision", "allowed_repositories"}
            or type(payload["expected_revision"]) is not int
            or not isinstance(payload["allowed_repositories"], list)
            or any(not isinstance(name, str) for name in payload["allowed_repositories"])):
        raise ValueError("invalid repository access settings")
    requested = {name.casefold() for name in payload["allowed_repositories"]}
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        current = repository_access(store, db)
        if current["revision"] != payload["expected_revision"]:
            raise ValueError("Repository access changed. Refresh Settings before saving again.")
        registered = {item["name"].casefold() for item in current["repositories"]}
        if requested - registered:
            raise ValueError("repository is not registered with this service")
        db.execute(
            """INSERT INTO delivery_repository_access VALUES (?,?,?)
               ON CONFLICT(scope) DO UPDATE SET
               revision=excluded.revision, allowed_json=excluded.allowed_json""",
            (str(store.config.state_root.resolve()), current["revision"] + 1,
             json.dumps(sorted(requested))),
        )
        return repository_access(store, db)


def require_repository_access(store, db, github_repo: str) -> None:
    if not any(item["name"].casefold() == github_repo.casefold() and item["allowed"]
               for item in repository_access(store, db)["repositories"]):
        raise ValueError("Repository is disabled in Settings. Enable it before starting a new run.")
