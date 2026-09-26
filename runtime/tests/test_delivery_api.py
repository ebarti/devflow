from __future__ import annotations

import json
import subprocess
from pathlib import Path

import httpx
import pytest

from devflow_temporal.delivery_api import create_app


def _git(path: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(path), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture
def api_fixture(tmp_path: Path) -> tuple[Path, dict]:
    source = tmp_path / "source"
    source.mkdir()
    (source / "README.md").write_text("Fixture\n")
    _git(source, "init", "-q")
    _git(source, "config", "user.name", "Fixture")
    _git(source, "config", "user.email", "fixture@example.invalid")
    _git(source, "add", "README.md")
    _git(source, "commit", "-qm", "initial")
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--bare", "-q", str(remote)], check=True)
    _git(source, "remote", "add", "origin", str(remote))
    root = Path(__file__).resolve().parents[2]
    config = {
        "version": 1,
        "tracking_db": str(tmp_path / "tracking.sqlite3"),
        "state_root": str(tmp_path / "state"),
        "helpers_dir": str(root / "skills" / "devflow" / "scripts"),
        "codex_bin": "/usr/bin/false",
        "provider": "fake",
        "dashboard_url": "http://127.0.0.1:18770",
        "repositories": {
            "fixture": {
                "source_path": str(source),
                "origin_url": str(remote),
                "github_repo": "example/fixture",
                "base_ref": "HEAD",
                "expected_base_sha": _git(source, "rev-parse", "HEAD"),
                "allowed_paths": ["README.md"],
            }
        },
        "roles": {
            role: {"model": "fake", "effort": "low"} for role in ("implement", "review", "verify")
        },
    }
    path = tmp_path / "service.json"
    path.write_text(json.dumps(config))
    request = {
        "command_id": "submit-1",
        "run_id": "run-1",
        "work_id": "work-1",
        "issue_url": "https://github.com/example/fixture/issues/3",
        "repository_key": "fixture",
        "goal": "Change fixture",
        "accepted_plan": "One bounded edit",
        "base_ref": "HEAD",
        "branch": "feat/fixture",
        "authorized_endpoint": "published_unmerged",
    }
    return path, request


@pytest.mark.asyncio
async def test_local_api_auth_csrf_submit_replay_and_conflict(api_fixture):
    path, request = api_fixture
    app = create_app(path)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as browser:
        assert (await browser.get("/api/session")).json() == {"authenticated": False}
        assert (await browser.get("/api/runs")).status_code == 401
        token = (
            (Path(json.loads(path.read_text())["state_root"]) / "service-token").read_text().strip()
        )
        assert (
            await browser.post(
                "/api/session", json={"token": token}, headers={"Origin": "http://evil.local"}
            )
        ).status_code == 403
        login = await browser.post(
            "/api/session", json={"token": token}, headers={"Origin": "http://127.0.0.1:18770"}
        )
        assert login.status_code == 200
        csrf = login.json()["csrf_token"]
        assert browser.cookies.get("devflow_session")
        assert (
            await browser.post(
                "/api/runs", json=request, headers={"Origin": "http://127.0.0.1:18770"}
            )
        ).status_code == 403
        headers = {"Origin": "http://127.0.0.1:18770", "X-Devflow-CSRF": csrf}
        first = await browser.post("/api/runs", json=request, headers=headers)
        assert first.status_code == 200
        assert first.json()["run_id"] == "run-1"
        assert (
            await browser.post("/api/runs", json=request, headers=headers)
        ).json() == first.json()
        assert (
            await browser.post("/api/runs", json={**request, "goal": "changed"}, headers=headers)
        ).status_code == 409
        detail = (await browser.get("/api/runs/run-1")).json()
        assert detail["run"]["phase"] == "accepted"
        assert detail["events"][0]["type"] == "accepted"
        assert detail["evidence"] == []
        assert (await browser.get("/api/runs/run-1/evidence/not-indexed")).status_code == 404
