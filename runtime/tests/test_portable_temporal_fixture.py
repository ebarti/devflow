"""The official metadata request keeps authentication and diagnostics bounded."""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import portable_temporal_fixture as fixture
import pytest
from portable_temporal_fixture import fetch_release_metadata


@pytest.mark.parametrize("token", [None, "ghs_TEST_ONLY_DO_NOT_USE"])
def test_metadata_auth_is_only_passed_on_stdin_and_not_in_child_environment(
    tmp_path, monkeypatch, token
):
    canary = "ghs_TEST_ONLY_DO_NOT_USE"
    monkeypatch.setenv("DEVFLOW_TEMPORAL_GITHUB_TOKEN", canary)
    calls = []

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        Path(argv[argv.index("--output") + 1]).write_text('{"assets": []}')
        return SimpleNamespace(returncode=0, stdout="HTTP/2 200\n\n__DEVFLOW_HTTP_STATUS__200\n")

    monkeypatch.setattr(subprocess, "run", run)
    target = tmp_path / "metadata.json"
    fetch_release_metadata("temporalio/sdk-java", "v1.32.0", target, token)
    argv, kwargs = calls[0]
    assert target.read_text() == '{"assets": []}'
    assert canary not in " ".join(argv)
    assert "DEVFLOW_TEMPORAL_GITHUB_TOKEN" not in kwargs["env"]
    assert "--location" not in argv and argv[1] == "--disable"
    if token:
        assert argv[argv.index("--header") + 1] == "@-"
        assert kwargs["input"] == f"Authorization: Bearer {canary}\n"
    else:
        assert "--header" not in argv and kwargs["input"] == ""


def test_metadata_failure_reports_only_safe_status_and_rate_limit_facts(
    tmp_path, monkeypatch, capsys
):
    canary = "ghs_TEST_ONLY_DO_NOT_USE"

    def run(argv, **_kwargs):
        Path(argv[argv.index("--output") + 1]).write_text(
            '{"message":"API rate limit exceeded for private address", "other":"' + canary + '"}'
        )
        return SimpleNamespace(
            returncode=22,
            stdout=("HTTP/2 403\nX-RateLimit-Remaining: 0\nX-RateLimit-Reset: 12345\n"
                    f"X-GitHub-Request-Id: {canary}\nX-Unknown: {canary}\n"
                    "\n__DEVFLOW_HTTP_STATUS__403\n"),
            stderr=f"sensitive error body {canary}",
        )

    monkeypatch.setattr(subprocess, "run", run)
    target = tmp_path / "metadata.json"
    with pytest.raises(RuntimeError) as failure:
        fetch_release_metadata("temporalio/sdk-java", "v1.32.0", target, canary)
    message = str(failure.value)
    assert "HTTP 403" in message and "curl exit 22" in message
    assert "x-ratelimit-remaining=0" in message and "x-ratelimit-reset=12345" in message
    assert "api-error=rate-limit" in message and "private address" not in message
    captured = capsys.readouterr()
    assert canary not in message and canary not in captured.out and canary not in captured.err
    assert not target.exists() and not list(tmp_path.iterdir())


def test_metadata_redirect_fails_without_forwarding_credentials(tmp_path, monkeypatch):
    def run(argv, **_kwargs):
        assert "--location" not in argv
        return SimpleNamespace(returncode=0, stdout="HTTP/2 302\n\n__DEVFLOW_HTTP_STATUS__302\n")

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(RuntimeError, match="HTTP 302"):
        fetch_release_metadata("temporalio/cli", "v1.9.1", tmp_path / "metadata.json", "test-token")
    assert not list(tmp_path.iterdir())


def test_metadata_cannot_persist_an_echoed_credential(tmp_path, monkeypatch):
    canary = "ghs_TEST_ONLY_DO_NOT_USE"

    def run(argv, **_kwargs):
        Path(argv[argv.index("--output") + 1]).write_text(canary)
        return SimpleNamespace(returncode=0, stdout="HTTP/2 200\n\n__DEVFLOW_HTTP_STATUS__200\n")

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(ValueError, match="unexpectedly contains its credential") as failure:
        fetch_release_metadata("temporalio/cli", "v1.9.1", tmp_path / "metadata.json", canary)
    assert canary not in str(failure.value)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("token", ["bad\nheader", "", "a" * 4097])
def test_metadata_rejects_malformed_credentials_before_launch(tmp_path, monkeypatch, token):
    monkeypatch.setattr(subprocess, "run", lambda *_a, **_k: pytest.fail("must not launch"))
    with pytest.raises(ValueError, match="credential is malformed"):
        fetch_release_metadata("temporalio/cli", "v1.9.1", tmp_path / "metadata.json", token)
    assert not list(tmp_path.iterdir())


def test_metadata_rejects_unpinned_repository_before_launch(tmp_path, monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *_a, **_k: pytest.fail("must not launch"))
    with pytest.raises(ValueError, match="target is not pinned"):
        fetch_release_metadata("untrusted/repo", "v1.9.1", tmp_path / "metadata.json", "token")
    assert not list(tmp_path.iterdir())


def test_build_removes_credential_before_dependency_setup(tmp_path, monkeypatch):
    canary = "ghs_TEST_ONLY_DO_NOT_USE"
    monkeypatch.setenv("DEVFLOW_TEMPORAL_GITHUB_TOKEN", canary)
    monkeypatch.setattr(fixture.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(fixture.platform, "machine", lambda: "arm64")
    calls = []

    def run(argv, **_kwargs):
        calls.append(argv)
        assert "DEVFLOW_TEMPORAL_GITHUB_TOKEN" not in fixture.os.environ

    def fetch(_repository, _tag, _path, token):
        assert token == canary
        raise RuntimeError("stop after verified handoff")

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(fixture, "fetch_release_metadata", fetch)
    with pytest.raises(RuntimeError, match="verified handoff"):
        fixture.build(tmp_path / "fixture")
    assert len(calls) == 1 and calls[0][:2] == ["uv", "sync"]
