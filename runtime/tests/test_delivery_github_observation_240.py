"""A feature assessment cannot mutate GitHub or infer missing pages."""
import json
from types import SimpleNamespace

import pytest

from devflow_temporal.delivery_github_contract import GitHubDelivery, bounded_github_observation


def test_github_observation_is_get_only_exact_host_and_repository(monkeypatch):
    calls = []

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(returncode=0, stdout="{}", stderr="")

    monkeypatch.setattr("devflow_temporal.delivery_github_contract.subprocess.run", run)
    with bounded_github_observation("owner/repo"):
        assert GitHubDelivery().api("repos/owner/repo/issues/1") == {}
        for target in ("repos/owner/other/issues/1", "repos/owner/repo/../../user",
                       "repos/owner/repo/%2e%2e/user", "repos/owner/repo/%2Fuser",
                       "repos/owner/repo//issues", "repos/owner/repo/./issues"):
            with pytest.raises(ValueError):
                GitHubDelivery().api(target)
        with pytest.raises(ValueError):
            GitHubDelivery().api("repos/owner/repo/issues/1", method="PATCH", body={})
    assert len(calls) == 1
    argv, kwargs = calls[0]
    assert argv[2:4] == ["--hostname", "github.com"]
    assert 0 < kwargs["timeout"] <= 15


def test_snapshot_observation_and_admission_pin_github_host(monkeypatch):
    calls = []
    monkeypatch.setenv("GH_HOST", "enterprise.example")

    def run(argv, **kwargs):
        calls.append(argv)
        endpoint = argv[-1]
        if endpoint == "repos/owner/repo":
            value = {"full_name": "owner/repo", "node_id": "R1", "default_branch": "main"}
        elif endpoint == "repos/owner/repo/issues/1":
            value = {"html_url": "https://github.com/owner/repo/issues/1", "node_id": "I1",
                     "id": 1, "title": "Feature", "state": "open", "updated_at": "now",
                     "labels": []}
        elif endpoint == "repos/owner/repo/issues/1/parent":
            return SimpleNamespace(returncode=1, stdout="", stderr="HTTP 404")
        elif endpoint.startswith("repos/owner/repo/issues/1/sub_issues?"):
            value = []
        else:
            raise AssertionError(endpoint)
        return SimpleNamespace(returncode=0, stdout=json.dumps(value), stderr="")

    monkeypatch.setattr("devflow_temporal.delivery_github_contract.subprocess.run", run)
    github = GitHubDelivery()
    with bounded_github_observation("owner/repo"):
        preview = github.snapshot("https://github.com/owner/repo/issues/1", "owner/repo")
    actual = github.snapshot("https://github.com/owner/repo/issues/1", "owner/repo")
    assert preview == actual
    assert all(argv[2:4] == ["--hostname", "github.com"] for argv in calls)
