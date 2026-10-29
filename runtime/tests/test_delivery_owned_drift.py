"""Explicit ambient acknowledgement preserves historical authority and current foreign bytes."""

from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest
from test_delivery_installation import (
    _private,
    _run,
)
from test_delivery_installation import (
    canonicalized_unknown as _unknown,
)
from test_delivery_installation import (
    installation as _installation,
)

installation = _installation
canonicalized_unknown = _unknown


@pytest.fixture
def ambient(canonicalized_unknown, monkeypatch, request):
    module, fixture, args, manifest, failed = canonicalized_unknown
    drift = importlib.import_module("owned_drift")
    # The predecessor reproducer replaces the owning seal while importing its
    # runtime dependencies. Restore that injected dependency for these checks.
    monkeypatch.setattr(drift, "unrelated_seal", module.unrelated_seal)
    registry = json.loads(fixture["registry"].read_text())
    plugin = {
        "pluginId": "foreign@remote",
        "name": "foreign",
        "marketplaceName": "remote",
        "version": "1",
        "installed": True,
        "enabled": True,
        "source": {"source": "remote", "id": "retained-source"},
        "installPolicy": "DEFAULT",
    }
    registry["plugins"] = {"installed": [plugin], "available": []}
    _private(fixture["registry"], registry)
    codex = Path(args[0])
    text = codex.read_text().replace(
        'd=json.loads(p.read_text()); args=sys.argv[2:]; name="devflow-local-delivery"\n',
        'd=json.loads(p.read_text()); args=sys.argv[2:]; name="devflow-local-delivery"\n'
        'if sys.argv[1]=="plugin":\n'
        ' if args==["list","--json"]: print(json.dumps(d["plugins"]))\n'
        ' elif args==["marketplace","list"]: print("No plugin marketplaces in scope.")\n'
        " else: sys.exit(7)\n"
        " sys.exit(0)\n",
    )
    codex.write_text(text)
    before = drift.foreign_snapshot(args[0], fixture["home"])
    registry["plugins"]["installed"][0]["version"] = "2"
    _private(fixture["registry"], registry)
    host_config = fixture["home"] / "config.toml"
    host_config.write_text('foreign_setting="priority"\n' + host_config.read_text())
    root = fixture["home"] / "plugins/cache/remote/foreign/2"
    package = root / ".codex-plugin/plugin.json"
    package.parent.mkdir(parents=True)
    package.write_text('{"name":"foreign","version":"2"}')
    # Broad content indexing is exercised once; field refusals need two files.
    for index in range(getattr(request, "param", 2)):
        (root / f"artifact-{index:03}.txt").write_text(f"retained content {index}\n")
    after = drift.foreign_snapshot(args[0], fixture["home"])
    evidence = manifest.parent / "inspected-evidence"
    authority = evidence / "authority.json"
    prior_path, current_path, index_path = [
        evidence / name for name in ("prior.json", "current.json", "content.json")
    ]
    _private(
        authority,
        {
            "decision_owner": "main task",
            "new_user_approval": False,
            "authority_source": "Existing root installation authority",
        },
    )
    _private(prior_path, before)
    _private(current_path, after)
    _private(index_path, drift.content_index(root))

    def reference(path):
        return {"path": str(path), "sha256": module.sha(path.read_bytes())}

    request = evidence / "acknowledgement-request.json"
    _private(
        request,
        {
            "command_id": json.loads(fixture["request"].read_text())["command_id"],
            "original_request": reference(fixture["request"]),
            "original_manifest_sha256": module.sha(manifest.read_bytes()),
            "original_unrelated_sha256": failed["unrelated_sha256"],
            "authority": reference(authority),
            "prior_snapshot": reference(prior_path),
            "current_snapshot": reference(current_path),
            "delta_sha256": module.seal(drift.delta(before, after)),
            "content_indexes": [reference(index_path)],
        },
    )
    return module, drift, fixture, args, manifest, request, root


def _ack(ambient):
    module, drift, fixture, args, manifest, request, root = ambient
    result = _run(fixture, "--acknowledge-owned-drift-request", str(request))
    assert result.returncode == 0, result.stderr
    return Path(json.loads(result.stdout)["acknowledgement"])


@pytest.mark.parametrize("ambient", [158], indirect=True)
def test_public_immutable_acknowledgement_replay_rollback_reapply_preserves_current_foreign_state(
    ambient,
):
    module, drift, fixture, args, manifest, request, root = ambient
    original = manifest.read_bytes()
    original_request = fixture["request"].read_bytes()
    with pytest.raises(ValueError, match="changed"):
        module.upgrade(*args)
    receipt = _ack(ambient)
    assert manifest.read_bytes() == original
    frozen = (request.read_bytes(), receipt.read_bytes(), drift.content_index(root))
    assert json.loads(_run(fixture, "--acknowledge-owned-drift-request", str(request)).stdout)[
        "existing"
    ]
    after = drift.foreign_snapshot(args[0], fixture["home"])
    first = _run(fixture, "--repoint-owned-request", str(fixture["request"]))
    assert first.returncode == 0, first.stderr
    assert json.loads(first.stdout)["existing"] is False
    assert json.loads(_run(fixture, "--repoint-owned-request", str(fixture["request"])).stdout)[
        "existing"
    ]
    assert len(json.loads(fixture["registry"].read_text())["adds"]) == 1
    rolled = _run(fixture, "--rollback-owned-manifest", str(manifest))
    assert rolled.returncode == 0, rolled.stderr
    assert fixture["skill"].read_text() == "Inspected previous owned skill\n"
    reapplied = _run(fixture, "--repoint-owned-request", str(fixture["request"]))
    assert reapplied.returncode == 0, reapplied.stderr
    assert drift.foreign_snapshot(args[0], fixture["home"]) == after
    assert (request.read_bytes(), receipt.read_bytes(), drift.content_index(root)) == frozen
    assert fixture["request"].read_bytes() == original_request
    assert {k: v for k, v in json.loads(manifest.read_text()).items() if k != "state"} == {
        k: v for k, v in json.loads(original).items() if k != "state"
    }


def _successor(ambient, predecessor, number=2):
    module, drift, fixture, args, manifest, first_request, root = ambient
    previous = json.loads(Path(json.loads(predecessor.read_text())["request_path"]).read_text())
    before = json.loads(Path(previous["current_snapshot"]["path"]).read_text())
    host = fixture["home"] / "config.toml"
    host.write_text(
        host.read_text().replace('foreign_setting="priority"', 'foreign_setting="default"')
        if number == 2
        else host.read_text() + f'foreign_{number}="observed"\n'
    )
    evidence = first_request.parent / f"successor-{number}"
    authority, current, index = [
        evidence / name for name in ("authority.json", "current.json", "content.json")
    ]
    _private(
        authority,
        {
            "decision_owner": "main task",
            "new_user_approval": False,
            "authority_source": f"Existing installation authority continuation {number}",
        },
    )
    after = drift.foreign_snapshot(args[0], fixture["home"])
    _private(current, after)
    _private(index, drift.content_index(root))

    def reference(path):
        return {"path": str(path), "sha256": module.sha(path.read_bytes())}

    first = json.loads(first_request.read_text())
    request = evidence / "request.json"
    _private(
        request,
        {
            **first,
            "authority": reference(authority),
            "prior_snapshot": previous["current_snapshot"],
            "current_snapshot": reference(current),
            "delta_sha256": module.seal(drift.delta(before, after)),
            "content_indexes": [reference(index)],
            "predecessor_sha256": module.sha(predecessor.read_bytes()),
            "expected_manifest_sha256": module.sha(manifest.read_bytes()),
            "expected_manifest_state": json.loads(manifest.read_text())["state"],
        },
    )
    return request


@pytest.mark.parametrize(
    "change",
    ["first-receipt", "latest-content"],
)
def test_all_history_and_latest_material_are_guarded_after_successor(ambient, change):
    module, drift, fixture, args, manifest, request, root = ambient
    first = _ack(ambient)
    successor = _successor(ambient, first)
    admitted = _run(fixture, "--acknowledge-owned-drift-request", str(successor))
    assert admitted.returncode == 0, admitted.stderr
    second = Path(json.loads(admitted.stdout)["acknowledgement"])
    if change in {"first-receipt", "first-request", "second-receipt"}:
        path = {"first-receipt": first, "first-request": request, "second-receipt": second}[change]
        path.write_bytes(path.read_bytes() + b" ")
    elif change in {"old-authority", "old-snapshot", "old-index"}:
        old = json.loads(request.read_text())
        if change == "old-index":
            ref = old["content_indexes"][0]
        else:
            ref = old[{"old-authority": "authority", "old-snapshot": "current_snapshot"}[change]]
        path = Path(ref["path"])
        path.write_bytes(path.read_bytes() + b" ")
    elif change == "receipt-hardlink":
        import os

        os.link(second, second.with_name("unrecognized-link"))
    elif change == "hole":
        first.unlink()
    elif change == "extra":
        _private(manifest.parent / "ambient-drift-acknowledgement-5.json", {})
    elif change == "latest-setting":
        host = fixture["home"] / "config.toml"
        host.write_text(host.read_text() + 'later="change"\n')
    else:
        (root / "artifact-000.txt").write_text("new content")
    frozen = manifest.read_bytes(), fixture["registry"].read_bytes(), fixture["skill"].read_bytes()
    assert _run(fixture, "--repoint-owned-request", str(fixture["request"])).returncode != 0
    assert _run(fixture, "--rollback-owned-manifest", str(manifest)).returncode != 0
    assert frozen == (
        manifest.read_bytes(),
        fixture["registry"].read_bytes(),
        fixture["skill"].read_bytes(),
    )
