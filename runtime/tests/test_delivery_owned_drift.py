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
def ambient(canonicalized_unknown, monkeypatch):
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
    for index in range(158):
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


@pytest.mark.parametrize(
    "change",
    [
        "setting",
        "absence",
        "plugin",
        "content",
        "extra-file",
        "mode",
        "link",
        "authority",
        "request",
        "owned-config",
        "owned-pointer",
        "owned-skill",
    ],
)
def test_acknowledged_operation_rejects_every_later_change_before_effects(ambient, change):
    module, drift, fixture, args, manifest, request, root = ambient
    receipt = _ack(ambient)
    if change in {"setting", "absence"}:
        host_config = fixture["home"] / "config.toml"
        text = host_config.read_text()
        host_config.write_text(
            text.replace(
                'foreign_setting="priority"\n',
                "" if change == "absence" else 'foreign_setting="default"\n',
            )
        )
    elif change in {"content", "extra-file"}:
        (root / ("artifact-000.txt" if change == "content" else "new.txt")).write_text("changed")
    elif change == "mode":
        (root / "artifact-000.txt").chmod(0o700)
    elif change == "link":
        (root / "artifact-000.txt").unlink()
        (root / "artifact-000.txt").symlink_to(root / "artifact-001.txt")
    elif change == "authority":
        path = Path(json.loads(request.read_text())["authority"]["path"])
        _private(
            path,
            {"schema": "devflow-existing-installation-authority-v2", "authority_source": "changed"},
        )
    elif change == "request":
        _private(request, {**json.loads(request.read_text()), "delta_sha256": "0" * 64})
    elif change == "owned-config":
        _private(fixture["trusted"], {"execution_mode": "trusted-local", "roles": "changed"})
    elif change == "owned-skill":
        fixture["skill"].write_text("uninspected owned bytes")
    else:
        registry = json.loads(fixture["registry"].read_text())
        if change == "plugin":
            registry["plugins"]["installed"][0]["enabled"] = False
        else:
            registry["entries"]["devflow-local-delivery"]["transport"]["args"] = ["foreign"]
        _private(fixture["registry"], registry)
    frozen = (
        manifest.read_bytes(),
        receipt.read_bytes(),
        fixture["registry"].read_bytes(),
        fixture["skill"].read_bytes(),
    )
    assert _run(fixture, "--repoint-owned-request", str(fixture["request"])).returncode != 0
    assert _run(fixture, "--rollback-owned-manifest", str(manifest)).returncode != 0
    assert frozen == (
        manifest.read_bytes(),
        receipt.read_bytes(),
        fixture["registry"].read_bytes(),
        fixture["skill"].read_bytes(),
    )


def test_creation_rejects_stale_current_snapshot_and_changed_original_journal_without_receipt(
    ambient,
):
    module, drift, fixture, args, manifest, request, root = ambient
    original = manifest.read_bytes()
    (root / "artifact-000.txt").write_text("later ambient content")
    rejected = _run(fixture, "--acknowledge-owned-drift-request", str(request))
    assert rejected.returncode != 0
    assert not (manifest.parent / drift.SIDECAR).exists()
    assert manifest.read_bytes() == original
    (root / "artifact-000.txt").write_text("retained content 0\n")
    value = json.loads(manifest.read_text())
    value["new_skill_sha256"] = "0" * 64
    _private(manifest, value)
    assert _run(fixture, "--acknowledge-owned-drift-request", str(request)).returncode != 0
    assert not (manifest.parent / drift.SIDECAR).exists()


def test_interrupted_acknowledged_effect_resumes_same_command_and_keeps_original_errors(
    ambient, monkeypatch
):
    module, drift, fixture, args, manifest, request, root = ambient
    _ack(ambient)
    original = json.loads(manifest.read_text())
    write = module.write

    def interrupt(path, content):
        write(path, content)
        if path == fixture["skill"]:
            raise SystemExit("lost acknowledgement after owned skill write")

    monkeypatch.setattr(module, "write", interrupt)
    with pytest.raises(SystemExit):
        module.upgrade(*args)
    monkeypatch.setattr(module, "write", write)
    assert module.upgrade(*args)["state"] == "applied"
    assert len(json.loads(fixture["registry"].read_text())["adds"]) == 1
    assert {k: v for k, v in json.loads(manifest.read_text()).items() if k != "state"} == {
        k: v for k, v in original.items() if k != "state"
    }


@pytest.mark.parametrize("operation", ["reapply", "rollback"])
def test_foreign_change_during_pointer_effect_stops_before_skill_and_preserves_unknown_evidence(
    ambient,
    monkeypatch,
    operation,
):
    module, drift, fixture, args, manifest, request, root = ambient
    receipt = _ack(ambient)
    if operation == "reapply":
        module.rollback(args[0], fixture["home"], manifest)
    else:
        module.upgrade(*args)
    original = json.loads(manifest.read_text())
    skill = fixture["skill"].read_bytes()
    frozen_receipt = receipt.read_bytes()
    pointer = module.install_pointer

    def concurrent_change(*values):
        pointer(*values)
        config = fixture["home"] / "config.toml"
        config.write_text(
            config.read_text().replace('foreign_setting="priority"', 'foreign_setting="default"')
        )

    monkeypatch.setattr(module, "install_pointer", concurrent_change)
    with pytest.raises(ValueError, match="ambient current public snapshot changed"):
        if operation == "reapply":
            module.upgrade(*args)
        else:
            module.rollback(args[0], fixture["home"], manifest)
    assert fixture["skill"].read_bytes() == skill
    assert receipt.read_bytes() == frozen_receipt
    current = json.loads(manifest.read_text())
    assert {k: v for k, v in current.items() if k != "state"} == {
        k: v for k, v in original.items() if k != "state"
    }
    assert 'foreign_setting="default"' in (fixture["home"] / "config.toml").read_text()


def test_creation_refuses_false_prior_seal_without_receipt(ambient):
    module, drift, fixture, args, manifest, request, root = ambient
    inputs = json.loads(request.read_text())
    prior = Path(inputs["prior_snapshot"]["path"])
    before = json.loads(prior.read_text())
    before["settings"]["model"] = "forged original observation"
    _private(prior, before)
    inputs["prior_snapshot"]["sha256"] = module.sha(prior.read_bytes())
    _private(request, inputs)
    assert _run(fixture, "--acknowledge-owned-drift-request", str(request)).returncode != 0
    assert not (manifest.parent / drift.SIDECAR).exists()
