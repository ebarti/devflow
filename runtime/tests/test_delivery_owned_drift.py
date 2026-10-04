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


def test_public_authority_index_references_are_hash_bound_owned_readonly_evidence(ambient):
    module, drift, fixture, args, manifest, request, root = ambient
    inputs = json.loads(request.read_text())
    authority = Path(inputs["authority"]["path"])
    content = Path(inputs["content_indexes"][0]["path"])
    authority.chmod(0o644)
    content.chmod(0o644)
    receipt = _ack(ambient)
    frozen = (receipt.read_bytes(), manifest.read_bytes())
    authority.chmod(0o664)
    assert _run(fixture, "--repoint-owned-request", str(fixture["request"])).returncode != 0
    assert (receipt.read_bytes(), manifest.read_bytes()) == frozen


@pytest.mark.parametrize("operation", ["upgrade", "rollback"])
@pytest.mark.parametrize("target", ["skill", "pointer"])
def test_live_owned_authority_changed_during_public_guard_is_never_overwritten(
    ambient,
    monkeypatch,
    operation,
    target,
):
    module, drift, fixture, args, manifest, request, root = ambient
    _ack(ambient)
    if operation == "rollback":
        module.upgrade(*args)
    original = json.loads(manifest.read_text())
    saved_skill = fixture["skill"].read_bytes()
    public = drift.foreign_snapshot
    calls = 0

    def concurrent_owned_change(*values):
        nonlocal calls
        observed = public(*values)
        calls += 1
        if calls == (2 if operation == "upgrade" else 4):
            if target == "skill":
                fixture["skill"].write_text("Unrecognized concurrent owned skill bytes\n")
            else:
                registry = json.loads(fixture["registry"].read_text())
                registry["entries"]["devflow-local-delivery"]["transport"]["args"] = [
                    "unrecognized"
                ]
                _private(fixture["registry"], registry)
        return observed

    monkeypatch.setattr(drift, "foreign_snapshot", concurrent_owned_change)
    with pytest.raises(ValueError, match="live owned installation changed"):
        if operation == "upgrade":
            module.upgrade(*args)
        else:
            module.rollback(args[0], fixture["home"], manifest)
    if target == "skill":
        assert fixture["skill"].read_text() == "Unrecognized concurrent owned skill bytes\n"
    else:
        assert fixture["skill"].read_bytes() == saved_skill
        registry = json.loads(fixture["registry"].read_text())
        assert registry["entries"]["devflow-local-delivery"]["transport"]["args"] == [
            "unrecognized"
        ]
    assert {k: v for k, v in json.loads(manifest.read_text()).items() if k != "state"} == {
        k: v for k, v in original.items() if k != "state"
    }


def test_acknowledged_foreign_mcp_enabled_change_refuses_before_effects(ambient):
    module, drift, fixture, args, manifest, request, root = ambient
    _ack(ambient)
    registry = json.loads(fixture["registry"].read_text())
    registry["entries"]["unrelated"]["enabled"] = False
    _private(fixture["registry"], registry)
    frozen = (
        manifest.read_bytes(),
        fixture["skill"].read_bytes(),
        fixture["registry"].read_bytes(),
    )
    assert _run(fixture, "--repoint-owned-request", str(fixture["request"])).returncode != 0
    assert frozen == (
        manifest.read_bytes(),
        fixture["skill"].read_bytes(),
        fixture["registry"].read_bytes(),
    )


def _freeze_current_mcp_enabled(ambient, explicit):
    module, drift, fixture, args, manifest, request, root = ambient
    config = fixture["home"] / "config.toml"
    config.write_text(
        config.read_text()
        + '[mcp_servers.unrelated]\ncommand="/bin/true"\n'
        + ("enabled=true\n" if explicit else "")
    )
    inputs = json.loads(request.read_text())
    current = Path(inputs["current_snapshot"]["path"])
    observed = drift.foreign_snapshot(args[0], fixture["home"])
    _private(current, observed)
    inputs["current_snapshot"]["sha256"] = module.sha(current.read_bytes())
    prior = json.loads(Path(inputs["prior_snapshot"]["path"]).read_text())
    inputs["delta_sha256"] = module.seal(drift.delta(prior, observed))
    _private(request, inputs)


@pytest.mark.parametrize("explicit", [True, False])
def test_same_acknowledged_command_rollback_survives_enabled_default_serialization(
    ambient,
    explicit,
):
    module, drift, fixture, args, manifest, request, root = ambient
    _freeze_current_mcp_enabled(ambient, explicit)
    receipt = _ack(ambient)
    module.upgrade(*args)
    immutable = (receipt.read_bytes(), request.read_bytes(), drift.content_index(root))
    original = json.loads(manifest.read_text())
    fake = Path(args[0])
    effect = (
        ' c=pathlib.Path(os.environ["CODEX_HOME"])/"config.toml"\n'
        ' c.write_text(c.read_text().replace("enabled=true\\n", ""))\n'
    )
    if not explicit:
        effect = (
            ' c=pathlib.Path(os.environ["CODEX_HOME"])/"config.toml"\n'
            ' if "enabled=true\\n" not in c.read_text():\n'
            '  c.write_text(c.read_text().replace("[mcp_servers.unrelated]\\n", '
            '"[mcp_servers.unrelated]\\nenabled=true\\n"))\n'
        )
    fake.write_text(fake.read_text().replace(" crash=d.pop(", effect + " crash=d.pop("))
    assert module.rollback(args[0], fixture["home"], manifest)["state"] == "rolled_back"
    assert fixture["skill"].read_text() == "Inspected previous owned skill\n"
    assert module.rollback(args[0], fixture["home"], manifest)["state"] == "rolled_back"
    assert module.upgrade(*args)["state"] == "applied"
    assert module.upgrade(*args)["existing"]
    assert immutable == (receipt.read_bytes(), request.read_bytes(), drift.content_index(root))
    assert {k: v for k, v in json.loads(manifest.read_text()).items() if k != "state"} == {
        k: v for k, v in original.items() if k != "state"
    }


@pytest.mark.parametrize(
    "change",
    [
        "disabled",
        "public-disabled",
        "string",
        "integer",
        "other-setting",
    ],
)
def test_acknowledged_enabled_default_guard_rejects_semantic_or_type_drift(ambient, change):
    module, drift, fixture, args, manifest, request, root = ambient
    _freeze_current_mcp_enabled(ambient, True)
    _ack(ambient)
    module.upgrade(*args)
    config = fixture["home"] / "config.toml"
    if change == "public-disabled":
        registry = json.loads(fixture["registry"].read_text())
        registry["entries"]["unrelated"]["enabled"] = False
        _private(fixture["registry"], registry)
    elif change == "other-setting":
        config.write_text(
            config.read_text().replace('foreign_setting="priority"', 'foreign_setting="default"')
        )
    else:
        value = {"disabled": "false", "string": '"true"', "integer": "1"}[change]
        config.write_text(config.read_text().replace("enabled=true", "enabled=" + value))
    frozen = (
        manifest.read_bytes(),
        fixture["skill"].read_bytes(),
        fixture["registry"].read_bytes(),
        config.read_bytes(),
    )
    with pytest.raises(ValueError):
        module.rollback(args[0], fixture["home"], manifest)
    with pytest.raises(ValueError):
        module.upgrade(*args)
    assert frozen == (
        manifest.read_bytes(),
        fixture["skill"].read_bytes(),
        fixture["registry"].read_bytes(),
        config.read_bytes(),
    )


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


def test_explicit_successor_preserves_history_and_same_command_partial_rollback_reapply(ambient):
    module, drift, fixture, args, manifest, request, root = ambient
    first = _ack(ambient)
    assert _run(fixture, "--repoint-owned-request", str(fixture["request"])).returncode == 0
    # Proven interrupted rollback state: old pointer, new skill, applied journal.
    registry = json.loads(fixture["registry"].read_text())
    registry["entries"][module.NAME] = fixture["entry"]
    _private(fixture["registry"], registry)
    original = (
        first.read_bytes(),
        request.read_bytes(),
        fixture["request"].read_bytes(),
        manifest.read_bytes(),
    )
    successor = _successor(ambient, first)
    assert _run(fixture, "--rollback-owned-manifest", str(manifest)).returncode != 0
    admitted = _run(fixture, "--acknowledge-owned-drift-request", str(successor))
    assert admitted.returncode == 0, admitted.stderr
    response = json.loads(admitted.stdout)
    assert response["chain_length"] == 2 and response["max_acknowledgements"] == 4
    second = Path(response["acknowledgement"])
    assert json.loads(second.read_text())["predecessor_sha256"] == module.sha(first.read_bytes())
    assert original == (
        first.read_bytes(),
        request.read_bytes(),
        fixture["request"].read_bytes(),
        manifest.read_bytes(),
    )
    frozen = (
        drift.foreign_snapshot(args[0], fixture["home"]),
        drift.content_index(root),
        second.read_bytes(),
    )
    assert json.loads(_run(fixture, "--acknowledge-owned-drift-request", str(successor)).stdout)[
        "existing"
    ]
    assert _run(fixture, "--rollback-owned-manifest", str(manifest)).returncode == 0
    assert _run(fixture, "--repoint-owned-request", str(fixture["request"])).returncode == 0
    assert json.loads(_run(fixture, "--acknowledge-owned-drift-request", str(successor)).stdout)[
        "existing"
    ]
    # A historical request replay observes the latest acknowledged state.
    assert (
        json.loads(_run(fixture, "--acknowledge-owned-drift-request", str(request)).stdout)[
            "chain_length"
        ]
        == 2
    )
    assert frozen == (
        drift.foreign_snapshot(args[0], fixture["home"]),
        drift.content_index(root),
        second.read_bytes(),
    )
    assert {k: v for k, v in json.loads(manifest.read_text()).items() if k != "state"} == {
        k: v for k, v in json.loads(original[-1]).items() if k != "state"
    }


@pytest.mark.parametrize(
    "change",
    [
        "missing-predecessor",
        "wrong-predecessor",
        "journal-hash",
        "journal-state",
        "prior",
        "authority",
        "original-request",
        "owned-pointer",
        "owned-skill",
        "later-setting",
        "later-content",
    ],
)
def test_successor_rejects_bad_admission_before_receipt_or_effect(ambient, change):
    module, drift, fixture, args, manifest, first_request, root = ambient
    first = _ack(ambient)
    request = _successor(ambient, first)
    value = json.loads(request.read_text())
    if change == "missing-predecessor":
        value.pop("predecessor_sha256")
    elif change == "wrong-predecessor":
        value["predecessor_sha256"] = "0" * 64
    elif change == "journal-hash":
        value["expected_manifest_sha256"] = "0" * 64
    elif change == "journal-state":
        value["expected_manifest_state"] = "applied"
    elif change == "prior":
        value["prior_snapshot"] = value["current_snapshot"]
    elif change == "authority":
        value["authority"] = json.loads(first_request.read_text())["authority"]
    elif change == "original-request":
        value["original_request"]["sha256"] = "0" * 64
    elif change == "owned-pointer":
        registry = json.loads(fixture["registry"].read_text())
        registry["entries"][module.NAME]["transport"]["args"] = ["unexpected"]
        _private(fixture["registry"], registry)
    elif change == "owned-skill":
        fixture["skill"].write_text("unrecognized live owned update")
    elif change == "later-setting":
        host = fixture["home"] / "config.toml"
        host.write_text(
            host.read_text().replace('foreign_setting="default"', 'foreign_setting="priority"')
        )
    else:
        (root / "artifact-000.txt").write_text("unacknowledged current content")
    _private(request, value)
    frozen = (
        manifest.read_bytes(),
        first.read_bytes(),
        fixture["registry"].read_bytes(),
        fixture["skill"].read_bytes(),
    )
    rejected = _run(fixture, "--acknowledge-owned-drift-request", str(request))
    assert rejected.returncode != 0
    assert not (manifest.parent / "ambient-drift-acknowledgement-2.json").exists()
    assert frozen == (
        manifest.read_bytes(),
        first.read_bytes(),
        fixture["registry"].read_bytes(),
        fixture["skill"].read_bytes(),
    )


@pytest.mark.parametrize(
    "change",
    [
        "first-receipt",
        "first-request",
        "old-authority",
        "old-snapshot",
        "old-index",
        "second-receipt",
        "receipt-hardlink",
        "hole",
        "extra",
        "latest-setting",
        "latest-content",
    ],
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


def test_acknowledgements_have_a_finite_explicit_bound_without_renewal(ambient):
    module, drift, fixture, args, manifest, request, root = ambient
    prior = _ack(ambient)
    history = {prior: prior.read_bytes()}
    for number in range(2, drift.MAX_ACKNOWLEDGEMENTS + 1):
        successor = _successor(ambient, prior, number)
        admitted = _run(fixture, "--acknowledge-owned-drift-request", str(successor))
        assert admitted.returncode == 0, admitted.stderr
        prior = Path(json.loads(admitted.stdout)["acknowledgement"])
        history[prior] = prior.read_bytes()
    assert json.loads(_run(fixture, "--acknowledge-owned-drift-request", str(successor)).stdout)[
        "existing"
    ]
    exhausted = _successor(ambient, prior, drift.MAX_ACKNOWLEDGEMENTS + 1)
    rejected = _run(fixture, "--acknowledge-owned-drift-request", str(exhausted))
    assert rejected.returncode != 0 and "exhausted" in rejected.stderr
    assert not (manifest.parent / "ambient-drift-acknowledgement-5.json").exists()
    assert all(path.read_bytes() == content for path, content in history.items())


def test_successor_double_capture_rejects_changed_snapshot_without_receipt(ambient, monkeypatch):
    module, drift, fixture, args, manifest, first_request, root = ambient
    first = _ack(ambient)
    request = _successor(ambient, first)
    snapshot = drift.foreign_snapshot
    reads = 0

    def concurrent_change(codex, home):
        nonlocal reads
        reads += 1
        if reads == 2:
            host = home / "config.toml"
            host.write_text(host.read_text() + 'during_capture="changed"\n')
        return snapshot(codex, home)

    monkeypatch.setattr(drift, "foreign_snapshot", concurrent_change)
    frozen = manifest.read_bytes(), first.read_bytes(), fixture["skill"].read_bytes()
    with pytest.raises(ValueError, match="current public snapshot changed"):
        drift.acknowledge(args[0], fixture["home"], args[2], fixture["trusted"], args[4], request)
    assert reads == 2
    assert not (manifest.parent / "ambient-drift-acknowledgement-2.json").exists()
    assert frozen == (manifest.read_bytes(), first.read_bytes(), fixture["skill"].read_bytes())


def test_successor_authenticates_old_proofs_after_deliberate_plugin_cache_replacement(ambient):
    module, drift, fixture, args, manifest, first_request, root = ambient
    first = _ack(ambient)
    request = _successor(ambient, first)
    old_index = Path(json.loads(first_request.read_text())["content_indexes"][0]["path"])
    history = first.read_bytes(), first_request.read_bytes(), old_index.read_bytes()
    replacement = root.with_name("3")
    root.rename(replacement)
    (replacement / ".codex-plugin/plugin.json").write_text('{"name":"foreign","version":"3"}')
    registry = json.loads(fixture["registry"].read_text())
    registry["plugins"]["installed"][0]["version"] = "3"
    _private(fixture["registry"], registry)
    value = json.loads(request.read_text())
    current_path = Path(value["current_snapshot"]["path"])
    current = drift.foreign_snapshot(args[0], fixture["home"])
    _private(current_path, current)
    index_path = Path(value["content_indexes"][0]["path"])
    _private(index_path, drift.content_index(replacement))
    value["current_snapshot"]["sha256"] = module.sha(current_path.read_bytes())
    value["content_indexes"][0]["sha256"] = module.sha(index_path.read_bytes())
    prior = json.loads(Path(value["prior_snapshot"]["path"]).read_text())
    value["delta_sha256"] = module.seal(drift.delta(prior, current))
    _private(request, value)
    result = _run(fixture, "--acknowledge-owned-drift-request", str(request))
    assert result.returncode == 0, result.stderr
    assert _run(fixture, "--repoint-owned-request", str(fixture["request"])).returncode == 0
    assert _run(fixture, "--rollback-owned-manifest", str(manifest)).returncode == 0
    assert not root.exists()
    assert history == (first.read_bytes(), first_request.read_bytes(), old_index.read_bytes())
    assert drift.foreign_snapshot(args[0], fixture["home"]) == current


@pytest.mark.parametrize("point", ["lost-response", "append-hardlink"])
def test_successor_interrupted_acknowledgement_replays_without_new_authority(
    ambient, monkeypatch, point
):
    module, drift, fixture, args, manifest, first_request, root = ambient
    first = _ack(ambient)
    request = _successor(ambient, first)
    append, unlink = drift._append, Path.unlink

    def interrupted(path, value):
        append(path, value)
        raise SystemExit("lost client acknowledgement")

    def interrupted_cleanup(path, *args, **kwargs):
        if path.name.startswith(".ambient-append-"):
            raise SystemExit("lost client acknowledgement during production temporary cleanup")
        return unlink(path, *args, **kwargs)

    if point == "append-hardlink":
        monkeypatch.setattr(Path, "unlink", interrupted_cleanup)
    else:
        monkeypatch.setattr(drift, "_append", interrupted)
    frozen = manifest.read_bytes(), first.read_bytes()
    with pytest.raises(SystemExit, match="lost client acknowledgement"):
        drift.acknowledge(args[0], fixture["home"], args[2], args[3], args[4], request)
    second = manifest.parent / "ambient-drift-acknowledgement-2.json"
    retained = second.read_bytes()
    if point == "append-hardlink":
        assert second.stat().st_nlink == 2
    monkeypatch.setattr(drift, "_append", append)
    monkeypatch.setattr(Path, "unlink", unlink)
    repeated = _run(fixture, "--acknowledge-owned-drift-request", str(request))
    assert repeated.returncode == 0, repeated.stderr
    assert json.loads(repeated.stdout)["existing"]
    assert second.read_bytes() == retained and second.stat().st_nlink == 1
    assert not list(manifest.parent.glob(".ambient-append-*.tmp"))
    assert frozen == (manifest.read_bytes(), first.read_bytes())
    assert _run(fixture, "--repoint-owned-request", str(fixture["request"])).returncode == 0


@pytest.mark.parametrize("stage", ["absent", "lookalike", "different-inode"])
def test_external_receipt_hardlinks_do_not_authorize_stage_cleanup(ambient, stage):
    import os

    module, drift, fixture, args, manifest, request, root = ambient
    first = _ack(ambient)
    external = manifest.parent / "unrecognized-receipt-link"
    os.link(first, external)
    temporary = manifest.parent / (
        ".ambient-append-not-a-pid.tmp" if stage == "lookalike" else ".ambient-append-12345.tmp"
    )
    if stage == "lookalike":
        external.unlink()
        os.link(first, temporary)
    elif stage == "different-inode":
        _private(temporary, json.loads(first.read_text()))
    original = manifest.read_bytes(), first.read_bytes(), fixture["registry"].read_bytes()
    assert _run(fixture, "--acknowledge-owned-drift-request", str(request)).returncode != 0
    assert _run(fixture, "--repoint-owned-request", str(fixture["request"])).returncode != 0
    assert first.stat().st_nlink == 2
    if stage != "absent":
        assert temporary.exists()
    assert original == (manifest.read_bytes(), first.read_bytes(), fixture["registry"].read_bytes())


@pytest.mark.parametrize("target", ["journal-before", "journal-after", "live-before", "live-after"])
def test_owning_boolean_integer_history_and_pointer_changes_reject_without_effects(ambient, target):
    module, drift, fixture, args, manifest, first_request, root = ambient
    first = _ack(ambient)
    successor = _successor(ambient, first)
    admitted = _run(fixture, "--acknowledge-owned-drift-request", str(successor))
    assert admitted.returncode == 0, admitted.stderr
    if target.startswith("journal"):
        value = json.loads(manifest.read_text())
        value[target.split("-")[1]]["enabled"] = 1
        _private(manifest, value)
    else:
        registry = json.loads(fixture["registry"].read_text())
        entry = registry["entries"][module.NAME]
        entry["transport"]["args"] = [
            "--config",
            str(fixture["original"] if target == "live-before" else fixture["trusted"]),
        ]
        entry["enabled"] = 1
        _private(fixture["registry"], registry)
    retained = (
        manifest.read_bytes(),
        first.read_bytes(),
        fixture["registry"].read_bytes(),
        fixture["skill"].read_bytes(),
    )
    for flag, path in [
        ("--acknowledge-owned-drift-request", successor),
        ("--repoint-owned-request", fixture["request"]),
        ("--rollback-owned-manifest", manifest),
    ]:
        assert _run(fixture, flag, str(path)).returncode != 0
    assert retained == (
        manifest.read_bytes(),
        first.read_bytes(),
        fixture["registry"].read_bytes(),
        fixture["skill"].read_bytes(),
    )


@pytest.mark.parametrize("before,after", [(True, 1), (1, 1.0)])
def test_journal_comparison_is_typed_but_allows_only_state_progression(ambient, before, after):
    module, drift, fixture, args, manifest, first_request, root = ambient
    original = {"state": "unknown", "owning": {"value": before}}
    drift._same_journal(original, {**original, "state": "applied"})
    with pytest.raises(ValueError, match="original owned journal changed"):
        drift._same_journal(original, {"state": "applied", "owning": {"value": after}})


def test_successor_cannot_reuse_an_earlier_authority_receipt(ambient):
    module, drift, fixture, args, manifest, request, root = ambient
    first = _ack(ambient)
    second_request = _successor(ambient, first)
    result = _run(fixture, "--acknowledge-owned-drift-request", str(second_request))
    assert result.returncode == 0, result.stderr
    second = Path(json.loads(result.stdout)["acknowledgement"])
    third_request = _successor(ambient, second, 3)
    body = json.loads(third_request.read_text())
    body["authority"] = json.loads(request.read_text())["authority"]
    _private(third_request, body)
    assert _run(fixture, "--acknowledge-owned-drift-request", str(third_request)).returncode != 0
    assert not (manifest.parent / "ambient-drift-acknowledgement-3.json").exists()
