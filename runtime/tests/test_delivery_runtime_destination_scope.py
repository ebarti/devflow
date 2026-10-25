"""Shared installed objects cannot merge or erase a run's frozen native grants."""

from copy import deepcopy

import pytest

from devflow_temporal import delivery_runtime_installation as owner
from devflow_temporal import delivery_runtime_location as location


def measured(tmp_path, *, browser=False):
    old, new = tmp_path / "legacy", tmp_path / "permanent"
    relative = "runtime/.venv/lib/python3.12/site-packages/codex_cli_bin/bin/codex"
    before = {
        "execution_mode": "trusted-local",
        "platform": "native-macos",
        "architecture": "arm64",
        "os_version": "15.0",
        "python": "/owned/python3.12",
        "python_sha256": "1" * 64,
        "codex_bin": str(old / relative),
        "codex_bin_sha256": "2" * 64,
        "protected_commands": owner._protected(old, old / relative),
        "packages": {"openai-codex": "0.160.0"},
        "runtime_dependencies": {"lock_sha256": "3" * 64},
        "runtime_payload_sha256": "4" * 64,
        "config_overrides": {"agents": False},
        "toolchain_roots": ["/owned/node"],
        "package_manager_cache": "/owned/corepack",
        "browser_read_roots": ["/owned/ms-playwright"] if browser else [],
    }
    original = {
        "policy": {
            "host_sandbox": "trusted-local",
            "codex_bin": before["codex_bin"],
            "native_identity": before,
            "toolchain_roots": before["toolchain_roots"],
            "package_manager_cache": before["package_manager_cache"],
            "browser_qa": {"read_roots": before["browser_read_roots"]} if browser else None,
        }
    }
    identity = {
        **deepcopy(before),
        "codex_bin": str(new / relative),
        "protected_commands": owner._protected(new, new / relative),
        "runtime_payload_sha256": "5" * 64,
    }
    directory = {"device": 1, "inode": 2, "volume_uuid": "owned", "birthtime": 3}
    result = {
        "root": str(new),
        "root_identity": deepcopy(directory),
        "environment_identity": {**directory, "inode": 4},
        "identity": identity,
        "revision": "6" * 40,
        "tree": "7" * 40,
        "payload_sha256": "5" * 64,
        "dependencies_sha256": "8" * 64,
        "helpers": {"state.py": "9" * 64},
    }
    return result, {"original_spec": original, "installed_native_identity": deepcopy(identity)}


def test_shared_physical_destination_preserves_two_different_complete_run_authorities(tmp_path):
    one, first = measured(tmp_path)
    two, second = measured(tmp_path, browser=True)
    shared = owner.shared_destination(one)
    assert shared == owner.shared_destination(two)
    assert owner.same_destination(shared, two)
    assert owner.scoped_identity(shared, first)["browser_read_roots"] == []
    assert owner.scoped_identity(shared, second)["browser_read_roots"] == ["/owned/ms-playwright"]
    assert one["identity"] == first["installed_native_identity"]
    assert two["identity"] == second["installed_native_identity"]
    assert not owner.same_destination(one, two)  # Legacy full grants remain strict.


@pytest.mark.parametrize("field", sorted(owner.RUN_NATIVE_SCOPE))
@pytest.mark.parametrize("drift", ["policy", "measurement", "missing"])
def test_per_run_scope_cannot_drift_or_inherit_a_peer_grant(tmp_path, field, drift):
    physical, item = measured(tmp_path, browser=True)
    shared = owner.shared_destination(physical)
    if drift == "measurement":
        item["installed_native_identity"][field] = "/foreign" if field.endswith("cache") else []
    elif drift == "missing":
        item.pop("installed_native_identity")
    else:
        policy = item["original_spec"]["policy"]
        if field == "browser_read_roots":
            policy["browser_qa"] = None
        else:
            policy[field] = "/foreign" if field.endswith("cache") else []
    with pytest.raises(ValueError, match="scope|full per-run"):
        owner.scoped_identity(shared, item)


@pytest.mark.parametrize(
    "field", ["python_sha256", "codex_bin_sha256", "packages", "config_overrides",
              "runtime_dependencies", "protected_commands", "runtime_payload_sha256"]
)
def test_shared_projection_keeps_every_physical_identity_field_pinned(tmp_path, field):
    physical, _ = measured(tmp_path)
    expected = owner.shared_destination(physical)
    physical["identity"][field] = "foreign"
    assert not owner.same_destination(expected, physical)


@pytest.mark.parametrize("field", ["root_identity", "environment_identity", "helpers"])
def test_shared_projection_keeps_object_and_helper_custody(tmp_path, field):
    physical, _ = measured(tmp_path)
    expected = owner.shared_destination(physical)
    if field == "helpers":
        physical[field]["state.py"] = "foreign"
    else:
        physical[field]["inode"] += 1
    assert not owner.same_destination(expected, physical)


def test_late_consumer_requires_full_current_run_identity_after_shared_comparison(
    tmp_path, monkeypatch
):
    physical, item = measured(tmp_path)
    shared = owner.shared_destination(physical)
    monkeypatch.setattr(owner, "destination", lambda _spec: physical)
    with pytest.raises(ValueError, match="full per-run"):
        location.validate_destination(shared, item["original_spec"])
    location.validate_destination(
        shared, item["original_spec"], installed_identity=item["installed_native_identity"]
    )
    physical["identity"]["browser_read_roots"] = ["/owned/ms-playwright"]
    with pytest.raises(ValueError, match="full native authority"):
        location.validate_destination(
            shared, item["original_spec"], installed_identity=item["installed_native_identity"]
        )


def test_shared_marker_cannot_hide_an_embedded_scope(tmp_path):
    physical, _ = measured(tmp_path)
    shared = owner.shared_destination(physical)
    shared["identity"]["browser_read_roots"] = []
    with pytest.raises(ValueError, match="contains a run"):
        owner.shared_destination(shared)
