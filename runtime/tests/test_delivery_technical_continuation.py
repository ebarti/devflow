from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
from test_delivery_gates_admission import stopped as stopped
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_native_renewal import payload_update as payload_update
from test_delivery_store import service as service

from devflow_temporal import delivery_native_renewal as renewal
from devflow_temporal import delivery_technical_continuation as technical
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_resources import read_private, write_private


def request():
    return {
        "continuation_kind": "accepted_technical_successor",
        "command_id": "technical-1",
        "expected_revision": 27,
        "expected_iteration": 4,
        "expected_candidate_id": "a" * 64,
        "expected_pr_number": 1047,
        "expected_pr_head": "b" * 40,
        "additional_iterations": 0,
        "authority_path": "/absent/authority.json",
        "authority_sha256": "c" * 64,
        "expected_source_revision": "d" * 40,
    }


@pytest.mark.parametrize(
    "change",
    ["missing", "partial-packet"],
)
def test_public_technical_preflight_refuses_before_intent_claim_archive_or_effect(service, change):
    store, submitted = service
    store.submit(submitted)
    payload = request()
    if change == "missing":
        payload.pop("authority_path")
    elif change == "wrong-hash":
        authority = store.config.state_root / "technical-authority.json"
        write_private(authority, {"decision_owner": "main task"})
        payload["authority_path"] = str(authority)
    elif change == "unknown-kind":
        payload["continuation_kind"] = "ordinary"
    elif change == "grant":
        payload["additional_iterations"] = 1
    elif change == "bool":
        payload["additional_iterations"] = False
    elif change == "extra-root":
        payload["evidence_root"] = str(store.config.state_root / "foreign")
    else:
        payload["prospective_path"] = "/absent/prospective.json"
    with store._connect() as db:
        claim = store.state.claim_for(db, "work-1")
        commands = list(db.execute("SELECT * FROM delivery_commands"))
    for operation in (store.repair_admission_preflight, store.continue_repair):
        with pytest.raises((ValueError, FileNotFoundError)):
            operation("run-1", payload)
    assert not (Path(store.spec("run-1")["state_dir"]) / "technical-successor").exists()
    with store._connect() as db:
        assert store.state.claim_for(db, "work-1") == claim
        assert list(db.execute("SELECT * FROM delivery_commands")) == commands
        assert db.execute("SELECT COUNT(*) FROM delivery_technical_successors").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_effects").fetchone()[0] == 0


def test_one_native_child_preserves_consumed_generation_and_explicit_base_lineage(
    payload_update,
    monkeypatch,
):
    store, original, payload, _old, package = payload_update
    predecessor_spec, predecessor_reference = renewal.renew(original, payload, digest(payload))
    predecessor_bytes = Path(predecessor_reference["path"]).read_bytes()
    before_receipt = read_private(Path(predecessor_reference["path"]))
    (package / "__init__.py").write_text(
        (package / "__init__.py").read_text() + "\n# Second owned controlled payload.\n"
    )
    controlled = package.parents[2]
    renewal._git(controlled, "add", ".")
    renewal._git(controlled, "commit", "-qm", "test: technical native child")
    authority = {
        "trigger_bindings": {
            "consumed_native_renewal_authority": {
                "path": payload["preparation_authority_path"],
                "sha256": payload["preparation_authority_sha256"],
            }
        },
        "installed_predecessor": {
            "source": before_receipt["source_revision"],
            "config_sha256": before_receipt["config_sha256"],
        },
    }
    predecessor = {
        "original_spec": original,
        "spec": predecessor_spec,
        "recovery": {
            "execution_spec": predecessor_spec,
            "native_preparation_renewal": predecessor_reference,
        },
    }
    next_payload = {
        **request(),
        "expected_source_revision": renewal._git(controlled, "rev-parse", "HEAD"),
        "preparation_authority_path": payload["preparation_authority_path"],
        "preparation_authority_sha256": payload["preparation_authority_sha256"],
    }
    assert technical.native_predecessor(predecessor, authority) == before_receipt
    observed = renewal.readiness(predecessor_spec, next_payload)
    observed["authority"] = authority
    proposed = deepcopy(predecessor_spec)
    proposed["base_sha"] = (
        "e" * 40
    )  # Explicit project applicability amendment; native identity is unchanged.
    child, child_reference = renewal.renew(
        proposed,
        next_payload,
        digest(next_payload),
        technical={"readiness": observed, "predecessor": predecessor},
    )
    assert Path(predecessor_reference["path"]).read_bytes() == predecessor_bytes
    assert child_reference["path"] != predecessor_reference["path"]
    assert "/technical-successor/native-generation/" in child_reference["path"]
    assert child["base_sha"] == proposed["base_sha"]
    assert (
        child["preparation"]["security_binding_sha256"]
        != (predecessor_spec["preparation"]["security_binding_sha256"])
    )
    assert renewal.renew(
        proposed,
        next_payload,
        digest(next_payload),
        technical={"readiness": observed, "predecessor": predecessor},
    ) == (child, child_reference)
    recovery = {
        "execution_spec": child,
        "native_preparation_renewal": child_reference,
        "native_predecessor": predecessor,
        "authority": authority,
    }
    assert renewal.effective_spec(proposed, recovery, technical=True) == child
    with pytest.raises(ValueError, match="exact owned path"):
        renewal.effective_spec(proposed, recovery)
    changed = deepcopy(recovery)
    changed["native_predecessor"]["spec"]["base_sha"] = "f" * 40
    with pytest.raises(ValueError, match="predecessor changed"):
        renewal.effective_spec(proposed, changed, technical=True)
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
