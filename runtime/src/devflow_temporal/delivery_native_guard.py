"""Controller-owned ancestry guard, alongside command sandbox executable denials."""

from __future__ import annotations

import os
from pathlib import Path

from .delivery_resources import read_private

NATIVE_OVERRIDES = [
    "features.plugins=false",
    "features.multi_agent=false",
    "agents.enabled=false",
]


def revision_role_identity(request: dict) -> dict | None:
    """Keep correction roles distinct from product roles on the same candidate."""
    if "revision_context" not in request:
        return None
    from .contracts import digest

    context = request["revision_context"]
    if (
        not isinstance(context, dict)
        or not isinstance(context.get("revision_id"), str)
        or not 1 <= len(context["revision_id"]) <= 128
        or request.get("role") not in {"intake", "review"}
        or type(request.get("iteration")) is not int
        or request["iteration"] != 0
        or request.get("resume_session") is not None
    ):
        raise ValueError("plan revision roles require a separate read-only attempt")
    proposal = context.get("proposed_plan")
    if (request["role"] == "review") != isinstance(proposal, dict):
        raise ValueError("revision review requires its exact proposed plan")
    return {
        "revision_id": context["revision_id"],
        "proposed_plan_sha256": digest(proposal) if proposal is not None else None,
    }


def validate_revision_role(request: dict, store) -> None:
    """A context object is never sufficient authority to launch a planner."""
    identity = revision_role_identity(request)
    if identity is None:
        return
    from .delivery_feature_revisions import authenticate_revision_role

    receipt = authenticate_revision_role(store, request)
    if any(receipt.get(key) != value for key, value in identity.items()):
        raise ValueError("plan revision role does not match its durable admission")


def protected_commands(binary: str) -> tuple[Path, ...]:
    runtime = Path(__file__).resolve().parents[2]
    paths = {
        Path(binary),
        runtime / "src" / "devflow_temporal",
        runtime / ".venv/bin/devflow-delivery",
        runtime / ".venv/bin/devflow-delivery-mcp",
        Path("/opt/homebrew/bin/codex"),
        Path("/usr/local/bin/codex"),
    }
    return tuple(sorted(paths | {path.resolve() for path in paths}, key=str))


def reject_nested_controller() -> None:
    if os.environ.get("DEVFLOW_MANAGED_DEPTH"):
        raise ValueError("managed role children cannot create another Devflow controller or run")


def validate_native_turn(spec: dict, role: str, iteration: int, store) -> None:
    """One finite ceiling for native roles and their broker-owned gate resources."""
    from .delivery_preparation import require_native_execution

    require_native_execution(spec)
    reject_nested_controller()
    maximum = (
        spec["policy"].get("max_intake_rounds", 8) - 1
        if role == "intake"
        else spec["policy"].get("max_repairs", 2)
    )
    if role != "intake" and spec.get("retry_budget_version") != 1:
        with store._connect() as db:
            grants = db.execute(
                """SELECT maximum_iteration FROM delivery_repair_grants WHERE run_id=?
                   UNION ALL SELECT maximum_iteration FROM delivery_scope_amendments WHERE run_id=?
                   UNION ALL SELECT maximum_iteration FROM delivery_policy_recoveries WHERE run_id=?
                   """,
                (spec["run_id"],) * 3,
            ).fetchall()
            row = db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                             (spec['run_id'],)).fetchone()
            import json

            recovery = json.loads(row[0]) if row and row[0] else None
            if recovery and recovery.get('kind') == 'stopped_delivery_resume':
                from .delivery_stopped_resume import custody

                if custody(db, recovery) != spec:
                    raise ValueError('native stopped resume policy changed')
                maximum = recovery['maximum_iteration']
            else:
                maximum = max([maximum, *(grant[0] for grant in grants)])
    if (
        role not in spec["policy"]["roles"]
        or type(iteration) is not int
        or not 0 <= iteration <= maximum
    ):
        raise ValueError("native role exceeded the controller-owned finite turn limit")


def validate_role_ancestry(request: dict) -> None:
    from .delivery_preparation import require_native_execution

    require_native_execution(request["spec"])
    if request["spec"].get("provider") != "codex":
        return
    if (
        request.get("native_authorized") is not True
        or os.environ.get("DEVFLOW_MANAGED_DEPTH") != "1"
        or os.environ.get("DEVFLOW_NATIVE_PID") != str(os.getpid())
    ):
        raise ValueError("native role recursion or untrusted launch ancestry")
    folder = Path(request["result_path"]).parent
    if os.environ.get("DEVFLOW_NATIVE_JOURNAL") != str(folder / "native-process.json"):
        raise ValueError("native role launch journal does not match its attempt")
    journal = read_private(folder / "native-process.json")
    if journal["phase"] != "authorized" or str(os.getpid()) not in journal["owned"]:
        raise ValueError("native role has no controller-authorized process identity")
    from .delivery_native_process import process_table

    if (
        process_table().get(os.getpid(), {}).get("identity")
        != journal["owned"][str(os.getpid())]["identity"]
    ):
        raise ValueError("native role process start identity changed")
