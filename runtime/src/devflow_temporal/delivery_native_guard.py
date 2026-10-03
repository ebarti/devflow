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
    if role != "intake":
        with store._connect() as db:
            grants = db.execute(
                """SELECT maximum_iteration FROM delivery_repair_grants WHERE run_id=?
                   UNION ALL SELECT maximum_iteration FROM delivery_scope_amendments WHERE run_id=?
                   UNION ALL SELECT maximum_iteration FROM delivery_policy_recoveries WHERE run_id=?
                   """,
                (spec["run_id"],) * 3,
            ).fetchall()
        maximum = max([maximum, *(row[0] for row in grants)])
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
