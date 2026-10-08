"""Run shared project prerequisites on the immutable base before feature roles."""
from __future__ import annotations

from .candidate import candidate_for
from .delivery_broker import DeliveryBroker, _git
from .delivery_resources import RunResources, private_directory


def run_baseline_checks(broker: DeliveryBroker) -> dict:
    spec = broker.spec
    checks = spec["policy"].get("baseline_checks", [])
    if spec.get("baseline_checks_version") not in (1, 2) or not checks:
        raise ValueError("baseline checks require an explicit immutable admission")
    path = broker._gate_path("baseline", 0)
    private_directory(path.parent)
    resources = RunResources(spec) if spec.get("resource_cleanup_version") == 1 else None
    if resources:
        resources.register(path, "gate")
    if path.exists():
        if _git(path, "rev-parse", "--show-toplevel") != str(path):
            raise ValueError("baseline checkout identity changed")
    else:
        _git(broker.source, "worktree", "add", "--detach", str(path), spec["base_sha"])
    if resources:
        resources.created(path)
    candidate = candidate_for(path)
    if (candidate["head"] != spec["base_sha"]
            or _git(path, "status", "--porcelain", "--untracked-files=all")):
        raise ValueError("baseline checkout must be clean at the admitted base")
    feature = broker.candidate()
    result = broker._run_check_list(
        path, checks, broker.evidence_dir / "baseline-checks", candidate,
    )
    feature_unchanged = broker.candidate() == feature
    return {
        **result, "base_sha": spec["base_sha"], "baseline_candidate": candidate,
        "feature_candidate_id": feature["id"], "feature_unchanged": feature_unchanged,
        "state": result["state"] if feature_unchanged else "failed",
    }
