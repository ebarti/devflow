"""Retained signed integration readback for already-admitted technical deliveries."""

from __future__ import annotations

from pathlib import Path

from .contracts import canonical_json
from .delivery_broker import _git
from .delivery_gates_admission import _reference
from .delivery_resources import _ancestors, read_private


def reference(path, sha256):
    _ancestors(Path(path))
    return _reference(path, sha256)


def _validate_commit(repo, head, grant):
    expected = grant["integration"]
    if (
        _git(repo, "rev-parse", head + "^{tree}") != expected["tree"]
        or _git(repo, "show", "-s", "--format=%P", head).split()
        != [expected["old_head"], expected["main"]]
        or _git(repo, "show", "-s", "--format=%s", head) != expected["subject"]
        or _git(repo, "show", "-s", "--format=%an <%ae>", head) != expected["signer"]
        or "Signed-off-by: " + expected["signer"]
        not in _git(repo, "show", "-s", "--format=%B", head).splitlines()
        or _git(repo, "show", "-s", "--format=%G?", head) not in {"G", "U"}
    ):
        raise ValueError("integration commit lost exact tree, parents or human signing authority")


def readback(spec, recovery):
    plan = recovery.get("integration")
    if not plan:
        return
    path = Path(spec["state_dir"]) / "technical-successor/integration.json"
    receipt = read_private(path)
    if any(
        canonical_json(receipt.get(key)) != canonical_json(value) for key, value in plan.items()
    ):
        raise ValueError("integration retained applicability mapping changed")
    _validate_commit(Path(spec["checkout"]), receipt["head"], {"integration": plan})
    if (
        _git(Path(spec["checkout"]), "for-each-ref", "--format=%(objectname)", receipt["ref"])
        != receipt["head"]
        or spec["base_sha"] != plan["main"]
    ):
        raise ValueError("integration retained ref or explicit base amendment changed")
