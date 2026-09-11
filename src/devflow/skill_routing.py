"""Read-only skill discovery and ownership of already-computed workflow actions.

The domain computes permissible actions. This module only names their instruction
owner and resolves actual files from the selected immutable release.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

from devflow.errors import WorkflowError

STAGE_SKILLS = (
    "using-devflow", "devflow-defining-work", "devflow-planning", "devflow-coordinating",
    "devflow-implementing", "devflow-reviewing", "devflow-verifying", "devflow-delivering",
)
SKILLS = (*STAGE_SKILLS, "devflow")
ROLE_SKILLS = {
    "implementation_worker": "devflow-implementing",
    "review": "devflow-reviewing", "qa": "devflow-verifying",
}
ACTION_SKILLS = {
    "prepare_scope": "devflow-defining-work",
    "prepare_workspace": "devflow-coordinating",
    "launch_role": "devflow-coordinating",
    "activate_role": "devflow-coordinating",
    "observe_role": "devflow-coordinating",
    "resume_role": "devflow-coordinating",
    "wait_roles": "devflow-coordinating",
    "import_gate_result": "devflow-coordinating",
    "request_user_action": "devflow-coordinating",
    "resolve_findings": "devflow-coordinating",
    "capture_candidate": "devflow-implementing",
    "implement": "devflow-implementing",
    "repair_findings": "devflow-implementing",
    "run_check": "devflow-verifying",
    "push_branch": "devflow-delivering",
    "publish_candidate": "devflow-delivering",
    "publish_findings": "devflow-delivering",
    "close_fixed_threads": "devflow-delivering",
    "record_accounting": "devflow-delivering",
    "deliver": "devflow-delivering",
    "done": "devflow-delivering",
}


def route_actions(actions):
    result = []
    for action in actions:
        kind = action["kind"]
        if kind == "reconcile_action":
            operation = action.get("action", {}).get("operation")
            skill = ("devflow-coordinating" if operation in {
                "prepare_workspace", "launch_role", "send_role",
            } else "devflow-verifying" if operation == "run_check" else "devflow-delivering")
        else:
            skill = ACTION_SKILLS.get(kind)
        if skill is None:
            raise WorkflowError("unknown_skill_route", f"Workflow action has no skill owner: {kind}")
        routed = {**action, "skill": skill}
        role = action.get("role") or action.get("action", {}).get("payload", {}).get("role")
        if role in ROLE_SKILLS:
            routed["role_skill"] = ROLE_SKILLS[role]
        result.append(routed)
    return result


def catalog(root: Path):
    """Catalog actual package files, including wheel resources, without creating state."""
    directory = root / "skills"
    if not directory.is_dir():
        directory = Path(__file__).parent / "resources" / "skills"
    result = []
    for name in SKILLS:
        path = directory / name / "SKILL.md"
        if not path.is_file():
            continue
        raw = path.read_bytes()
        text = raw.decode("utf-8")
        header = re.match(r"\A---\n(.*?)\n---(?:\n|$)", text, re.S)
        if not header or not re.search(rf"(?m)^name: {re.escape(name)}$", header[1]):
            raise WorkflowError("invalid_skill", f"Skill metadata does not match its directory: {name}")
        description = re.search(r"(?m)^description: (.+)$", header[1])
        if not description:
            raise WorkflowError("invalid_skill", f"Skill lacks a discovery description: {name}")
        result.append({"name": name, "description": description[1], "path": str(path.resolve()),
                       "sha256": hashlib.sha256(raw).hexdigest(), "compatibility": name == "devflow"})
    if not result:
        raise WorkflowError("skills_unavailable", "The selected release has no discoverable skills")
    return result


def resolve_catalog(repository, *, state_dir, work_id, release_root):
    from devflow.installation import installed_release
    from devflow.profiles import load_profile
    from devflow.runtime import active_revision, package_root

    root = package_root()
    revision = active_revision(state_dir, work_id)
    if (repository / ".devflow").exists():
        profile = load_profile(repository)
        revision = revision or profile.lock["revision"]
    if revision:
        root = release_root.expanduser().absolute() / "releases" / revision
        metadata = installed_release(root)
        if metadata["revision"] != revision:
            raise WorkflowError("release_mismatch", "Skill release differs from the selected pin")
    elif (root / ".devflow-release.json").exists():
        revision = installed_release(root)["revision"]
    return {"package_revision": revision, "unmanaged_package": revision is None,
            "skills": catalog(root)}


def resolve_request(action, request, args):
    if action not in {"list", "resolve"}:
        raise WorkflowError("unknown_command", "Use skill list or skill resolve")
    result = resolve_catalog(args.repository, state_dir=args.state_dir,
                             work_id=request.get("work_id"), release_root=args.release_root)
    if action == "resolve":
        name = request.get("name")
        selected = next((entry for entry in result["skills"] if entry["name"] == name), None)
        if selected is None:
            raise WorkflowError("skill_unavailable", "Named skill is absent from the selected release; "
                                "preserve the attempt and use its compatible recovery/upgrade path")
        result = {key: value for key, value in result.items() if key != "skills"} | {"skill": selected}
    return result
