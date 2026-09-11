"""Pure endpoint authorization rules shared by admission, dispatch and delivery."""

import posixpath
import re

from devflow.errors import WorkflowError

ENDPOINT_OPERATIONS = {"local_delivery", "publish_pr", "merge", "release"}
TARGET_FIELD = {"local": "path", "pr": "base_ref", "merge": "target_ref", "release": "tag"}


def _reject(message):
    raise WorkflowError("endpoint_target_mismatch", message)


def validate_endpoint(endpoint):
    kind, target = endpoint["kind"], endpoint["target"]
    if kind == "local":
        if (
            not target.startswith("/")
            or target.startswith("//")
            or posixpath.normpath(target) != target
        ):
            _reject("Local endpoint target must be a canonical absolute checkout path")
    elif (
        not target
        or target.startswith(("-", "/", "refs/"))
        or target.endswith(("/", "."))
        or target == "@"
        or ".." in target
        or "@{" in target
        or "//" in target
        or re.search(r"[\x00-\x20\x7f~^:?*\[\\]", target)
        or any(part.startswith(".") or part.endswith(".lock") for part in target.split("/"))
    ):
        _reject("Remote endpoint target must be an exact short branch or tag name")
    return target


def validate_action_target(
    state, operation, payload, refs, *, repository_path=None, terminal=False
):
    if operation == "push_branch":
        if terminal or state["contract"]["endpoint"]["kind"] not in {"pr", "merge"}:
            _reject("Branch publication requires an accepted PR or merge endpoint")
        if (set(payload) - {"scope_hash", "candidate_id"} != {"head_ref"}
                or set(refs) != {"head_sha", "remote_head_sha"}):
            _reject("Push requires one branch and exact source/old remote head bindings")
        head_ref = validate_endpoint({"kind": "pr", "target": payload["head_ref"]})
        if head_ref == state["contract"]["endpoint"]["target"] or head_ref in {"main", "master"}:
            _reject("Push source cannot be the accepted target or a canonical branch")
        candidate = state["records"].get("candidate:" + str(state.get("candidate_id")))
        if not candidate or refs["head_sha"] != candidate["head_sha"]:
            _reject("Push source must match the current candidate")
        remote_head = refs["remote_head_sha"]
        if remote_head is not None and (not isinstance(remote_head, str) or not re.fullmatch(
                r"[a-f0-9]{40}|[a-f0-9]{64}", remote_head)):
            _reject("Old remote head must be an exact object ID or explicit null for creation")
        return
    if operation not in ENDPOINT_OPERATIONS:
        return
    endpoint = state["contract"]["endpoint"]
    target = validate_endpoint(endpoint)
    expected_kind = {"local_delivery": "local", "merge": "merge", "release": "release"}.get(
        operation
    )
    if operation == "publish_pr":
        if endpoint["kind"] not in {"pr", "merge"}:
            _reject("PR publication requires an accepted PR or merge target branch")
    elif endpoint["kind"] != expected_kind:
        _reject("Action operation differs from the accepted endpoint")
    if payload.get("endpoint", endpoint) != endpoint:
        _reject("Action payload changes the accepted endpoint")
    if any(payload[key] != refs[key] for key in payload.keys() & refs.keys()):
        _reject("Payload and expected refs contain conflicting values")
    values = {**refs, **payload}
    if (
        values.get("repository", state["authority"]["repository"])
        != state["authority"]["repository"]
    ):
        _reject("Action repository differs from its authority boundary")
    if operation == "local_delivery":
        if repository_path is not None and repository_path != target:
            _reject("Dispatch checkout differs from the accepted local target")
        for field in ("path", "target_path", "repository_path"):
            if field in values and values[field] != target:
                _reject("Local action redirects its accepted checkout path")
        if any(field in values for field in ("base_ref", "target_ref", "tag")):
            _reject("Local delivery cannot contain remote target selectors")
    elif operation == "publish_pr":
        if values.get("base_ref") != target:
            _reject("PR base branch differs from the accepted target")
        if values.get("target_ref", target) != target or "tag" in values:
            _reject("PR target selectors conflict with the accepted branch")
    elif operation == "release":
        if values.get("tag") != target:
            _reject("Release tag differs from the accepted target")
        if "base_ref" in values or "target_ref" in values:
            _reject("Release action cannot substitute a branch target")
    else:
        binding = payload.get("merge_binding", {})
        if binding.get("target_ref") != target or refs.get("target_ref") != target:
            _reject("Merge target branch differs from the accepted target")
        if values.get("base_ref", target) != target or "tag" in values:
            _reject("Merge target selectors conflict with the accepted branch")
        if refs.get("target_sha") != binding.get("target_sha"):
            _reject("Merge binding and expected refs identify different target commits")
    if terminal and payload.get("endpoint") != endpoint:
        _reject("Terminal delivery requires an explicit accepted endpoint binding")


def readback_matches(endpoint, observation):
    """A copied endpoint label alone is never evidence of the delivered target."""
    actual = observation.get(TARGET_FIELD[endpoint["kind"]])
    return actual == endpoint["target"] and observation.get("endpoint") == {
        "kind": endpoint["kind"],
        "target": actual,
    }
