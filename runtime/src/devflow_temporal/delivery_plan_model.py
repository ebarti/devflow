"""Canonical feature plans, independent from frozen execution authority.

v1 is the historical exact file-allowlist plan. v2 is exactly
{version: 2, scope, acceptance, workstreams, final_gates}. A workstream retains
{id, title, issue_number, acceptance, chunks}. A chunk retains its historical
business fields and dependencies, replaces allowed_paths with expected_paths,
and adds gates. Gate selections are {stage, recipe_id, selectors, reason?};
stage is checks, prepublish_checks, or browser_qa. They contain no commands,
environment, ports, or resource grants. Admitted recipe and coverage validation
belongs to delivery_feature_gates, against the frozen policy.
"""

from __future__ import annotations

import re
from copy import deepcopy

from .contracts import digest

IDENTIFIER = re.compile(r"[a-z][a-z0-9_-]{0,63}")
GATE_STAGES = frozenset({"checks", "prepublish_checks", "browser_qa"})


def _text(value, label, maximum=8192):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum or "\x00" in value:
        raise ValueError(label + " must be nonempty bounded text")
    return value


def _strings(value, label, maximum=64):
    if not isinstance(value, list) or not 1 <= len(value) <= maximum:
        raise ValueError(label + " must be a bounded nonempty list")
    return [_text(item, label) for item in value]


def _validate_v1(value: dict, *, allowed_paths: list[str] | None = None) -> dict:
    """Validate business decomposition without accepting new execution authority."""
    if not isinstance(value, dict) or set(value) != {"scope", "acceptance", "workstreams"}:
        raise ValueError("delivery plan requires scope, acceptance and workstreams")
    _text(value["scope"], "feature scope")
    _strings(value["acceptance"], "feature acceptance")
    streams = value["workstreams"]
    if not isinstance(streams, list) or not 1 <= len(streams) <= 16:
        raise ValueError("delivery plan requires between one and sixteen workstreams")
    stream_ids, chunks = set(), {}
    issue_numbers = set()
    required_stream = {"id", "title", "issue_number", "acceptance", "chunks"}
    required_chunk = {
        "id",
        "title",
        "scope",
        "steps",
        "verification",
        "acceptance",
        "allowed_paths",
        "depends_on",
    }
    for stream in streams:
        if not isinstance(stream, dict) or set(stream) != required_stream:
            raise ValueError("workstream fields do not match the delivery contract")
        ident = stream["id"]
        if not isinstance(ident, str) or not IDENTIFIER.fullmatch(ident) or ident in stream_ids:
            raise ValueError("workstream ID is invalid or duplicated")
        stream_ids.add(ident)
        _text(stream["title"], "workstream title", 200)
        _strings(stream["acceptance"], "workstream acceptance")
        number = stream["issue_number"]
        if number is not None and (type(number) is not int or number < 1):
            raise ValueError("workstream issue number must be positive or null")
        if number is not None:
            if number in issue_numbers:
                raise ValueError("a sub-issue can own only one workstream")
            issue_numbers.add(number)
        if not isinstance(stream["chunks"], list) or not 1 <= len(stream["chunks"]) <= 32:
            raise ValueError("workstream requires a bounded nonempty chunk list")
        previous = None
        for chunk in stream["chunks"]:
            if not isinstance(chunk, dict) or set(chunk) != required_chunk:
                raise ValueError("chunk fields do not match the delivery contract")
            key = chunk["id"]
            if not isinstance(key, str) or not IDENTIFIER.fullmatch(key) or key in chunks:
                raise ValueError("chunk ID is invalid or duplicated")
            for field in ("title", "scope"):
                _text(chunk[field], "chunk " + field, 200 if field == "title" else 8192)
            for field in ("steps", "verification", "acceptance", "allowed_paths"):
                _strings(chunk[field], "chunk " + field)
            paths = chunk["allowed_paths"]
            if len(set(paths)) != len(paths):
                raise ValueError("chunk paths must be unique")
            for path in paths:
                if (
                    path.startswith("/")
                    or "\\" in path
                    or ".." in path.split("/")
                    or any(part in {".git", ".codex", ".agents"} for part in path.split("/"))
                    or path in {"", "."}
                ):
                    raise ValueError("chunk path is outside an owned source scope")
            if allowed_paths is not None and set(paths) - set(allowed_paths):
                raise ValueError("delivery plan exceeds the configured source scope")
            dependencies = chunk["depends_on"]
            if (
                not isinstance(dependencies, list)
                or len(dependencies) > 32
                or any(not isinstance(item, str) for item in dependencies)
                or len(set(dependencies)) != len(dependencies)
                or key in dependencies
            ):
                raise ValueError("chunk dependencies are invalid")
            if previous and previous not in dependencies:
                raise ValueError("chunks in a workstream must declare their sequential dependency")
            chunks[key] = chunk
            previous = key
    if len(chunks) > 32:
        raise ValueError("feature exceeds the thirty-two chunk bound")
    for chunk in chunks.values():
        if set(chunk["depends_on"]) - chunks.keys():
            raise ValueError("chunk depends on an unknown feature chunk")
    ready = set()
    while len(ready) < len(chunks):
        next_items = {
            key
            for key, chunk in chunks.items()
            if key not in ready and set(chunk["depends_on"]) <= ready
        }
        if not next_items:
            raise ValueError("chunk dependency graph contains a cycle")
        ready |= next_items
    return deepcopy(value)



def plan_version(plan: dict) -> int:
    if not isinstance(plan, dict):
        raise ValueError("delivery plan must be an object")
    if "version" not in plan:
        return 1
    if type(plan["version"]) is not int or plan["version"] != 2:
        raise ValueError("unsupported delivery plan version")
    return 2


def _source_path(path, label="expected path"):
    _text(path, label, 4096)
    if (path.startswith("/") or "\\" in path or path in {"", "."}
            or any(part in {"", ".", "..", ".git", ".codex", ".agents"}
                   for part in path.split("/"))):
        raise ValueError(label + " is outside an owned source scope")


def validate_gate_selections(value: list, label="gate selections") -> list:
    if not isinstance(value, list) or len(value) > 64:
        raise ValueError(label + " must be a bounded list")
    seen = set()
    for gate in value:
        if (not isinstance(gate, dict)
                or not {"stage", "recipe_id", "selectors"} <= gate.keys()
                or gate.keys() - {"stage", "recipe_id", "selectors", "reason"}
                or not isinstance(gate["stage"], str) or gate["stage"] not in GATE_STAGES
                or not isinstance(gate["recipe_id"], str)
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", gate["recipe_id"])):
            raise ValueError("gate selection must reference an admitted recipe")
        key = (gate["stage"], gate["recipe_id"])
        if key in seen:
            raise ValueError("gate recipes must be selected once per chunk")
        seen.add(key)
        selectors = gate["selectors"]
        if (not isinstance(selectors, list) or len(selectors) > 64
                or any(not isinstance(item, str) for item in selectors)
                or len(set(selectors)) != len(selectors)):
            raise ValueError("gate selectors must be a bounded unique list")
        for selector in selectors:
            _text(selector, "gate selector", 4096)
            # Recipe-specific named selectors are authenticated by gate admission.
            # File selectors have only pytest :: names or a Playwright :line suffix.
            path = selector.split("::", 1)[0]
            if re.search(r":\d+$", path):
                path = path.rsplit(":", 1)[0]
            if ":" in path or selector.startswith("-") or any(
                    char in selector for char in ("\n", "\r", "\t")):
                raise ValueError("gate selector must be a concrete owned selector")
            _source_path(path, "gate selector")
        if "reason" in gate:
            _text(gate["reason"], "gate reason", 8192)
    return deepcopy(value)


def validate_plan(value: dict, *, allowed_paths: list[str] | None = None) -> dict:
    """Validate v1 unchanged; v2 expected files do not grant execution authority."""
    if plan_version(value) == 1:
        return _validate_v1(value, allowed_paths=allowed_paths)
    if set(value) != {"version", "scope", "acceptance", "workstreams", "final_gates"}:
        raise ValueError("v2 delivery plan requires version, scope, acceptance, "
                         "workstreams, final_gates")
    validate_gate_selections(value["final_gates"], "feature final gates")
    legacy = {key: deepcopy(value[key]) for key in ("scope", "acceptance", "workstreams")}
    if not isinstance(legacy["workstreams"], list):
        raise ValueError("delivery plan workstreams must be a list")
    for stream in legacy["workstreams"]:
        if not isinstance(stream, dict) or not isinstance(stream.get("chunks"), list):
            raise ValueError("workstream fields do not match the delivery contract")
        for chunk in stream["chunks"]:
            if (not isinstance(chunk, dict) or "allowed_paths" in chunk
                    or "expected_paths" not in chunk or "gates" not in chunk):
                raise ValueError("v2 chunks require expected_paths and gates")
            validate_gate_selections(chunk.pop("gates"))
            paths = chunk.pop("expected_paths")
            _strings(paths, "chunk expected_paths")
            for path in paths:
                _source_path(path)
            chunk["allowed_paths"] = paths
    _validate_v1(legacy)
    return deepcopy(value)


def ordered_chunks(plan: dict) -> list[dict]:
    """Stable topological order; independent streams can build concurrently."""
    validate_plan(plan)
    pending = [
        {**deepcopy(chunk), "workstream_id": stream["id"], "issue_number": stream["issue_number"]}
        for stream in plan["workstreams"] for chunk in stream["chunks"]
    ]
    done, result = set(), []
    while pending:
        selected = next(chunk for chunk in pending if set(chunk["depends_on"]) <= done)
        result.append(selected)
        done.add(selected["id"])
        pending.remove(selected)
    return result


def resolve_plan_issues(plan: dict, bindings: dict) -> dict:
    value = validate_plan(plan)
    if set(bindings) != {stream["id"] for stream in value["workstreams"]}:
        raise ValueError("published plan requires every resolved workstream issue")
    for stream in value["workstreams"]:
        number = bindings[stream["id"]]["number"]
        if type(number) is not int or number < 1:
            raise ValueError("published plan requires positive workstream issue numbers")
        if stream["issue_number"] is not None and stream["issue_number"] != number:
            raise ValueError("resolved workstream identity differs from the plan")
        stream["issue_number"] = number
    return validate_plan(value)


def plan_digest(plan: dict) -> str:
    return digest(validate_plan(plan))


def migrate_plan_v1(plan: dict, bindings: dict, gates_by_chunk: dict, final_gates: list) -> dict:
    """Explicit conversion only; never infer or drop historical gate requirements."""
    if plan_version(plan) != 1:
        raise ValueError("plan migration requires a legacy v1 plan")
    value = resolve_plan_issues(plan, bindings)
    chunks = {chunk["id"] for stream in value["workstreams"] for chunk in stream["chunks"]}
    if set(gates_by_chunk) != chunks:
        raise ValueError("plan migration requires explicit gate selections for every chunk")
    value.update(version=2, final_gates=deepcopy(final_gates))
    for stream in value["workstreams"]:
        for chunk in stream["chunks"]:
            chunk["expected_paths"] = chunk.pop("allowed_paths")
            chunk["gates"] = deepcopy(gates_by_chunk[chunk["id"]])
    return validate_plan(value)


def compact_plan(plan: dict) -> dict:
    value = validate_plan(plan)
    if plan_version(value) != 2:
        raise ValueError("compact parent index requires v2")
    for stream in value["workstreams"]:
        stream["chunks"] = [
            {key: chunk[key] for key in ("id", "title", "depends_on")}
            for chunk in stream["chunks"]
        ]
    return value


def validate_plan_index(value: dict) -> dict:
    """Authenticate graph/identity summaries without inventing implementation detail."""
    if (not isinstance(value, dict)
            or set(value) != {"version", "scope", "acceptance", "workstreams", "final_gates"}
            or value.get("version") != 2):
        raise ValueError("compact plan index has an unsupported schema")
    reconstructed = deepcopy(value)
    if not isinstance(reconstructed["workstreams"], list):
        raise ValueError("compact plan index workstreams must be a list")
    for stream in reconstructed["workstreams"]:
        if not isinstance(stream, dict) or not isinstance(stream.get("chunks"), list):
            raise ValueError("compact plan index requires workstream chunk summaries")
        for chunk in stream["chunks"]:
            if not isinstance(chunk, dict) or set(chunk) != {"id", "title", "depends_on"}:
                raise ValueError("parent index must not contain detailed implementation chunks")
            chunk.update(scope="Index summary", steps=["Child plan"], verification=["Child plan"],
                         acceptance=["Child plan"], expected_paths=["index-placeholder"], gates=[])
    validate_plan(reconstructed)
    return deepcopy(value)


def index_chunks(value: dict) -> list[dict]:
    validate_plan_index(value)
    pending = [{**deepcopy(chunk), "workstream_id": stream["id"],
                "issue_number": stream["issue_number"]}
               for stream in value["workstreams"] for chunk in stream["chunks"]]
    done, result = set(), []
    while pending:
        selected = next(chunk for chunk in pending if set(chunk["depends_on"]) <= done)
        result.append(selected)
        done.add(selected["id"])
        pending.remove(selected)
    return result
