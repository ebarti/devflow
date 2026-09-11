"""Resolve explicit subagent settings without inheriting the active coordinator."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

from devflow.errors import WorkflowError
from devflow.validation import digest

_ROLES = {"implementation_worker": "implementer", "review": "reviewer", "qa": "qa"}
_EFFORTS = {"none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"}
_FIELDS = {"model", "reasoning_effort"}
_POLICY_FIELDS = {
    "role", "role_name", "model", "reasoning_effort", "agent_type", "policy_hash", "sources"
}


def _invalid(message: str) -> WorkflowError:
    return WorkflowError("role_policy_invalid", message)


def _text(value) -> bool:
    return isinstance(value, str) and bool(value.strip()) and value == value.strip()


def _read(path: Path) -> dict:
    try:
        with path.open("rb") as stream:
            return tomllib.load(stream)
    except (OSError, ValueError) as exc:
        # Parser diagnostics can contain arbitrary TOML values, including secrets.
        raise WorkflowError(
            "role_policy_unavailable", "Role settings file is unavailable or malformed"
        ) from exc


def _table(value) -> dict:
    if not isinstance(value, dict):
        raise _invalid("Role settings must use TOML tables")
    return value


def _settings(table: dict, model_key: str, effort_keys: tuple[str, ...]) -> dict:
    result = {"model": table[model_key]} if model_key in table else {}
    efforts = [table[key] for key in effort_keys if key in table]
    if efforts:
        if any(value != efforts[0] for value in efforts):
            raise _invalid("Conflicting reasoning effort aliases")
        result["reasoning_effort"] = efforts[0]
    return result


def resolve_role_policy(
    role: str, *, config_path: Path, overrides: dict | None = None, role_name: str | None = None
) -> dict:
    """Resolve each setting: explicit override, role file, subagent default, saved default.

    ``overrides`` is an already selected user role/session override, not an active
    coordinator snapshot. It accepts model and reasoning_effort (or the TOML
    alias model_reasoning_effort). Provenance records only settings that win and
    the selected role-file route; it never snapshots whole configuration files.
    """
    if not isinstance(role, str) or role not in _ROLES:
        raise _invalid("Unsupported workflow role")
    selected = _ROLES[role] if role_name is None else role_name
    if not isinstance(selected, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", selected):
        raise _invalid("Role name must be a simple configured agent name")
    if overrides is None:
        overrides = {}
    if not isinstance(overrides, dict) or not set(overrides) <= {
        "model", "reasoning_effort", "model_reasoning_effort"
    }:
        raise _invalid("Only explicit model and reasoning effort overrides are accepted")

    config_path = Path(config_path).expanduser().resolve()
    config = _read(config_path)
    agents = _table(config.get("agents", {}))
    role_config = _table(agents.get(selected, {}))
    route = []
    if "config_file" in role_config:
        if not _text(role_config["config_file"]):
            raise _invalid("Configured role file must be a nonempty path")
        role_path = Path(role_config["config_file"]).expanduser()
        if not role_path.is_absolute():
            role_path = config_path.parent / role_path
        role_path = role_path.resolve()
        route.append((f"{config_path}#agents.{selected}", {"config_file": str(role_path)}))
        role_settings = _read(role_path)
    else:
        role_path = config_path.parent / "agents" / f"{selected}.toml"
        role_settings = _read(role_path) if role_path.exists() else {}

    layers = [
        ("explicit-user-overrides", _settings(
            overrides, "model", ("reasoning_effort", "model_reasoning_effort")
        )),
        (str(role_path), _settings(role_settings, "model", ("model_reasoning_effort",))),
        (f"{config_path}#agents", _settings(
            agents, "default_subagent_model",
            ("default_subagent_reasoning_effort", "default_subagent_model_reasoning_effort"),
        )),
        (str(config_path), _settings(config, "model", ("model_reasoning_effort",))),
    ]
    resolved = {}
    used = list(route)
    for reference, settings in layers:
        winning = {key: value for key, value in settings.items() if key not in resolved}
        if winning:
            resolved.update(winning)
            used.append((reference, winning))
    if set(resolved) != _FIELDS:
        raise WorkflowError(
            "role_policy_unresolved", "Role requires an explicit resolved model and reasoning effort"
        )
    sources = []
    for reference, settings in used:
        source = {"reference": reference, "settings": settings}
        sources.append({**source, "hash": digest(source)})
    policy = {
        "role": role, "role_name": selected, **resolved, "agent_type": "default",
        "sources": sources,
    }
    policy["policy_hash"] = digest(policy)
    return validate_role_policy(policy)


def validate_role_policy(policy: dict) -> dict:
    """Validate saved allowlisted provenance and hashes without rereading mutable files.

    Hashes establish record integrity, not user authorization or host conformance.
    """
    if not isinstance(policy, dict) or set(policy) != _POLICY_FIELDS:
        raise _invalid("Role policy has missing or unexpected fields")
    if (
        not isinstance(policy["role"], str) or policy["role"] not in _ROLES
        or not isinstance(policy["role_name"], str)
        or not re.fullmatch(r"[A-Za-z0-9_-]+", policy["role_name"])
        or policy["agent_type"] != "default"
        or not _text(policy["model"])
        or not isinstance(policy["reasoning_effort"], str)
        or policy["reasoning_effort"] not in _EFFORTS
    ):
        raise _invalid("Role policy requires a model, supported effort and generic agent type")
    if not isinstance(policy["sources"], list) or not policy["sources"]:
        raise _invalid("Role policy requires settings provenance")
    references = set()
    resolved = {}
    for source in policy["sources"]:
        if (
            not isinstance(source, dict) or set(source) != {"reference", "settings", "hash"}
            or not _text(source["reference"])
            or not isinstance(source["settings"], dict) or not source["settings"]
            or not set(source["settings"]) <= _FIELDS | {"config_file"}
        ):
            raise _invalid("Role policy source must contain only allowlisted settings and evidence")
        if source["reference"] in references:
            raise _invalid("Role policy source references must be unique")
        references.add(source["reference"])
        for key, value in source["settings"].items():
            if not _text(value):
                raise _invalid("Role policy source settings must be nonempty strings")
            if key in _FIELDS:
                if key in resolved or value != policy[key]:
                    raise _invalid("Role policy sources must bind each resolved setting once")
                resolved[key] = value
        expected = digest({key: source[key] for key in ("reference", "settings")})
        if source["hash"] != expected:
            raise _invalid("Role policy source hash mismatch")
    if set(resolved) != _FIELDS:
        raise _invalid("Role policy sources do not substantiate resolved settings")
    if policy["policy_hash"] != digest({k: v for k, v in policy.items() if k != "policy_hash"}):
        raise _invalid("Role policy hash mismatch")
    return policy
