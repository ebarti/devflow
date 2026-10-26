"""Literal, pinned workflow structure for one ordinary execution inventory.

This parses workflow formats and evaluates their authored literal conditions.
It does not infer test coverage from names, prose or script contents.
"""

from __future__ import annotations

import itertools
import re
from pathlib import PurePosixPath

import yaml

from .contracts import digest

SCHEMA = "devflow.ordinary-ci-existing-dispatch-profile/v1"
# No declaration can authorize observations until the reviewed complete source,
# original selection and report profile is bound in this versioned owner.
APPROVED_PROFILE_SHA256 = "aaae71efa5bd32ef7a8f10ce61955c202e8186bad27ca24be93b7c40fcd4de6a"
UNKNOWN = object()


def approved_profile(value):
    if (
        not isinstance(value, dict)
        or value.get("schema") != SCHEMA
        or digest(value) != APPROVED_PROFILE_SHA256
    ):
        raise ValueError("ordinary CI inventory requires its exact reviewed provenance profile")
    return value


def relative(value):
    if (
        not isinstance(value, str)
        or not value
        or value.startswith("/")
        or any(part in {"", ".", ".."} for part in value.split("/"))
        or str(PurePosixPath(value)) != value
        or "\\" in value
        or "\x00" in value
    ):
        raise ValueError("ordinary CI source path is not canonical")
    return value


class _WorkflowLoader(yaml.SafeLoader):
    def compose_node(self, parent, index):
        if self.check_event(yaml.AliasEvent):
            raise ValueError("ordinary CI workflow aliases are unsupported")
        return super().compose_node(parent, index)


_WorkflowLoader.yaml_implicit_resolvers = {
    key: [(tag, pattern) for tag, pattern in values if tag != "tag:yaml.org,2002:bool"]
    for key, values in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
_WorkflowLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool", re.compile(r"^(?:true|false)$", re.I), list("tTfF")
)


def _mapping(loader, node, deep=False):
    pairs = loader.construct_pairs(node, deep=deep)
    keys = [key for key, _ in pairs]
    if any(not isinstance(key, (str, int)) for key in keys) or len(set(keys)) != len(keys):
        raise ValueError("ordinary CI workflow has ambiguous mapping keys")
    return dict(pairs)


_WorkflowLoader.add_constructor("tag:yaml.org,2002:map", _mapping)


def document(raw):
    if not isinstance(raw, bytes) or len(raw) > 128 * 1024:
        raise ValueError("ordinary CI workflow exceeds its fixed source bound")
    value = yaml.load(raw, Loader=_WorkflowLoader)
    if not isinstance(value, dict):
        raise ValueError("ordinary CI workflow is not a mapping")
    return value


TOKEN = re.compile(
    r"\s*(==|!=|&&|\|\||\(|\)|'[^']*'|true\b|false\b|"
    r"[A-Za-z_][A-Za-z0-9_.-]*)"
)


def condition(value, context):
    if value is None or type(value) is bool:
        return True if value is None else value
    if not isinstance(value, str) or len(value) > 2048:
        raise ValueError("ordinary CI workflow condition is unsupported")
    source = value.strip()
    if source.startswith("${{") and source.endswith("}}"):
        source = source[3:-2].strip()
    tokens, position = [], 0
    while position < len(source):
        match = TOKEN.match(source, position)
        if not match:
            raise ValueError("ordinary CI workflow condition grammar is unsupported")
        tokens.append(match[1])
        position = match.end()
    index = 0

    def atom():
        nonlocal index
        if index >= len(tokens):
            raise ValueError("ordinary CI workflow condition is incomplete")
        item = tokens[index]
        index += 1
        if item == "(":
            result = either()
            if index >= len(tokens) or tokens[index] != ")":
                raise ValueError("ordinary CI workflow condition is unbalanced")
            index += 1
            return result
        if item.startswith("'"):
            return item[1:-1]
        if item in {"true", "false"}:
            return item == "true"
        if index < len(tokens) and tokens[index] == "(":
            if (
                item not in {"success", "always"}
                or index + 1 >= len(tokens)
                or tokens[index + 1] != ")"
            ):
                raise ValueError("ordinary CI workflow condition function is unsupported")
            index += 2
            return True if item == "always" else context.get("prior_steps_success", UNKNOWN)
        if item in {"==", "!=", "&&", "||", ")"}:
            raise ValueError("ordinary CI workflow condition has an invalid value")
        return context.get(item, UNKNOWN)

    def compare():
        nonlocal index
        left = atom()
        if index < len(tokens) and tokens[index] in {"==", "!="}:
            operator = tokens[index]
            index += 1
            right = atom()
            return (
                UNKNOWN
                if left is UNKNOWN or right is UNKNOWN
                else (left == right if operator == "==" else left != right)
            )
        return left

    def both():
        nonlocal index
        left = compare()
        while index < len(tokens) and tokens[index] == "&&":
            index += 1
            right = compare()
            if left is False or right is False:
                left = False
            elif left is UNKNOWN or right is UNKNOWN:
                left = UNKNOWN
            elif type(left) is not bool or type(right) is not bool:
                raise ValueError("ordinary CI workflow condition is not boolean")
            else:
                left = left and right
        return left

    def either():
        nonlocal index
        left = both()
        while index < len(tokens) and tokens[index] == "||":
            index += 1
            right = both()
            if left is True or right is True:
                left = True
            elif left is UNKNOWN or right is UNKNOWN:
                left = UNKNOWN
            elif type(left) is not bool or type(right) is not bool:
                raise ValueError("ordinary CI workflow condition is not boolean")
            else:
                left = left or right
        return left

    result = either()
    if index != len(tokens) or type(result) is not bool:
        raise ValueError("ordinary CI workflow condition is not completely determined")
    return result


def expand(value, context):
    if isinstance(value, str):

        def replace(match):
            key = match[1].strip()
            result = context.get(key, UNKNOWN)
            if result is UNKNOWN or not isinstance(result, (str, int, bool)):
                raise ValueError("ordinary CI workflow expansion is unsupported")
            return str(result).lower() if type(result) is bool else str(result)

        result = re.sub(r"\$\{\{([^{}]+)\}\}", replace, value)
        if "${{" in result:
            raise ValueError("ordinary CI workflow expansion is incomplete")
        return result
    if isinstance(value, dict):
        return {key: expand(item, context) for key, item in value.items()}
    if isinstance(value, list):
        return [expand(item, context) for item in value]
    if value is None or type(value) in (bool, int):
        return value
    raise ValueError("ordinary CI workflow literal has an unsupported type")


def workflow_plan(raw, *, repository, branch, inputs):
    value = document(raw)
    if value.get("defaults") or value.get("env"):
        raise ValueError("ordinary CI workflow defaults/global environment are unsupported")
    if not isinstance(value.get("on"), dict) or "workflow_dispatch" not in value["on"]:
        raise ValueError("ordinary CI workflow has no original dispatch entry")
    jobs = value.get("jobs")
    if not isinstance(jobs, dict) or not 1 <= len(jobs) <= 16:
        raise ValueError("ordinary CI workflow job set is unsupported")
    context = {
        "github.event_name": "workflow_dispatch",
        "github.repository": repository,
        "github.ref": "refs/heads/" + branch,
        "runner.os": "Linux",
        "runner.environment": "github-hosted",
        "prior_steps_success": True,
        **{"inputs." + key: item for key, item in inputs.items()},
    }
    result = []
    for identity, job in jobs.items():
        if (
            not isinstance(identity, str)
            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", identity)
            or not isinstance(job, dict)
            or job.get("uses")
            or job.get("defaults")
        ):
            raise ValueError("ordinary CI reusable job contract is unsupported")
        strategy = job.get("strategy") or {}
        if not isinstance(strategy, dict):
            raise ValueError("ordinary CI workflow strategy is unsupported")
        matrix = strategy.get("matrix") or {}
        if (
            not isinstance(matrix, dict)
            or any(key in matrix for key in ("include", "exclude"))
            or any(
                not isinstance(key, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", key)
                for key in matrix
            )
            or any(
                not isinstance(v, list)
                or not 1 <= len(v) <= 8
                or any(type(x) not in (str, int, bool) for x in v)
                for v in matrix.values()
            )
        ):
            raise ValueError("ordinary CI workflow matrix is unsupported")
        combinations = list(itertools.product(*matrix.values()))
        if len(combinations) > 16:
            raise ValueError("ordinary CI workflow matrix exceeds its complete bound")
        for combination in combinations:
            selection = dict(zip(matrix, combination, strict=True))
            selected = {**context, **{"matrix." + key: item for key, item in selection.items()}}
            active = condition(job.get("if"), selected)
            runner = expand(job.get("runs-on"), selected)
            if runner not in {"ubuntu-latest", "ubuntu-24.04", "ubuntu-22.04"}:
                raise ValueError("ordinary CI workflow runner contract is unsupported")
            steps = job.get("steps")
            if not isinstance(steps, list) or not 1 <= len(steps) <= 64:
                raise ValueError("ordinary CI workflow step set is unsupported")
            compiled = []
            for index, step in enumerate(steps):
                if (
                    not isinstance(step, dict)
                    or not isinstance(step.get("name"), str)
                    or ("uses" in step) == ("run" in step)
                ):
                    raise ValueError("ordinary CI workflow step identity is unsupported")
                enabled = active and condition(step.get("if"), selected)
                # Excluded deploy/secret-bearing steps remain literal source
                # data. No secret/environment value is read or defaulted.
                compiled.append(
                    {
                        "index": index,
                        "enabled": enabled,
                        "source": {
                            key: expand(item, selected) if key != "if" else item
                            for key, item in step.items()
                        }
                        if enabled
                        else step,
                    }
                )
            names = [step["source"]["name"] for step in compiled]
            if len(set(names)) != len(names):
                raise ValueError("ordinary CI workflow step names are ambiguous")
            result.append(
                {
                    "id": identity,
                    "name": expand(job.get("name", identity), selected),
                    "matrix": selection,
                    "enabled": active,
                    "runs_on": runner,
                    "environment": expand(job.get("env") or {}, selected),
                    "timeout_minutes": job.get("timeout-minutes"),
                    "steps": compiled,
                }
            )
    if len({job["name"] for job in result}) != len(result):
        raise ValueError("ordinary CI workflow job names are ambiguous")
    return result
