"""Schedule identical broad validation once, without synthesizing cached evidence."""
from __future__ import annotations

from copy import deepcopy
from pathlib import PurePosixPath

from .contracts import canonical_json


def _signature(check):
    value = {key: item for key, item in check.items() if key != "id"}
    value.setdefault("cwd", ".")
    value.setdefault("timeout_seconds", 600)
    return canonical_json(value)


def _preparation(check):
    argv = check["argv"]
    return ("/store" in argv or bool(check.get("native_addon_prerequisite"))
            or any(token in {"install", "sync"} for token in argv))


def prepublication(spec):
    """Return checks still needed before publication and exact deferred ID bindings.

    A build-only worker has no postpublication gate. Dependency setup and Git
    history checks remain where requested. Unknown unique commands may depend on
    earlier build output, so their presence conservatively keeps the whole list.
    """
    checks = list(spec["policy"].get("prepublish_checks", []))
    if (spec.get("validation_schedule_version") not in {1, 2}
            or spec.get("feature_worker", {}).get("kind") == "build"):
        return checks, {}
    post = {_signature(c): c for c in spec["policy"].get("checks", [])}
    deferred, retained = {}, []
    for check in checks:
        match = post.get(_signature(check))
        history = PurePosixPath(check["argv"][0]).name == "git"
        if match and not history and not _preparation(check):
            deferred[check["id"]] = match["id"]
        else:
            retained.append(check)
            if not history and not _preparation(check):
                return checks, {}
    return retained, deferred


def local_checks(spec, configured, planned):
    """Bind compatible static plan requirements to one configured execution.

    Keep old admitted schedules intact. Tests, selected recipes, Git history and
    preparation commands retain separate execution and evidence. A merged static
    command uses the stricter deadline and retains both requirement identities;
    it never emits a second synthetic pass or a cached duration.
    """
    dependencies = [c for c in planned
                    if c['id'].startswith('planned-python-dependencies-')]
    remaining = [c for c in planned if c not in dependencies]
    result = deepcopy(configured)
    if spec.get('validation_schedule_version') != 2:
        return dependencies + result + remaining
    ordinary = {'id', 'kind', 'argv', 'cwd', 'timeout_seconds',
                'min_tests', 'junit_required', 'generated_directories'}

    def compatible(check):
        return (check.get('kind') in {'check', 'static'}
                and check.get('min_tests', 1) == 1
                and not check.get('junit_required', False)
                and PurePosixPath(check['argv'][0]).name != 'git'
                and not _preparation(check))

    retained = []
    for check in remaining:
        matches = [c for c in result
                   if set(c) <= ordinary and compatible(c)
                   and c['argv'] == check['argv']
                   and c.get('cwd', '.') == check.get('cwd', '.')]
        if (check.get('kind') != 'static'
                or set(check) - ordinary != {'plan_provenance'}
                or not check.get('plan_provenance') or not compatible(check)
                or len(matches) != 1):
            retained.append(check)
            continue
        match = matches[0]
        match['timeout_seconds'] = min(match.get('timeout_seconds', 600),
                                       check.get('timeout_seconds', 600))
        match['plan_provenance'] = deepcopy(check['plan_provenance'])
        match['satisfies'] = [match['id'], check['id']]
        generated = sorted(set(match.get('generated_directories', []))
                           | set(check.get('generated_directories', [])))
        if generated:
            match['generated_directories'] = generated
    return dependencies + result + retained


def require_deferred_checks(spec, prechecked, checked):
    """A deferred requirement is met only by its actual passing later command."""
    _retained, expected = prepublication(spec)
    if (prechecked.get("deferred_to_checks") or {}) != expected:
        raise ValueError("prepublication deferral differs from frozen validation inputs")
    completed = {r["id"]: r for r in checked.get("results", [])}
    recipes = {r["id"]: r for r in spec["policy"].get("checks", [])}
    for ident in expected.values():
        result = completed.get(ident, {})
        if result.get("passed") is not True or result.get("argv") != recipes[ident]["argv"]:
            raise ValueError("deferred validation has no passing execution: " + ident)
