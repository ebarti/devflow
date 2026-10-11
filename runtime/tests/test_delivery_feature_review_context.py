"""Data-only context provenance, relevant changes and unsupported bindings."""
from __future__ import annotations

import ast
import os
import subprocess
from pathlib import Path

import pytest

from devflow_temporal import delivery_feature_review_context as context


def git(root, *args):
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1")
    return subprocess.check_output(
        ["/usr/bin/git", "-c", "user.name=Isolated fixture", "-c", "user.email=fixture@invalid",
         "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", "-C", str(root), *args],
        env=environment, stderr=subprocess.DEVNULL,
    ).decode().strip()


def context_repository(tmp_path, monkeypatch):
    root = tmp_path / "review-runtime"
    package = root / context.PREFIX
    package.mkdir(parents=True)
    sources = {path.relative_to(Path(context.__file__).parent).as_posix(): path.read_text()
               for path in Path(context.__file__).parent.rglob("*.py")}
    # This old derivation is source DATA. The test never imports or runs it.
    old = dict(sources)
    name = "delivery_feature_execution.py"
    old[name] = old[name].replace(
        'result["policy"].update({stage: gate_policy[stage] for stage in STAGES})',
        'result["policy"] = gate_policy',
    )
    assert old[name] != sources[name]
    for name, value in old.items():
        path = package / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)
    git(root, "init", "-q")
    git(root, "add", ".")
    git(root, "commit", "-qm", "Original isolated review runtime")
    old_revision = git(root, "rev-parse", "HEAD")
    _, old_payload = context._sources(root, old_revision)
    for name, value in sources.items():
        (package / name).write_text(value)
    git(root, "add", ".")
    git(root, "commit", "-qm", "Retain worker authority during gate derivation")
    monkeypatch.setattr(context, "_protected_root", lambda: root)
    current = context.current_runtime_context()
    historical_identity = {"dirty": False, "local_digest": None, "revision": old_revision,
                           "runtime_payload_sha256": old_payload, "source_root": "/ignored"}
    return root, sources, historical_identity, current


def test_git_payload_uses_fixed_protected_objects_and_semantic_context(tmp_path, monkeypatch):
    root, sources, historical, current = context_repository(tmp_path, monkeypatch)
    previous = context.authenticated_runtime_context(historical)
    assert previous["context_sha256"] != current["context_sha256"]
    assert previous["guards"] == current["guards"]
    git(root, "replace", historical["revision"], current["revision"])
    monkeypatch.setenv("GIT_DIR", "/caller/owned/repository")
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.fsmonitor")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "caller-command")
    assert context.authenticated_runtime_context(historical) == previous
    cosmetic = {name: "# unrelated comment\n" + ast.unparse(ast.parse(value)) + "\n"
                for name, value in sources.items()}
    cosmetic["delivery_feature_execution.py"] += '\ndef unrelated_fixture():\n    return 777\n'
    assert context.normalized_context(cosmetic) == context.normalized_context(sources)
    invalid = {**historical, "runtime_payload_sha256": current["runtime_payload_sha256"]}
    with pytest.raises(ValueError, match="payload differs"):
        context.authenticated_runtime_context(invalid)
    linked = root / context.PREFIX / "linked.py"
    linked.symlink_to(root / context.PREFIX / "contracts.py")
    git(root, "add", ".")
    git(root, "commit", "-qm", "Unsupported linked source fixture")
    with pytest.raises(ValueError, match="unsupported Git source"):
        context._sources(root, git(root, "rev-parse", "HEAD"))


def test_normalization_refuses_ambiguous_dynamic_import_and_parser_bindings(tmp_path, monkeypatch):
    _, sources, _, _ = context_repository(tmp_path, monkeypatch)
    name = "delivery_feature_execution.py"
    for addition in (
        '\ndef revised_worker_spec(*args, **kwargs):\n    return None\n',
        '\nrevised_worker_spec: object = None\n',
        '\nrevised_worker_spec += replacement\n',
        '\nif True:\n    revised_worker_spec = replacement\n',
        '\nfrom caller_module import revised_worker_spec\n',
        '\nglobals()["revised_worker_spec"] = replacement\n',
        '\nunrelated = exec("revised_worker_spec = replacement")\n',
        '\nmodule = setattr(sys.modules[__name__], "revised_worker_spec", replacement)\n',
        '\ndef unrelated(value=exec("revised_worker_spec = replacement")):\n    pass\n',
        '\n@exec("revised_worker_spec = replacement")\ndef unrelated():\n    pass\n',
        '\n@replacement\ndef unrelated():\n    pass\n',
        '\nclass Unrelated:\n    exec("revised_worker_spec = replacement")\n',
        '\nclass FixtureMeta(type):\n    pass\nclass Unrelated(metaclass=FixtureMeta):\n    pass\n',
        '\nclass FixtureBase:\n    pass\nclass Unrelated(FixtureBase):\n    pass\n',
        '\nsys.modules[__name__].revised_worker_spec = replacement\n',
        '\n__builtins__ = {"len": replacement}\n',
        '\nfrom .contracts import *\n',
        '\nimport unbounded_plugin\n',
    ):
        with pytest.raises(ValueError, match="ambiguous|dynamic|rebound|import binding|class init"):
            context.normalized_context({**sources, name: sources[name] + addition})
    for replacement in (
        sources[name].replace('    plan = validate_plan(proposed_plan',
                              '    globals()\n    plan = validate_plan(proposed_plan'),
        sources[name].replace('from .delivery_feature_gates import',
                              'from .unbounded_plugin import'),
        sources[name].replace('from .contracts import', 'from contracts import'),
        sources[name].replace('from copy import', 'from .copy import'),
    ):
        with pytest.raises(ValueError, match="dynamic|import binding"):
            context.normalized_context({**sources, name: replacement})
    monkeypatch.setattr(context.sys, "version_info", (3, 13, 0))
    with pytest.raises(ValueError, match="parser schema"):
        context.normalized_context(sources)
