"""Data-only, bounded provenance for a fresh review after a controller repair.

Historical Python is parsed, never imported or executed. Version one deliberately
admits only the two declared derivation roots and unchanged external guards.
"""
from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from copy import deepcopy
from pathlib import Path, PurePosixPath

from .contracts import digest

SCHEMA = "python-ast-3.12-v1"
PREFIX = "runtime/src/devflow_temporal/"
ROOTS = {
    "delivery_feature_execution.py": ("revised_worker_spec",),
    "delivery_feature_gates.py": ("derive_chunk_gates",),
}
UNITS = {
    "delivery_feature_execution.py": {"revised_worker_spec"},
    "delivery_feature_gates.py": {
        "GateAdmissionError", "STAGES", "_TEST_FILE", "_base_content", "_base_files", "_git",
        "_operands", "_runner", "_selection_map", "admitted_recipes", "derive_chunk_gates",
        "recipe_project", "recipe_selectors", "selected_recipe", "selector_path",
        "validate_chunk_gates",
    },
}
GUARDS = (
    "contracts.py", "delivery_github_contract.py", "delivery_plan_model.py",
    "delivery_source_scope.py",
)
IMPORTS = {
    "copy", "hashlib", "json", "pathlib", "re", "subprocess", "tomllib",
    "contracts", "delivery_feature_gates", "delivery_github_contract",
    "delivery_plan_model", "delivery_source_scope",
}
MODULE_IMPORTS = IMPORTS | {"__future__", "delivery_execution_registry", "xml.etree.ElementTree"}
LOCAL_IMPORTS = {
    "contracts", "delivery_execution_registry", "delivery_feature_gates",
    "delivery_github_contract", "delivery_plan_model", "delivery_source_scope",
}
RESETS = {
    "security_binding_sha256", "environment_proof_sha256", "initial_decision_prompt",
    "recovery", "baseline_checks", "pr_body",
}
STAGES = {"checks", "prepublish_checks", "browser_qa"}


def _protected_root():
    # A request's source_root is descriptive evidence, never an execution path.
    return Path(__file__).resolve(strict=True).parents[3]


def _git(root, *args, input=None):
    if root.resolve(strict=True) != root or not root.is_dir():
        raise ValueError("review context requires the canonical protected runtime repository")
    environment = {
        key: value for key, value in os.environ.items()
        if not key.startswith(("GIT_", "DYLD_", "LD_"))
    }
    environment.update(
        GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_SYSTEM="/dev/null",
        GIT_NO_REPLACE_OBJECTS="1", GIT_TERMINAL_PROMPT="0",
    )
    result = subprocess.run(
        ["/usr/bin/git", "--no-pager", "--no-replace-objects", "-c", "core.hooksPath=/dev/null",
         "-c", "core.fsmonitor=false", "-c", "core.untrackedCache=false", "-C", str(root),
         *args], input=input, capture_output=True, timeout=15, env=environment, check=False,
    )
    if result.returncode:
        raise ValueError("authenticated runtime Git object data is unavailable")
    return result.stdout


def _sources(root, revision):
    """Verify the complete native Python payload before selecting AST units."""
    if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("review runtime revision must be an exact clean Git commit")
    if _git(root, "rev-parse", "--show-toplevel").decode().strip() != str(root):
        raise ValueError("review context repository root changed")
    metadata = root / ".git"
    info = metadata.lstat()
    if (metadata.resolve(strict=True) != metadata or info.st_uid != os.getuid()
            or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode))):
        raise ValueError("review context Git metadata is not the protected canonical store")
    for option in ("--absolute-git-dir", "--git-common-dir"):
        raw = _git(root, "rev-parse", option).decode().strip()
        directory = Path(raw) if Path(raw).is_absolute() else root / raw
        if directory.resolve(strict=True) != directory or not directory.is_dir():
            raise ValueError("review context Git object store is linked or unavailable")
    if _git(root, "rev-parse", revision + "^{commit}").decode().strip() != revision:
        raise ValueError("review runtime revision is not an exact commit object")
    tree = _git(root, "ls-tree", "-r", "-z", revision, "--", PREFIX)
    entries = []
    for entry in tree.split(b"\0"):
        if not entry:
            continue
        header, raw_path = entry.split(b"\t", 1)
        name = raw_path.decode("utf-8")
        if not name.endswith(".py"):
            continue
        mode, kind, oid = header.split()
        relative = name.removeprefix(PREFIX)
        if (not name.startswith(PREFIX) or not relative
                or any(p in {".", ".."} for p in PurePosixPath(relative).parts)
                or mode not in {b"100644", b"100755"} or kind != b"blob"
                or not re.fullmatch(rb"[0-9a-f]{40}", oid)):
            raise ValueError("runtime Python payload contains unsupported Git source entries")
        entries.append((relative, oid))
    if not 1 <= len(entries) <= 256 or len({name for name, _ in entries}) != len(entries):
        raise ValueError("runtime Python payload exceeds its bounded manifest")
    object_input = b"".join(oid + b"\n" for _, oid in entries)
    metadata = _git(root, "cat-file", "--batch-check", input=object_input).splitlines()
    sizes = []
    for (_, oid), line in zip(entries, metadata, strict=True):
        object_id, kind, size = line.split()
        if object_id != oid or kind != b"blob" or not 0 <= int(size) <= 1024 * 1024:
            raise ValueError("runtime Python blob is missing or exceeds its bound")
        sizes.append(int(size))
    if sum(sizes) > 16 * 1024 * 1024:
        raise ValueError("runtime Python payload exceeds its byte bound")
    content = _git(root, "cat-file", "--batch", input=object_input)
    offset, sources, manifest = 0, {}, {}
    for (relative, oid), size in zip(entries, sizes, strict=True):
        end = content.index(b"\n", offset)
        if content[offset:end] != oid + b" blob " + str(size).encode():
            raise ValueError("runtime Python object readback changed")
        raw = content[end + 1:end + 1 + size]
        offset = end + size + 2
        if len(raw) != size or content[offset - 1:offset] != b"\n":
            raise ValueError("runtime Python object readback is incomplete")
        sources[relative] = raw.decode("utf-8")
        manifest["devflow_temporal/" + relative] = hashlib.sha256(raw).hexdigest()
    if offset != len(content):
        raise ValueError("runtime Python object readback has unexpected bytes")
    encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    return sources, hashlib.sha256(encoded).hexdigest()


class _WithoutDocumentation(ast.NodeTransformer):
    def strip(self, node):
        node = self.generic_visit(node)
        if (node.body and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)):
            node.body = node.body[1:]
        return node

    visit_Module = strip
    visit_FunctionDef = strip
    visit_AsyncFunctionDef = strip
    visit_ClassDef = strip


def _normalized(node):
    return ast.dump(_WithoutDocumentation().visit(deepcopy(node)), include_attributes=False)


def _supported_import(node, modules):
    if any(alias.name == "*" for alias in node.names):
        return False
    if isinstance(node, ast.ImportFrom):
        return node.module in modules and node.level == (1 if node.module in LOCAL_IMPORTS else 0)
    return all(alias.name in modules and alias.name not in LOCAL_IMPORTS for alias in node.names)


def _initializers(node):
    """Inspect eager definition expressions without treating function bodies as eager."""
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        if node.decorator_list:
            raise ValueError("review context has dynamic eager definition decorators")
        values = [*node.decorator_list, *node.args.defaults,
                  *(value for value in node.args.kw_defaults if value is not None)]
        arguments = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
        arguments += [arg for arg in (node.args.vararg, node.args.kwarg) if arg is not None]
        values.extend(arg.annotation for arg in arguments if arg.annotation is not None)
        if node.returns is not None:
            values.append(node.returns)
        for value in values:
            yield from ast.walk(value)
    elif isinstance(node, ast.ClassDef):
        if node.decorator_list:
            raise ValueError("review context has dynamic eager definition decorators")
        for value in [*node.bases, *node.keywords, *node.decorator_list]:
            yield from ast.walk(value)
        for value in node.body:
            yield from _initializers(value)
    else:
        yield from ast.walk(node)


def _units(text, name):
    module = ast.parse(text)
    definitions, imports, mutations = {}, {}, set()
    for node in module.body:
        bound = {}
        if isinstance(node, ast.ClassDef) and (
            name != "delivery_feature_gates.py" or node.name != "GateAdmissionError"
            or len(node.bases) != 1 or not isinstance(node.bases[0], ast.Name)
            or node.bases[0].id != "ValueError" or node.keywords
        ):
            raise ValueError("review context has unsupported implicit eager class initialization")
        initializers = list(_initializers(node))
        calls = [unit for unit in initializers if isinstance(unit, ast.Call)]
        frozen_pattern = (isinstance(node, ast.Assign) and len(node.targets) == 1
                          and isinstance(node.targets[0], ast.Name)
                          and node.targets[0].id == "_TEST_FILE"
                          and name == "delivery_feature_gates.py"
                          and len(calls) == 1 and isinstance(calls[0].func, ast.Attribute)
                          and isinstance(calls[0].func.value, ast.Name)
                          and calls[0].func.value.id == "re" and calls[0].func.attr == "compile")
        if (calls and not frozen_pattern
                or any(isinstance(unit, (ast.Global, ast.Nonlocal, ast.NamedExpr, ast.Delete))
                       for unit in initializers)):
            raise ValueError("review context has dynamic eager module initialization")
        for unit in initializers:
            if isinstance(unit, ast.Assign) and any(
                not isinstance(target, ast.Name) for target in unit.targets
            ):
                raise ValueError("review context has dynamic indirect module stores")
            if isinstance(unit, (ast.Import, ast.ImportFrom)):
                if not _supported_import(unit, MODULE_IMPORTS):
                    raise ValueError("review context has an unsupported module import binding")
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound[node.name] = node
        elif isinstance(node, ast.Assign):
            if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
                mutations.update(n.id for n in ast.walk(node) if isinstance(n, ast.Name))
            else:
                bound[node.targets[0].id] = node
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                key = alias.asname or alias.name.split(".")[0]
                if key in definitions or key in imports:
                    raise ValueError("review context contains ambiguous module bindings")
                imports[key] = (node, alias)
        elif not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
                  and isinstance(node.value.value, str)):
            # Module-level conditions/annotations/augmented stores/deletions can
            # replace a selected global without defining a distinct AST unit.
            mutations.update(n.id for n in ast.walk(node) if isinstance(n, ast.Name))
            if not isinstance(node, ast.Pass):
                raise ValueError("review context has unsupported dynamic module initialization")
        for key, value in bound.items():
            if key == "__builtins__":
                raise ValueError("review context has dynamic module builtins rebinding")
            if key in definitions or key in imports:
                raise ValueError("review context contains ambiguous module definitions")
            definitions[key] = value
    selected, queue = {}, list(ROOTS[name])
    while queue:
        key = queue.pop()
        if key in selected:
            continue
        if key not in UNITS[name] or key not in definitions:
            raise ValueError("review context has an unsupported derivation helper closure")
        node = definitions[key]
        selected[key] = node
        referenced = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
        queue.extend(sorted((referenced & definitions.keys()) - selected.keys()))
    if set(selected) != UNITS[name]:
        raise ValueError("review context changed its declared derivation helper closure")
    referenced = {n.id for node in selected.values() for n in ast.walk(node)
                  if isinstance(n, ast.Name)}
    if (referenced | selected.keys()) & mutations:
        raise ValueError("review context has dynamic or rebound module authority")
    bindings = {}
    for key in sorted(referenced & imports.keys()):
        node, alias = imports[key]
        binding = deepcopy(node)
        binding.names = [deepcopy(alias)]
        bindings[key] = _normalized(binding)
    for node in [*selected.values(), *(imports[key][0] for key in referenced & imports.keys())]:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) \
                and node.decorator_list:
            raise ValueError("review context has unsupported dynamic definition decorators")
        for unit in ast.walk(node):
            if isinstance(unit, (ast.Import, ast.ImportFrom)):
                if not _supported_import(unit, IMPORTS):
                    raise ValueError("review context has an unsupported import binding")
            if isinstance(unit, ast.Call) and isinstance(unit.func, ast.Name) and unit.func.id in {
                "eval", "exec", "compile", "globals", "locals", "__import__", "getattr",
                "setattr", "delattr",
            }:
                raise ValueError("review context has unsupported dynamic derivation code")
            if isinstance(unit, (ast.Global, ast.Nonlocal)):
                raise ValueError("review context mutates derivation bindings")
    return {"definitions": {k: _normalized(v) for k, v in sorted(selected.items())},
            "imports": bindings}


def normalized_context(sources):
    if sys.version_info[:2] != (3, 12) or ast.FunctionDef._fields != (
        "name", "args", "body", "decorator_list", "returns", "type_comment", "type_params"
    ):
        raise ValueError("review context requires the python-ast-3.12-v1 parser schema")
    units = {name: _units(sources[name], name) for name in ROOTS}
    return {"context_sha256": digest(units), "guards": {
        name: hashlib.sha256(_normalized(ast.parse(sources[name])).encode()).hexdigest()
        for name in GUARDS
    }}


def authenticated_runtime_context(identity):
    """Recorded actual native identity authenticates data, not an old import."""
    if (not isinstance(identity, dict) or identity.get("dirty") is not False
            or identity.get("local_digest") is not None
            or not isinstance(identity.get("runtime_payload_sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", identity["runtime_payload_sha256"])):
        raise ValueError("review context lacks an exact clean actual native runtime")
    sources, payload = _sources(_protected_root(), identity.get("revision"))
    if payload != identity["runtime_payload_sha256"]:
        raise ValueError("actual native runtime payload differs from immutable Git objects")
    return {"revision": identity["revision"], "runtime_payload_sha256": payload,
            **normalized_context(sources)}


def current_runtime_context():
    """Require actual installed bytes and HEAD to be the same clean payload."""
    from .payload import payload_digest

    root = _protected_root()
    if _git(root, "status", "--porcelain", "--untracked-files=normal"):
        raise ValueError("reconsideration requires a clean protected current runtime")
    identity = {"dirty": False, "local_digest": None,
                "revision": _git(root, "rev-parse", "HEAD").decode().strip(),
                "runtime_payload_sha256": payload_digest(root / PREFIX)}
    return authenticated_runtime_context(identity)


def candidate_subject(candidate):
    keys = ("head", "content_sha256", "base_sha", "environment_digest")
    if not isinstance(candidate, dict) or any(not candidate.get(key) for key in keys):
        raise ValueError("reconsideration lost product candidate custody")
    return {key: candidate[key] for key in keys}


def non_gate_policy(policy, *, semantic=False):
    result = {key: deepcopy(value) for key, value in policy.items()
              if key not in STAGES | RESETS}
    if semantic and isinstance(result.get("native_identity"), dict):
        result["native_identity"].pop("runtime_payload_sha256", None)
    return result


def preserved_worker(original, derived):
    """Prove current derivation keeps retained authority before fresh review."""
    from .delivery_source_scope import authority

    if (authority(original["policy"]) != authority(derived["policy"])
            or non_gate_policy(original["policy"]) != non_gate_policy(derived["policy"])):
        raise ValueError("current derivation changes retained worker non-gate authority")
    excluded = {"policy", "policy_digest", "accepted_plan", "feature_plan_revision",
                "gate_selections_version", "feature_gate_selections", "expected_paths",
                "baseline_checks_version", "prepared_environment", "preparation"}
    if ({k: v for k, v in original.items() if k not in excluded}
            != {k: v for k, v in derived.items() if k not in excluded}):
        raise ValueError("current derivation changes retained worker execution custody")
    old_plan, new_plan = json.loads(original["accepted_plan"]), json.loads(derived["accepted_plan"])
    if any(old_plan.get(k) != new_plan.get(k) for k in ("scope", "acceptance", "integration")):
        raise ValueError("current derivation changes retained worker accepted outcome")
    return {
        "run_id": original["run_id"], "chunk_id": original["feature_worker"]["chunk_id"],
        "kind": original["feature_worker"]["kind"],
        "authority_sha256": digest(authority(original["policy"])),
        "non_gate_policy_sha256": digest(non_gate_policy(original["policy"], semantic=True)),
        "gate_selections": deepcopy(derived.get("feature_gate_selections", [])),
        "retained_authority_preserved": True,
    }
