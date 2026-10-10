"""Admit chunk gate selections and bind every named source to passing evidence.

Commands, resources and fixture ports come only from frozen policy or fixed
recipe metadata in the admitted base. Plans can select concrete tests; they
cannot invent commands or use aggregate counts to qualify a missing selector.
Historical inputs without gate_selections_version retain their original gates.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import tomllib
import xml.etree.ElementTree as ET
from copy import deepcopy
from pathlib import Path, PurePosixPath

from .contracts import digest
from .delivery_source_scope import require_authorized, source_path

STAGES = ("checks", "prepublish_checks", "browser_qa")
_TEST_FILE = re.compile(r"(?:test_[A-Za-z0-9_]+\.py|[A-Za-z0-9_.-]+\.(?:test|spec)\.[cm]?[jt]sx?)$")


class GateAdmissionError(ValueError):
    """A concrete planning error, distinct from an assertion/product failure."""

    def __init__(self, detail, *, chunk_id=None, selector=None, planning_defect=False):
        super().__init__(detail)
        self.planning_defect = planning_defect
        self.diagnostic = {
            "category": "gate_prerequisite",
            "detail": detail,
            "chunk_id": chunk_id,
            "selector": selector,
        }


def selector_path(selector: str) -> str:
    # The current execution protocol selects whole files. Node IDs and line
    # filters need distinct case receipts and are deliberately not inferred.
    source_path(selector)
    if not _TEST_FILE.fullmatch(PurePosixPath(selector).name):
        raise GateAdmissionError(
            "gate selector must name a concrete supported test file", selector=selector
        )
    return selector


def _git(source: Path, *args: str) -> str:
    result = subprocess.run(
        [
            "git",
            "--no-replace-objects",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-C",
            str(source),
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode:
        raise ValueError("gate source metadata is unavailable")
    return result.stdout.rstrip("\n")


def _base_files(spec: dict) -> list[str]:
    base, raw = spec.get("base_sha"), spec.get("source_path")
    if (
        not isinstance(base, str)
        or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", base)
        or not isinstance(raw, str)
        or not Path(raw).is_absolute()
        or Path(raw).resolve(strict=True) != Path(raw)
    ):
        raise ValueError("gate admission requires the frozen source commit")
    return _git(Path(raw), "ls-tree", "-r", "--name-only", base).splitlines()


def _base_content(spec: dict, relative: str) -> str:
    source_path(relative)
    source, base = Path(spec["source_path"]), spec["base_sha"]
    entry = _git(source, "ls-tree", "--full-tree", base, "--", relative).split()
    if (
        len(entry) != 4
        or entry[0] not in {"100644", "100755"}
        or entry[1] != "blob"
        or entry[3] != relative
    ):
        raise ValueError("admitted gate metadata must be a fixed tracked file")
    return _git(source, "cat-file", "blob", entry[2])


def admitted_recipes(spec: dict) -> dict[tuple[str, str], dict]:
    result = {}
    for stage in STAGES:
        entries = (
            [spec["policy"][stage]]
            if stage == "browser_qa" and spec["policy"].get(stage)
            else spec["policy"].get(stage, [])
        )
        if stage == "browser_qa" and not entries:
            entries = []
        for recipe in entries:
            key = (stage, recipe.get("id", "browser_qa"))
            if key in result:
                raise ValueError("admitted gate recipe IDs are ambiguous")
            result[key] = deepcopy(recipe)
    # Metadata is read from the immutable admitted commit, never the candidate.
    if spec.get("source_path") and "scripts/checks.toml" in _base_files(spec):
        metadata = tomllib.loads(_base_content(spec, "scripts/checks.toml"))
        if metadata.get("schema_version") != 1 or not isinstance(metadata.get("checks"), dict):
            raise ValueError("unsupported admitted repository gate recipes")
        for name, recipe in metadata["checks"].items():
            if not isinstance(recipe, dict) or recipe.get("kind") not in {"junit", "static"}:
                continue
            key = ("checks", "checks." + name)
            if key in result:
                raise ValueError("repository gate recipe ID collides with frozen policy")
            result[key] = {
                **deepcopy(recipe),
                "id": key[1],
                "tracked_recipe": True,
                "junit_required": recipe["kind"] == "junit",
            }
    return result


def _runner(recipe: dict) -> tuple[str | None, int | None]:
    argv = recipe.get("argv", [])
    for index, arg in enumerate(argv):
        if arg in {"playwright", "vitest"} and argv[index + 1 : index + 2] == [
            "test" if arg == "playwright" else "run"
        ]:
            return arg, index + 2
        if arg == "pytest" or PurePosixPath(arg).name == "pytest":
            return "pytest", index + 1
        if PurePosixPath(arg).name == "node" and argv[index + 1 : index + 2] == ["--test"]:
            return "node", index + 2
    return None, None


def recipe_project(recipe: dict, spec: dict) -> str:
    cwd = recipe.get("cwd", ".")
    if cwd != ".":
        source_path(cwd)
    argv = recipe.get("argv", [])
    filters = [argv[i + 1] for i, arg in enumerate(argv[:-1]) if arg == "--filter"]
    filters += [arg.split("=", 1)[1] for arg in argv if arg.startswith("--filter=")]
    if filters:
        if len(filters) != 1:
            raise GateAdmissionError("gate package filter must resolve one admitted package")
        matches = []
        for relative in _base_files(spec):
            if PurePosixPath(relative).name != "package.json":
                continue
            if json.loads(_base_content(spec, relative)).get("name") == filters[0]:
                matches.append(PurePosixPath(relative).parent.as_posix())
        if len(matches) != 1:
            raise GateAdmissionError("gate package filter must have one fixed source owner")
        return matches[0]
    # uv --project changes dependency selection, not pytest's process cwd.
    return cwd


def _operands(recipe: dict) -> list[tuple[int, str]]:
    """Only literal file operands of supported admitted test runner forms."""
    runner, start = _runner(recipe)
    if runner is None:
        return []
    argv, result = recipe["argv"], []
    takes_value = {
        "--config",
        "--project",
        "--grep",
        "--grep-invert",
        "--retries",
        "--workers",
        "--timeout",
        "--reporter",
        "--output",
        "--shard",
        "--repeat-each",
        "--global-timeout",
        "--trace",
        "-c",
        "-m",
        "-k",
        "-o",
        "--junitxml",
        "--outputFile",
        "--test-name-pattern",
        "--test-reporter",
        "--test-reporter-destination",
    }
    index = start
    while index < len(argv):
        arg = argv[index]
        if arg in takes_value:
            index += 2
            continue
        if arg.startswith("-"):
            index += 1
            continue
        if _TEST_FILE.fullmatch(PurePosixPath(arg).name):
            source_path(arg)
            result.append((index, arg))
        index += 1
    return result


def recipe_selectors(recipe: dict, spec: dict) -> list[str]:
    operands = _operands(recipe)
    if not operands:
        return []
    project = recipe_project(recipe, spec)
    return [(PurePosixPath(project) / raw).as_posix() for _, raw in operands]


def _selection_map(gates: list[dict], recipes: dict) -> dict[tuple[str, str], dict]:
    result = {}
    for gate in gates:
        key = (gate["stage"], gate["recipe_id"])
        if key not in recipes:
            raise GateAdmissionError("gate selection names an unadmitted recipe: " + key[1])
        if key in result:
            raise GateAdmissionError("gate selection repeats an ambiguous recipe")
        selectors = gate["selectors"]
        if len(selectors) != len(set(selectors)):
            raise GateAdmissionError("gate selectors must be unique")
        for selector in selectors:
            selector_path(selector)
        runner = _runner(recipes[key])[0]
        if selectors and runner == "node":
            raise GateAdmissionError(
                "Node --test named selectors lack admitted per-file report provenance"
            )
        if selectors and runner is None and not recipes[key].get("junit_required"):
            raise GateAdmissionError(
                "named gate selectors require a supported runner or owned JUnit"
            )
        result[key] = deepcopy(gate)
    return result


def validate_chunk_gates(plan: dict, spec: dict) -> dict:
    """Prove recipe admission, prerequisites and complete final selector coverage."""
    from .delivery_plan_model import ordered_chunks, plan_version, validate_plan

    plan = validate_plan(plan, allowed_paths=spec["policy"].get("allowed_paths"))
    if plan_version(plan) == 1:
        return {"version": 1, "final_chunk_id": ordered_chunks(plan)[-1]["id"]}
    chunks = ordered_chunks(plan)
    recipes = admitted_recipes(spec)
    finals = _selection_map(plan["final_gates"], recipes)
    baseline = set(_base_files(spec))
    by_id = {c["id"]: c for c in chunks}
    selections = {c["id"]: _selection_map(c["gates"], recipes) for c in chunks}
    for gates in [finals, *selections.values()]:
        for key, selection in gates.items():
            selected_recipe(recipes[key], selection, spec, browser=key[0] == "browser_qa")
    for chunk in chunks:
        require_authorized(spec["policy"], chunk["expected_paths"])
    closure = {}

    def prerequisites(key):
        if key not in closure:
            closure[key] = {key}
            for dep in by_id[key]["depends_on"]:
                closure[key] |= prerequisites(dep)
        return closure[key]

    def available(selector, chunk):
        if selector in baseline:
            return
        possible = {
            path for ident in prerequisites(chunk["id"]) for path in by_id[ident]["expected_paths"]
        }
        if selector not in possible:
            raise GateAdmissionError(
                "required selector is created outside this chunk's prerequisites: " + selector,
                chunk_id=chunk["id"],
                selector=selector,
                planning_defect=True,
            )

    for key, recipe in recipes.items():
        if recipe.get("tracked_recipe"):
            continue
        original = recipe_selectors(recipe, spec)
        if original and not recipe.get("tracked_recipe"):
            if key not in finals or not set(original) <= set(finals[key]["selectors"]):
                raise GateAdmissionError(
                    "final feature gates dropped admitted required selectors: " + key[1]
                )
            for chunk in chunks:
                selected = selections[chunk["id"]].get(key)
                if not selected or not selected["selectors"]:
                    raise GateAdmissionError(
                        "chunk needs current compatibility selectors for " + key[1],
                        chunk_id=chunk["id"],
                    )
    for chunk in chunks:
        for selection in selections[chunk["id"]].values():
            for selector in selection["selectors"]:
                available(selector, chunk)
    final_chunk = chunks[-1]
    for key, selection in finals.items():
        for selector in selection["selectors"]:
            available(selector, final_chunk)
            if not any(
                selector in gates.get(key, {}).get("selectors", []) for gates in selections.values()
            ):
                raise GateAdmissionError(
                    "final selector has no assigned chunk gate: " + selector,
                    chunk_id=final_chunk["id"],
                    selector=selector,
                )
    return {
        "version": 2,
        "final_chunk_id": final_chunk["id"],
        "plan_sha256": digest(plan),
        "final_gates": deepcopy(plan["final_gates"]),
    }


def selected_recipe(recipe: dict, selection: dict, spec: dict, *, browser=False) -> dict:
    result = deepcopy(recipe)
    selectors = selection["selectors"]
    if not selectors:
        return result
    project = recipe_project(recipe, spec)
    runner, _ = _runner(recipe)
    parent = PurePosixPath(project)
    for selector in selectors:
        path = PurePosixPath(selector)
        if parent.as_posix() != "." and not path.is_relative_to(parent):
            raise GateAdmissionError(
                "selected test escaped the admitted recipe package", selector=selector
            )
        if (
            runner == "pytest"
            and path.suffix != ".py"
            or runner in {"playwright", "vitest", "node"}
            and path.suffix == ".py"
        ):
            raise GateAdmissionError(
                "selected file is unsupported by the admitted runner", selector=selector
            )
    if runner and (_operands(recipe) or recipe.get("tracked_recipe")):
        operands = {index for index, _ in _operands(recipe)}
        translated = []
        for selector in selectors:
            path = PurePosixPath(selector)
            parent = PurePosixPath(project)
            if parent.as_posix() != "." and not path.is_relative_to(parent):
                raise GateAdmissionError(
                    "selected test escaped the admitted recipe package", selector=selector
                )
            translated.append(path.relative_to(parent).as_posix())
        if operands:
            first = min(operands)
            result["argv"] = [
                item
                for index, arg in enumerate(recipe["argv"])
                for item in (translated if index == first else [] if index in operands else [arg])
            ]
        else:
            start = _runner(recipe)[1]
            result["argv"] = [*recipe["argv"][:start], *translated, *recipe["argv"][start:]]
    result.update(
        required_selectors=list(selectors), selector_evidence_version=1, selector_project=project
    )
    if browser:
        if runner != "playwright":
            raise GateAdmissionError(
                "browser named selectors require an admitted Playwright recipe"
            )
        # Fixed built-in reporter output goes into the existing retained log.
        # It adds no environment, port, file or network authority.
        argv = result["argv"]
        clean, index = [], 0
        while index < len(argv):
            if argv[index] == "--reporter":
                index += 2
            elif argv[index].startswith("--reporter="):
                index += 1
            else:
                clean.append(argv[index])
                index += 1
        result["argv"] = [*clean, "--reporter=line,json"]
    return result


def derive_chunk_gates(spec: dict, plan: dict, chunk: dict, *, final=False) -> dict:
    """Return a fresh policy and selected tracked recipes; preserve legacy inputs."""
    if plan.get("version") != 2:
        return {"policy": deepcopy(spec["policy"])}
    validate_chunk_gates(plan, spec)
    recipes = admitted_recipes(spec)
    gates = _selection_map(chunk["gates"], recipes)
    if final:
        for key, selection in _selection_map(plan["final_gates"], recipes).items():
            if key in gates:
                selection["selectors"] = list(
                    dict.fromkeys([*gates[key]["selectors"], *selection["selectors"]])
                )
            gates[key] = selection
    policy = deepcopy(spec["policy"])
    for stage in STAGES:
        entries = (
            [policy[stage]]
            if stage == "browser_qa" and policy.get(stage)
            else policy.get(stage, [])
        )
        if stage == "browser_qa" and not entries:
            entries = []
        selected = [
            selected_recipe(
                recipe,
                gates[(stage, recipe.get("id", "browser_qa"))],
                spec,
                browser=stage == "browser_qa",
            )
            if (stage, recipe.get("id", "browser_qa")) in gates
            else recipe
            for recipe in entries
        ]
        if stage == "browser_qa":
            policy[stage] = selected[0] if selected else None
        else:
            policy[stage] = selected
    return {
        "policy": policy,
        "gate_selections_version": 2,
        "feature_gate_selections": list(gates.values()),
        "expected_paths": list(chunk["expected_paths"]),
    }


def resolve_required_selectors(
    recipe: dict, checkout: Path, *, spec: dict | None = None
) -> list[dict]:
    """Bind tracked tests or authorized new v2 files already in the candidate hash.

    Implementers do not stage new files. The broker commits after prechecks, so
    explicit v2 admission also accepts nonignored, regular source within frozen
    execution authority. Legacy selectors still require tracked files.
    """
    selectors = recipe.get("required_selectors", [])
    if not selectors:
        return []
    if (
        recipe.get("selector_evidence_version") != 1
        or not isinstance(selectors, list)
        or not 1 <= len(selectors) <= 32
        or len(selectors) != len(set(selectors))
    ):
        raise GateAdmissionError("required selectors lost their admitted version or uniqueness")
    checkout = checkout.resolve(strict=True)
    result = []
    for selector in selectors:
        selector_path(selector)
        path = checkout / selector
        tracked = _git(checkout, "ls-files", "--", selector) == selector
        visible_new = False
        if not tracked and spec is not None and spec.get("gate_selections_version") == 2:
            from .candidate import SKIP_NAMES

            if (
                not any(part in SKIP_NAMES for part in PurePosixPath(selector).parts)
                and _git(checkout, "ls-files", "--others", "--exclude-standard", "--", selector)
                == selector
            ):
                try:
                    require_authorized(spec["policy"], [selector], checkout=checkout)
                except ValueError:
                    pass
                else:
                    visible_new = True
        if (
            not path.is_file()
            or path.is_symlink()
            or path.resolve(strict=True) != path
            or not (tracked or visible_new)
        ):
            raise GateAdmissionError(
                "required selector must be one fixed tracked candidate file "
                "or authorized new v2 source: " + selector,
                selector=selector,
            )
        result.append(
            {
                "selector": selector,
                "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    return result


def _report_source(raw: str, checkout: Path, roots: list[Path], selectors: set[str]) -> str | None:
    if not isinstance(raw, str) or not raw:
        return None
    path = Path(raw)
    matches = set()
    for root in roots:
        candidate = path if path.is_absolute() else root / path
        candidate = candidate.resolve()
        if candidate.is_relative_to(checkout):
            relative = candidate.relative_to(checkout).as_posix()
            if relative in selectors:
                matches.add(relative)
    if len(matches) > 1:
        raise ValueError("test report source attribution is ambiguous")
    return next(iter(matches), None)


def selector_evidence(
    recipe: dict,
    checkout: Path,
    sources: list[dict],
    *,
    junit: bytes | None = None,
    playwright: str | None = None,
) -> list[dict]:
    """Require actual passing cases from every selected source, without basename guesses."""
    if not sources:
        return []
    checkout = checkout.resolve(strict=True)
    names = {source["selector"] for source in sources}
    counts = {name: {"passed": 0, "failed": 0, "skipped": 0} for name in names}
    project = checkout / recipe.get("selector_project", recipe.get("cwd", "."))
    roots = [checkout, project]
    cases = []
    if junit is not None:
        if (
            len(junit) > 50 * 1024 * 1024
            or b"<!DOCTYPE" in junit.upper()
            or b"<!ENTITY" in junit.upper()
        ):
            raise ValueError("selector JUnit report is unsupported or exceeds its bound")
        root = ET.fromstring(junit)
        if root.tag not in {"testsuites", "testsuite"}:
            raise ValueError("selector evidence has no JUnit suites")
        for suite in root.iter("testsuite"):
            for case in suite.findall("testcase"):
                origin = (
                    case.get("file")
                    or suite.get("file")
                    or case.get("classname")
                    or suite.get("name")
                )
                # pytest JUnit's default classname is the relative Python module.
                if isinstance(origin, str) and "/" not in origin and origin.startswith("tests."):
                    origin = origin.replace(".", "/") + ".py"
                outcome = (
                    "failed"
                    if case.find("failure") is not None or case.find("error") is not None
                    else "skipped"
                    if case.find("skipped") is not None
                    else "passed"
                )
                cases.append((origin, outcome))
    elif playwright is not None:
        decoder = json.JSONDecoder()
        reports = []
        for match in re.finditer(r"(?m)^\s*\{", playwright):
            try:
                value, _ = decoder.raw_decode(playwright[match.start() :].lstrip())
            except ValueError:
                continue
            if isinstance(value, dict) and {"config", "suites", "stats"} <= value.keys():
                reports.append(value)
        if len(reports) != 1:
            raise ValueError("required Playwright JSON report is missing or ambiguous")
        report = reports[0]
        if report.get("errors"):
            raise ValueError("Playwright selector report records runner errors")
        root_dir = Path(report.get("config", {}).get("rootDir", ""))
        if not root_dir.is_absolute() or not root_dir.resolve().is_relative_to(checkout):
            raise ValueError("Playwright report source root escaped its candidate")
        roots.append(root_dir)

        def visit(suite):
            for item in suite.get("specs", []):
                origin = item.get("file", suite.get("file"))
                for case in item.get("tests", []):
                    results = case.get("results", [])
                    passed = (
                        case.get("expectedStatus", "passed") == "passed"
                        and case.get("status") == "expected"
                        and len(results) == 1
                        and results[0].get("status") == "passed"
                    )
                    outcome = (
                        "passed"
                        if passed
                        else "skipped"
                        if not results or all(r.get("status") == "skipped" for r in results)
                        else "failed"
                    )
                    cases.append((origin, outcome))
            for child in suite.get("suites", []):
                visit(child)

        for suite in report["suites"]:
            visit(suite)
    else:
        raise ValueError("named selectors require retained per-file test report evidence")
    for origin, outcome in cases:
        selector = _report_source(origin, checkout, roots, names)
        if selector:
            counts[selector][outcome] += 1
    receipts = []
    for source in sources:
        values = counts[source["selector"]]
        if not values["passed"] or values["failed"] or values["skipped"]:
            raise ValueError(
                "required selector contributed no complete passing evidence: " + source["selector"]
            )
        if hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest() != source["sha256"]:
            raise ValueError("selected test source changed during verification")
        receipts.append({**source, **values})
    return receipts


def admission_diagnostic(
    spec: dict, chunk_id: str, candidate: dict, error: GateAdmissionError, evidence: list[dict]
) -> dict:
    identity = spec.get("feature_plan_revision", {})
    if (
        not isinstance(error, GateAdmissionError)
        or not evidence
        or any(
            not isinstance(e, dict)
            or set(e) != {"path", "sha256"}
            or not isinstance(e["path"], str)
            or not isinstance(e["sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", e["sha256"])
            for e in evidence
        )
        or not candidate.get("id")
    ):
        raise ValueError("planning diagnostic requires exact candidate and durable evidence")
    return {
        "version": 1,
        "kind": "planning_defect",
        "category": "gate_prerequisite",
        "chunk_id": chunk_id,
        "plan_revision": identity.get("plan_revision", 1),
        "plan_sha256": identity.get("plan_digest") or digest(json.loads(spec["accepted_plan"])),
        "candidate_id": candidate["id"],
        "evidence": deepcopy(evidence),
        "detail": str(error),
    }


def bind_selector_report(recipe: dict, folder: Path) -> dict:
    """Bind built-in runner reports only to the check's owned artifact folder."""
    if not recipe.get("required_selectors"):
        return recipe
    runner, start = _runner(recipe)
    if runner == "node":
        # The built-in Node 22 JUnit reporter records classname="test" and no
        # source path. It cannot qualify any exact file, even when cases pass.
        raise GateAdmissionError(
            "Node --test named selectors lack admitted per-file report provenance"
        )
    if recipe.get("junit_required"):
        return recipe
    if runner not in {"pytest", "vitest"}:
        raise ValueError("selected local recipe requires an owned JUnit report")
    result = deepcopy(recipe)
    from .delivery_resources import private_directory

    private_directory(folder / "pytest-artifacts")
    report = folder / "pytest-artifacts" / "junit.xml"
    if runner == "pytest":
        flags = ["--junitxml=" + str(report), "-o", "junit_family=xunit1"]
    else:
        flags = ["--reporter=junit", "--outputFile=" + str(report)]
    result["argv"] = [*result["argv"][:start], *flags, *result["argv"][start:]]
    result["junit_required"] = True
    return result


def retained_junit(reference: dict, candidate_id: str, state_dir: Path) -> bytes:
    from .delivery_check_evidence import verify_manifest

    manifest = verify_manifest(reference, candidate_id, state_dir)
    reports = [item for item in manifest["artifacts"] if item["relative_path"] == "junit.xml"]
    if len(reports) != 1:
        raise ValueError("required per-selector JUnit report is missing")
    return Path(reports[0]["path"]).read_bytes()


def retain_admission_failure(
    spec: dict, recipe: dict, folder: Path, candidate: dict, error: GateAdmissionError
) -> dict:
    """Retain a concrete source-admission observation before launching tests."""
    from .delivery_resources import private_directory, write_private

    private_directory(folder)
    observation = {
        "version": 1,
        "candidate_id": candidate["id"],
        "policy_digest": spec["policy_digest"],
        "recipe_sha256": digest(recipe),
        "required_selectors": recipe.get("required_selectors", []),
        "error": error.diagnostic,
    }
    path = folder / ("selector-admission-" + digest(observation) + ".json")
    if path.exists():
        from .delivery_resources import read_private

        if read_private(path) != observation:
            raise ValueError("selector admission evidence changed")
    else:
        write_private(path, observation)
    evidence = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    result = {
        "state": "failed",
        "cleanup": "confirmed",
        "launched": False,
        "candidate_id": candidate["id"],
        "source_unchanged": True,
        "diagnostic": str(error),
        "selector_admission": evidence,
        "results": [],
    }
    worker = spec.get("feature_worker")
    # A worker failing to create its own expected test is an ordinary product
    # repair. Only a proven placement/dependency flaw is a planning trigger.
    if worker and error.planning_defect and spec.get("gate_selections_version") == 2:
        result["planning_defect"] = admission_diagnostic(
            spec, worker["chunk_id"], candidate, error, [evidence]
        )
    return result
