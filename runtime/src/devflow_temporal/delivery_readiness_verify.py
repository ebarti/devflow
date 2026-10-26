"""Authenticate readiness observations against their native journals and current owners."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .contracts import digest
from .delivery_resources import read_private


def _native(spec, result, folder, argv, cwd, environment, timeout):
    from .delivery_readiness import ReadinessError

    journal = folder / "native-process.json"
    if (
        result.get("journal") != str(journal)
        or result.get("exit_code") != 0
        or result.get("cleanup") != "observed-native-confirmed"
        or result.get("monitoring_complete") is not True
        or result.get("stdio_drained") is not True
        or any(result.get(k) is not False for k in ("cancelled", "timed_out", "listener_conflict"))
        or result.get("interrupted_monitor", False) is not False
    ):
        raise ReadinessError("readiness native result is incomplete")
    saved, launch = read_private(journal), read_private(folder / "launch.json")
    from .payload import payload_digest

    identity = saved.get("runtime_identity") or {}
    if identity.get("source_root") != str(Path(__file__).resolve().parents[3]) or identity.get(
        "runtime_payload_sha256"
    ) != payload_digest(Path(__file__).parent):
        raise ReadinessError("readiness native runtime identity changed")
    intent = {
        "run_id": spec["run_id"],
        "policy_digest": spec["policy_digest"],
        "argv": argv,
        "cwd": str(cwd),
        "environment_sha256": digest(environment),
        "timeout": timeout,
        "ports": [],
    }
    if (
        saved.get("phase") != "finished"
        or saved.get("result") != result
        or saved.get("intent") != intent
        or launch != {"argv": argv, "environment": environment}
    ):
        raise ReadinessError("readiness native intent or result changed")
    log = folder / "process.log"
    raw = read_private(log, raw=True)
    if (
        len(raw) > 8 * 1024 * 1024
        or result.get("log") != str(log)
        or hashlib.sha256(raw).hexdigest() != result.get("log_sha256")
    ):
        raise ReadinessError("readiness native log changed")
    return raw


def preparation(store, spec, candidate, requirements, receipt):
    """Reconstruct exact prerequisite recipes and each owned native invocation."""
    from .delivery_broker import DeliveryBroker
    from .delivery_configured_resources import implementation_prerequisites
    from .delivery_plan_checks import planned_checks
    from .delivery_readiness import (
        ReadinessError,
        _python_environment_fingerprint,
        plan_evidence_folder,
    )
    from .delivery_readiness_pnpm import _fetch_authority
    from .delivery_sandbox import native_check_argv, prepare_native_check

    stage, iteration, attempt = receipt["stage"], receipt["iteration"], receipt["attempt"]
    observed = receipt["observations"].get(stage)
    if (
        not isinstance(observed, dict)
        or observed.get("state") != "passed"
        or observed.get("cleanup") != "confirmed"
        or observed.get("candidate") != candidate
        or observed.get("requirement_sha256") != digest(requirements)
    ):
        raise ReadinessError("readiness prerequisite observation is not a confirmed pass")
    result = observed.get("result") or {}
    if (
        result.get("state") != "passed"
        or result.get("candidate_id") != candidate["id"]
        or result.get("source_unchanged") is not True
    ):
        raise ReadinessError("readiness prerequisite candidate or result changed")
    broker = DeliveryBroker(store, spec)
    broker.checkout = Path(receipt["workspace"])
    folder = (
        broker.evidence_dir / "implementation-preparation" / ("configured-" + str(attempt))
        if stage == "configured"
        else plan_evidence_folder(spec, broker.evidence_dir, iteration, candidate, attempt)
    )
    recipes = implementation_prerequisites(spec)
    if stage == "plan":
        recipes += [
            check
            for check in planned_checks(spec, broker.checkout, folder, preparation=True)
            if check["id"].startswith("planned-python-dependencies-")
        ]
    rows = result.get("results")
    if not isinstance(rows, list) or [row.get("id") for row in rows] != [r["id"] for r in recipes]:
        raise ReadinessError("readiness prerequisite list differs from frozen recipes")
    if result.get("native_addon_preparation"):
        raise ReadinessError("readiness native-addon proof is not yet supported")
    interpreters = {
        item["project"]: item["interpreter"] for item in result.get("python_interpreters", [])
    }
    for recipe, row in zip(recipes, rows, strict=True):
        if (
            row.get("passed") is not True
            or row.get("cleanup") != "confirmed"
            or row.get("argv") != recipe["argv"]
            or row.get("interrupted_native_processes")
        ):
            raise ReadinessError("readiness prerequisite result differs from frozen recipe")
        cwd = (broker.checkout / recipe.get("cwd", ".")).resolve(strict=True)
        if broker.checkout not in (cwd, *cwd.parents) or row.get("cwd") != str(cwd):
            raise ReadinessError("readiness prerequisite cwd changed")
        dependency = row.get("dependency_preparation")
        if dependency:
            _fetch_authority(spec, broker.checkout, dependency)
        dependency_store = Path(dependency["store"]) if dependency else None
        profile, environment = prepare_native_check(
            spec,
            broker.checkout,
            folder,
            recipe,
            dependency_store=dependency_store,
            resources=broker.run_resources(),
        )
        if recipe.get("id", "").startswith("planned-python-dependencies-"):
            interpreter = interpreters.get(recipe.get("cwd"))
            if interpreter:
                envroot = Path(interpreter).parent.parent
                if not envroot.is_relative_to(
                    Path(spec["state_dir"]) / "transient/implementation-python"
                ):
                    raise ReadinessError("readiness Python environment escaped its owned root")
                environment["UV_PROJECT_ENVIRONMENT"] = str(envroot)
        argv = [str(dependency_store) if item == "/store" else item for item in recipe["argv"]]
        command = native_check_argv(spec, profile, cwd, argv)
        native = row.get("native_process") or {}
        native_folder = Path(native.get("journal", "")).parent
        if native_folder.parent != folder / recipe["id"] or native_folder.name not in {
            "native",
            "native-retry",
        }:
            raise ReadinessError("readiness prerequisite native folder changed")
        _native(
            spec,
            native,
            native_folder,
            command,
            cwd,
            environment,
            int(recipe.get("timeout_seconds", 600)),
        )
    projects = sorted(
        {
            str(Path(name).parent)
            for recipe in recipes
            for name in recipe.get("generated_directories", [])
            if Path(name).name == ".venv"
        }
    )
    fingerprints = [
        _python_environment_fingerprint(spec, project, result, workspace=broker.checkout)
        for project in projects
    ]
    if fingerprints != observed.get("python_environments"):
        raise ReadinessError("readiness Python dependency environment changed")


def browser(store, spec, candidate, requirements, receipt):
    """Revalidate the exact selected binaries, loaded closure and both native launches."""
    from .delivery_broker import DeliveryBroker
    from .delivery_readiness import (
        _BROWSER_PROBE,
        _BROWSER_RESOLVE,
        ReadinessError,
        _file_reference,
    )
    from .delivery_readiness_pnpm import authenticate_playwright
    from .delivery_resources import RunResources
    from .delivery_sandbox import prepare_browser_qa, trusted_local

    observed = receipt["observations"].get("browser")
    if (
        not isinstance(observed, dict)
        or observed.get("state") != "passed"
        or observed.get("cleanup") != "confirmed"
        or observed.get("candidate") != candidate
        or observed.get("requirement_sha256") != digest(requirements)
    ):
        raise ReadinessError("browser readiness is not a confirmed pass")
    broker = DeliveryBroker(store, spec)
    broker.checkout = Path(receipt["workspace"])
    folder = (
        Path(spec["state_dir"])
        / "readiness/browser"
        / str(requirements["declaration"]["generation"])
        / str(receipt["attempt"])
        / str(receipt["iteration"])
    )
    qa = requirements["browser_qa"]
    cwd = (broker.checkout / qa.get("cwd", ".")).resolve(strict=True)
    profile, environment = prepare_browser_qa(
        spec, broker.checkout, folder, RunResources(spec).browser_scratch(), qa
    )
    node = observed.get("node") or {}
    if (
        _file_reference(node.get("path", "")) != node
        or digest(environment) != observed.get("environment_sha256")
        or hashlib.sha256(profile.read_bytes()).hexdigest() != observed.get("profile_sha256")
    ):
        raise ReadinessError("browser readiness executable or environment changed")

    def args(script, operands):
        command = [node["path"], "-e", script, *operands]
        return (
            command
            if trusted_local(spec)
            else ["/usr/bin/sandbox-exec", "-f", str(profile), *command]
        )

    first = _native(
        spec,
        observed.get("resolution_process") or {},
        folder / "resolve-native",
        args(_BROWSER_RESOLVE, [str(broker.checkout)]),
        cwd,
        environment,
        60,
    )
    second = _native(
        spec,
        observed.get("native_process") or {},
        folder / "browser-native",
        args(
            _BROWSER_PROBE,
            [str(broker.checkout), json.dumps(requirements["declaration"]["browser_engines"])],
        ),
        cwd,
        environment,
        60,
    )

    def payload(raw, prefix):
        lines = [
            line[len(prefix) :] for line in raw.decode().splitlines() if line.startswith(prefix)
        ]
        if len(lines) != 1:
            raise ReadinessError("browser readiness log lacks its exact inventory")
        return json.loads(lines[0])

    entry = payload(first, "DEVFLOW_READINESS_ENTRY=")
    data = payload(second, "DEVFLOW_READINESS_BROWSER=")
    if (
        not isinstance(data, dict)
        or data.get("entry") != entry
        or data.get("modules") != observed.get("modules")
        or set(data.get("executables", {})) != set(requirements["declaration"]["browser_engines"])
        or {name: _file_reference(path) for name, path in data["executables"].items()}
        != observed.get("binaries")
        or _file_reference(entry, maximum=16 * 1024 * 1024) != observed.get("package_entry")
    ):
        raise ReadinessError("browser readiness pinned package or executable changed")
    proof = authenticate_playwright(
        spec, broker.checkout, observed.get("dependency_preparation"), entry, data["modules"]
    )
    if proof != observed.get("package_proof"):
        raise ReadinessError("browser readiness loaded package closure changed")


def temporal(store, spec, candidate, requirements, receipt):
    """Reauthenticate the exact pinned server/CLI probe and its owned Python environment."""
    from .delivery_broker import DeliveryBroker
    from .delivery_native_process import NativeProcess
    from .delivery_readiness import (
        _TEMPORAL_PROBE,
        ReadinessError,
        _file_reference,
        _python_environment_fingerprint,
        _temporal_interpreter,
    )
    from .delivery_sandbox import prepare_native_check
    from .delivery_temporal_prerequisites import check_launch, launch_authority

    observed = receipt["observations"].get("temporal")
    if (
        not isinstance(observed, dict)
        or observed.get("state") != "passed"
        or observed.get("cleanup") != "confirmed"
        or observed.get("candidate") != candidate
        or observed.get("requirement_sha256") != digest(requirements)
        or observed.get("preparation") != receipt["observations"][receipt["stage"]]["result"]
    ):
        raise ReadinessError("Temporal readiness is not a confirmed pass")
    broker = DeliveryBroker(store, spec)
    broker.checkout = Path(receipt["workspace"])
    proof = launch_authority(spec)
    check = proof["check"]
    evidence = broker.evidence_dir / "checks" / str(receipt["iteration"])
    profile, environment = prepare_native_check(
        spec, broker.checkout, evidence, check, resources=broker.run_resources()
    )
    interpreter = _temporal_interpreter(
        spec, proof, preparation=observed["preparation"], workspace=broker.checkout
    )
    if (
        str(interpreter) != observed.get("interpreter")
        or _file_reference(str(interpreter.resolve(strict=True)))["sha256"]
        != observed.get("interpreter_sha256")
        or digest(environment) != observed.get("environment_sha256")
        or profile != observed.get("profile")
    ):
        raise ReadinessError("Temporal readiness environment or interpreter changed")
    if _python_environment_fingerprint(
        spec, proof["declaration"]["python_project"], observed["preparation"],
        workspace=broker.checkout
    ) != observed.get("python_environment"):
        raise ReadinessError("Temporal readiness Python dependency graph changed")
    cwd = (broker.checkout / check.get("cwd", ".")).resolve(strict=True)
    folder = evidence / check["id"] / ("readiness-native-" + str(receipt["attempt"]))
    argv = [str(interpreter), "-I", "-B", "-c", _TEMPORAL_PROBE]
    process = NativeProcess(spec, folder, argv=argv, cwd=cwd, environment=environment, timeout=180)
    process.readiness_temporal = True
    check_launch(process)
    raw = _native(spec, observed.get("native_process") or {}, folder, argv, cwd, environment, 180)
    prefix = "DEVFLOW_READINESS_TEMPORAL="
    lines = [line[len(prefix) :] for line in raw.decode().splitlines() if line.startswith(prefix)]
    expected = {
        "sdk": "1.26.0",
        "server": environment["JOBCTRL_TEMPORAL_TEST_SERVER_PATH"],
        "cli": environment["JOBCTRL_TEMPORAL_CLI_PATH"],
    }
    if len(lines) != 1 or json.loads(lines[0]) != expected:
        raise ReadinessError("Temporal readiness backend result changed")


def tools(store, spec, candidate, requirements, receipt):
    """Reauthenticate every supported check executable probe in its native profile."""
    from .delivery_broker import DeliveryBroker
    from .delivery_native_process import NativeProcess
    from .delivery_readiness import (
        ReadinessError,
        _file_reference,
        _node_environment_fingerprint,
        _planned_tool_recipes,
        _tool_command,
    )
    from .delivery_sandbox import native_check_argv, prepare_native_check
    from .delivery_temporal_prerequisites import check_launch

    observation = receipt["observations"].get("tools")
    if (
        not isinstance(observation, dict)
        or observation.get("state") != "passed"
        or observation.get("cleanup") != "confirmed"
        or observation.get("candidate") != candidate
        or observation.get("requirement_sha256") != digest(requirements)
    ):
        raise ReadinessError("readiness check executable observations are incomplete")
    broker = DeliveryBroker(store, spec)
    broker.checkout = Path(receipt["workspace"])
    configured = {digest(check) for check in requirements["configured"]}
    temporal_ids = set(requirements["declaration"]["temporal_checks"])
    recipes = [
        item
        for item in requirements["check_recipes"]
        if digest(item["check"]) not in configured
        and not (item["check"].get("id") in temporal_ids and item["stage"] != "checks")
    ]
    if receipt["stage"] == "plan":
        recipes.extend(_planned_tool_recipes(
            broker, candidate, receipt["iteration"], receipt["attempt"],
            receipt["observations"]["plan"]))
    needs_graph = any(
        Path(item["check"]["argv"][0]).name in {"vitest", "eslint", "tsc", "corepack"}
        or (
            Path(item["check"]["argv"][0]).name == "node"
            and len(item["check"]["argv"]) > 1
            and item["check"]["argv"][1] == "--test"
        )
        for item in recipes
    )
    expected_graph = _node_environment_fingerprint(spec, broker.checkout) if needs_graph else None
    if observation.get("node_environment") != expected_graph:
        raise ReadinessError("readiness Node dependency environment changed")
    rows = observation.get("items")
    if not isinstance(rows, list) or len(rows) != len(recipes):
        raise ReadinessError("readiness check executable inventory changed")
    for item, row in zip(recipes, rows, strict=True):
        check, stage = item["check"], item["stage"]
        if (
            row.get("stage") != stage
            or row.get("check_id") != check["id"]
            or row.get("recipe_sha256") != digest(check)
        ):
            raise ReadinessError("readiness check executable recipe changed")
        evidence = broker.evidence_dir / stage / str(receipt["iteration"])
        cwd = (broker.checkout / check.get("cwd", ".")).resolve(strict=True)
        profile, environment = prepare_native_check(
            spec, broker.checkout, evidence, check, resources=broker.run_resources(),
            readiness_probe=stage == "plan-checks"
        )
        environment["COREPACK_ENABLE_NETWORK"] = "0"
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        command = _tool_command(check, cwd, environment)
        if command is None:
            raise ReadinessError("readiness check executable disappeared")
        name = Path(check["argv"][0]).name
        import re

        python_module = (
            name in {"pytest", "ruff"}
            or re.fullmatch(r"python(?:3(?:\.\d+)?)?", name)
            and len(check["argv"]) > 2
            and check["argv"][1] == "-m"
        )
        if python_module:
            roots = [
                Path(value["root"])
                for value in receipt["observations"][receipt["stage"]].get(
                    "python_environments", []
                )
            ]
            if not any(Path(command[0]).is_relative_to(root / "bin") for root in roots):
                raise ReadinessError("readiness Python tool left its locked environment")
        executable = _file_reference(str(Path(command[0]).resolve(strict=True)))
        if executable != row.get("executable"):
            raise ReadinessError("readiness check executable changed")
        argv = native_check_argv(spec, profile, cwd, command)
        folder = evidence / check["id"] / ("readiness-tool-" + str(receipt["attempt"]))
        if check["id"] in temporal_ids:
            process = NativeProcess(
                spec, folder, argv=argv, cwd=cwd, environment=environment, timeout=60
            )
            process.readiness_tool = True
            check_launch(process)
        _native(spec, row.get("native_process") or {}, folder, argv, cwd, environment, 60)
