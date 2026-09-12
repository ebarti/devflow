"""The documented direct helper must preserve a materialized, marked release."""

import hashlib
import json
import os
import stat
import subprocess
import sys
from pathlib import Path

from test_intake_source_console import synthetic_capture
from test_user_request_console import installed as installed  # noqa: F401

from devflow.errors import WorkflowError
from devflow.installation import installed_release
from devflow.validation import digest


def release_inventory(release):
    return {str(path.relative_to(release)): {
        "mode": stat.S_IMODE(path.stat().st_mode),
        "kind": "directory" if path.is_dir() else "file",
        **({"sha256": hashlib.sha256(path.read_bytes()).hexdigest()} if path.is_file() else {}),
    } for path in sorted(release.rglob("*"))}


def test_documented_mapping_preserves_installed_release_and_next_skill_resolution(tmp_path, installed):
    release, revision = installed
    metadata = installed_release(release)
    before = release_inventory(release)
    marker = (release / ".devflow-release.json").read_bytes()
    assert all(not item["mode"] & 0o222 for item in before.values() if item["kind"] == "file")
    assert not list(release.rglob("__pycache__"))
    environment = {key: value for key, value in os.environ.items()
                   if key not in {"PYTHONDONTWRITEBYTECODE", "PYTHONPYCACHEPREFIX"}}
    environment["PYTHONPATH"] = str(release / "src")
    state_dir = tmp_path / "private"
    resolve_argv = [sys.executable, "-B", "-m", "devflow.cli", "skill", "resolve",
                    "--request-file", "-", "--repository", str(tmp_path), "--state-dir", str(state_dir),
                    "--release-root", str(release.parents[1]), "--json"]

    def resolve():
        result = subprocess.run(resolve_argv, input=json.dumps({"name": "devflow-coordinating"}),
                                capture_output=True, text=True, check=False, timeout=30,
                                cwd=tmp_path, env=environment)
        return {"argv": resolve_argv, "exit_code": result.returncode,
                "stdout": result.stdout, "stderr": result.stderr}

    initial_resolution = resolve()
    assert initial_resolution["exit_code"] == 0, initial_resolution
    recipe = release / "skills/devflow-defining-work/references/source-lineage.md"
    code = recipe.read_text().split("```python\n")[1].split("```")[0]
    capture, other = synthetic_capture()
    script = (
        "import json,sys\n"
        "initial_no_bytecode=sys.dont_write_bytecode\n"
        "initial_cache_prefix=sys.pycache_prefix\n"
        "capture,other_consumed_observations=json.load(sys.stdin)\n" + code
        + "\nimport devflow\n"
        "print(json.dumps({'source':source,'module':devflow.__file__,"
        "'initial_no_bytecode':initial_no_bytecode,'initial_cache_prefix':initial_cache_prefix,"
        "'final_no_bytecode':sys.dont_write_bytecode}))\n"
    )
    helper = subprocess.run([sys.executable, "-c", script], input=json.dumps([capture, other]),
                            capture_output=True, text=True, check=False, timeout=30,
                            cwd=tmp_path, env=environment)
    after_helper = release_inventory(release)
    try:
        checked = installed_release(release)
        integrity_error = None
    except WorkflowError as exc:
        checked, integrity_error = None, exc.as_dict()
    final_resolution = resolve()
    after_resolution = release_inventory(release)
    evidence = {
        "release": str(release), "revision": revision, "metadata": metadata,
        "environment_guards_removed": ["PYTHONDONTWRITEBYTECODE", "PYTHONPYCACHEPREFIX"],
        "initial_resolution": initial_resolution,
        "helper": {"argv": [sys.executable, "-c", script], "executed_recipe": code,
                   "capture": capture, "other_consumed_observations": other,
                   "exit_code": helper.returncode, "stdout": helper.stdout, "stderr": helper.stderr},
        "inventory_before": before, "inventory_after_helper": after_helper,
        "inventory_after_resolution": after_resolution, "integrity_error": integrity_error,
        "final_resolution": final_resolution,
        "cache_files": [str(path.relative_to(release)) for path in release.rglob("*.pyc")],
    }
    (tmp_path / "installed-helper-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    assert helper.returncode == 0, evidence["helper"]
    output = json.loads(helper.stdout)
    assert Path(output["module"]).resolve() == release / "src/devflow/__init__.py"
    assert output["initial_no_bytecode"] is False and output["initial_cache_prefix"] is None
    assert after_helper == after_resolution == before, evidence
    assert (release / ".devflow-release.json").read_bytes() == marker
    assert not evidence["cache_files"] and not list(release.rglob("__pycache__"))
    assert output["final_no_bytecode"] is True
    assert checked == metadata and integrity_error is None
    assert output["source"]["lineage"][:-1] == capture["payload"]["source_lineage"] + other
    assert output["source"]["lineage"][-1]["origin"] == "unknown"
    assert output["source"]["consumed_digest"] == digest(output["source"]["lineage"])
    assert final_resolution["exit_code"] == 0, final_resolution
    resolved = json.loads(final_resolution["stdout"])["result"]
    assert resolved["package_revision"] == revision and not resolved["unmanaged_package"]
    assert resolved["skill"]["path"] == str(release / "skills/devflow-coordinating/SKILL.md")
    assert resolved["skill"]["sha256"] == before["skills/devflow-coordinating/SKILL.md"]["sha256"]
    assert not state_dir.exists()
