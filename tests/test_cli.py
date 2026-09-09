import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

from devflow.cli import main


def test_invalid_record_is_structured_and_does_not_initialize_state(tmp_path, capsys):
    request = tmp_path / "record.json"
    request.write_text('{"record_type":"gate_result","status":"PASS"}')
    state = tmp_path / "private"
    assert main(["validate", "record", "--request-file", str(request), "--state-dir", str(state), "--json"]) == 2
    result = json.loads(capsys.readouterr().out)
    assert not result["ok"]
    assert not state.exists()


def test_prepare_is_read_only_and_does_not_invent_authority(tmp_path, capsys):
    request = tmp_path / "request.json"
    request.write_text('{"record":{"title":"Unspecified feature"}}')
    state = tmp_path / "private"
    assert main(["work", "prepare", "--request-file", str(request), "--state-dir", str(state), "--json"]) == 0
    result = json.loads(capsys.readouterr().out)["result"]
    assert result["missing"] and result["authority_required"]
    assert not result["ready"] and not state.exists()


def test_doctor_missing_enrollment_is_honest_and_read_only(tmp_path, capsys):
    assert main(["doctor", "--repository", str(tmp_path), "--state-dir", str(tmp_path / "private"), "--json"]) == 2
    result = json.loads(capsys.readouterr().out)["result"]
    assert result["status"] == "BLOCKED"
    assert not (tmp_path / "private").exists()


def test_installer_manifest_cannot_supply_its_own_approval(tmp_path, capsys):
    request = tmp_path / "manifest.json"
    request.write_text('{"owned_paths":["/some/target"],"approved_plan_id":"invented"}')
    assert main(["install", "apply", "--request-file", str(request), "--json"]) == 2
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "installation_scope_required"


def test_unknown_command_cannot_create_live_state(tmp_path, capsys):
    assert main(["invented", "--repository", str(tmp_path), "--state-dir", str(tmp_path / "private"), "--json"]) == 2
    assert not (tmp_path / "private").exists()
    assert not json.loads(capsys.readouterr().out)["ok"]


def test_actual_console_entry_and_valid_example():
    root = Path(__file__).parents[1]
    result = subprocess.run([sys.executable, "-m", "devflow.cli", "validate", "record",
                             "--request-file", str(root / "docs/design/work-contract.example.json"),
                             "--json"], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["result"]["record_type"] == "work_contract"


def test_exact_decimal_json_survives_cli_boundary(tmp_path, capsys):
    request = tmp_path / "report.json"
    request.write_text('{"records":[],"cutoff":"2026-09-09T00:00:00Z"}')
    assert main(["report", "usage", "--request-file", str(request), "--json"]) == 0
    assert json.loads(capsys.readouterr().out, parse_float=Decimal)["ok"]
