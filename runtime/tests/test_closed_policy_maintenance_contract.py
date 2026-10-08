"""Memory-only historical validation; no physical fixture imports."""

from __future__ import annotations

import ast
import gzip
import hashlib
import json
import unittest
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

RUNTIME = Path(__file__).resolve().parents[1]
SOURCE = RUNTIME / "src/devflow_temporal"


def source_functions(path, names, namespace):
    """Compile only the designated pure functions from this checkout's source."""
    tree = ast.parse(path.read_text(), filename=str(path))
    functions = [
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    assert {node.name for node in functions} == set(names)
    module = ast.Module(
        body=[
            ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
            *functions,
        ],
        type_ignores=[],
    )
    ast.fix_missing_locations(module)
    exec(compile(module, str(path), "exec"), namespace)


class HistoricalPolicyMaintenanceContract(unittest.TestCase):
    def test_actual_historical_validator_preserves_inputs_and_refuses_changed_authority(self):
        recorded = json.loads(
            gzip.decompress(
                (RUNTIME / "tests/fixtures/policy-c04-admitted-row.json.gz").read_bytes()
            )
        )
        namespace = {"hashlib": hashlib, "json": json, "deepcopy": deepcopy, "Path": Path}
        source_functions(SOURCE / "contracts.py", {"canonical_json", "digest"}, namespace)
        source_functions(
            SOURCE / "delivery_policy_recovery.py", {"amended_config", "effective_spec"}, namespace
        )
        for tamper in (None, "missing", "original", "recovery", "maximum", "config"):
            with self.subTest(tamper=tamper):
                original = deepcopy(recorded["original"])
                recovery = json.loads(recorded["run"]["recovery_json"])
                grant = deepcopy(recorded["grant"])
                content = recorded["trusted_config_bytes"].encode()

                if tamper == "missing":
                    grant = None
                elif tamper in {"original", "recovery"}:
                    field = "original_spec_digest" if tamper == "original" else "recovery_digest"
                    grant[field] = "f" * 64
                elif tamper == "maximum":
                    grant["maximum_iteration"] += 1
                elif tamper == "config":
                    content += b" "

                def private_bytes(
                    path, root, original=original, recovery=recovery, content=content
                ):
                    self.assertEqual(path, Path(recovery["effective_spec"]["config_path"]))
                    self.assertEqual(root, Path(original["state_dir"]).parents[1])
                    return content  # Historical file transport only; never access that pathname.

                def load(path, original=original, recovery=recovery):
                    self.assertIn(
                        str(path),
                        {original["config_path"], recovery["effective_spec"]["config_path"]},
                    )
                    field = (
                        "original_config_bytes"
                        if str(path) == original["config_path"]
                        else "trusted_config_bytes"
                    )
                    return SimpleNamespace(path=path, raw=json.loads(recorded[field]))

                namespace.update(
                    _private_bytes=private_bytes, DeliveryConfig=SimpleNamespace(load=load)
                )
                before = deepcopy((original, recovery, grant))
                validator = namespace["effective_spec"]
                if tamper:
                    with self.assertRaisesRegex(
                        ValueError, "not durable|configuration bytes changed"
                    ):
                        validator(None, original, recovery, grant)
                else:
                    self.assertEqual(
                        validator(None, original, recovery, grant), recorded["effective_spec"]
                    )
                self.assertEqual((original, recovery, grant), before)
