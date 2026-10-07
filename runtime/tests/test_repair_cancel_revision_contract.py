"""Source-bound caller sequencing and cancellation contracts using memory only."""

from __future__ import annotations

import ast
import hashlib
import json
import unittest
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

RUNTIME = Path(__file__).resolve().parents[1]
SOURCE = RUNTIME / "src/devflow_temporal"


def compile_nodes(path, nodes, namespace):
    definitions = deepcopy(nodes)
    for node in definitions:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            node.decorator_list = []  # Do not import or invoke Temporal decorators.
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[
        ast.alias(name="annotations")], level=0), *definitions], type_ignores=[])
    ast.fix_missing_locations(module)
    exec(compile(module, str(path), "exec"), namespace)


def source_contracts():
    namespace = {"json": json, "hashlib": hashlib}
    contracts = SOURCE / "contracts.py"
    nodes = ast.parse(contracts.read_text()).body
    compile_nodes(contracts, [n for n in nodes if isinstance(n, ast.FunctionDef)
                             and n.name in {"canonical_json", "digest"}], namespace)
    workflow_path = SOURCE / "delivery_workflow.py"
    workflow = next(n for n in ast.parse(workflow_path.read_text()).body
                    if isinstance(n, ast.ClassDef) and n.name == "DeliveryWorkflow")
    compile_nodes(workflow_path, [n for n in workflow.body
                  if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                  and n.name in {"status", "cancel"}], namespace)
    store_path = SOURCE / "delivery_store.py"
    store = next(n for n in ast.parse(store_path.read_text()).body
                 if isinstance(n, ast.ClassDef) and n.name == "DeliveryStore")
    begin = next(n for n in store.body if isinstance(n, ast.FunctionDef)
                 and n.name == "begin_mutation")
    prior = next(n for n in ast.walk(begin) if isinstance(n, ast.If)
                 and isinstance(n.test, ast.Name) and n.test.id == "prior")
    check = ast.FunctionDef(name="check_prior", args=ast.arguments(
        posonlyargs=[], args=[ast.arg(arg=name) for name in
                             ("prior", "run_id", "kind", "request_digest")],
        kwonlyargs=[], kw_defaults=[], defaults=[]), body=[prior], decorator_list=[])
    compile_nodes(store_path, [check], namespace)  # No SQL, DB or store construction.
    caller_path = RUNTIME / "tests/test_delivery_store.py"
    caller = next(n for n in ast.parse(caller_path.read_text()).body
                  if isinstance(n, ast.AsyncFunctionDef) and n.name ==
                  "test_public_repair_grant_resumes_original_session_and_runs_broker_gates")
    cancel_block = next(n for n in ast.walk(caller) if isinstance(n, ast.AsyncWith)
                        and isinstance(n.items[0].optional_vars, ast.Name)
                        and n.items[0].optional_vars.id == "browser"
                        and any(isinstance(item, ast.Constant)
                                and item.value == "/api/runs/run-1/cancel" for item in ast.walk(n)))
    # Reject a broadened slice before execution; only memory transport calls
    # belong here, never an enclosing Worker/server/fixture block.
    for node in ast.walk(ast.Module(body=cancel_block.body, type_ignores=[])):
        if isinstance(node, ast.Call):
            assert isinstance(node.func, ast.Attribute)
            assert isinstance(node.func.value, ast.Name)
            assert node.func.value.id in {"browser", "login", "repair_handle"}
    submit = ast.AsyncFunctionDef(name="submit", args=ast.arguments(
        posonlyargs=[], args=[ast.arg(arg=name) for name in
                             ("browser", "detail", "repair_handle")],
        kwonlyargs=[], kw_defaults=[], defaults=[]), body=cancel_block.body, decorator_list=[])
    namespace["origin_url"] = "memory-only-owner"
    compile_nodes(caller_path, [submit], namespace)
    for name, path in (("cancel", workflow_path), ("status", workflow_path),
                       ("check_prior", store_path), ("submit", caller_path)):
        assert namespace[name].__code__.co_filename == str(path)
    return namespace


class Rejected(Exception):
    def __init__(self, message, *, non_retryable):
        super().__init__(message)
        self.non_retryable = non_retryable


class MemoryTransport:
    """Only in-memory protocol transport; not an HTTP/Temporal/native fixture."""

    def __init__(self, namespace, *, advance_after_query=False):
        self.namespace = namespace
        self.owner = SimpleNamespace(state={"revision": 10, "outcome": None, "checks": {}},
                                     cancel_requested=False)
        self.receipts, self.acknowledgements = {}, []
        self.queries = 0
        self.updates = 0
        self.advance_after_query = advance_after_query

        async def ready(condition):
            assert condition()

        namespace.update(workflow=SimpleNamespace(wait_condition=ready), ApplicationError=Rejected)

    async def get(self, path):
        assert path == "/api/session"
        self.owner.state["revision"] += 1  # Deterministic async-setup interleaving.
        return SimpleNamespace(json=lambda: {"csrf_token": "memory-token"})

    async def query(self, name):
        assert name == "status"
        self.queries += 1
        return deepcopy(self.namespace["status"](self.owner))

    async def post(self, path, *, json, headers):
        assert path == "/api/runs/run-1/cancel"
        assert headers["X-Devflow-CSRF"] == "memory-token"
        payload = json
        command_id = payload["command_id"]
        request_digest = self.namespace["digest"](payload)
        prior = self.receipts.get(command_id)
        try:
            response = self.namespace["check_prior"](prior, "run-1", "cancel", request_digest)
            if response is not None:
                return self.response(200, response)
            if self.advance_after_query:
                self.owner.state["revision"] += 1
            self.updates += 1
            state = await self.namespace["cancel"](self.owner, payload)
        except (Rejected, ValueError) as exc:
            if prior is None:
                self.receipts[command_id] = ("run-1", "cancel", request_digest, "rejected",
                                            self.namespace["canonical_json"]({"error": str(exc)}))
            return self.response(409, {"detail": str(exc)})
        response = {"phase": state["phase"], "revision": state["revision"]}
        self.receipts[command_id] = ("run-1", "cancel", request_digest, "complete",
                                    self.namespace["canonical_json"](response))
        self.acknowledgements.append(deepcopy(payload))
        return self.response(200, response)

    @staticmethod
    def response(status, content):
        return SimpleNamespace(status_code=status, text=str(content), json=lambda: content)


class RepairCancelRevisionContract(unittest.IsolatedAsyncioTestCase):
    async def test_actual_caller_refreshes_revision_after_async_setup(self):
        namespace = source_contracts()
        transport = MemoryTransport(namespace)
        await namespace["submit"](transport, {"protocol_revision": 9}, transport)
        self.assertEqual(transport.queries, 1)
        self.assertEqual(len(transport.acknowledgements), 1)
        self.assertTrue(transport.owner.cancel_requested)
        self.assertEqual(transport.acknowledgements[0]["expected_revision"], 11)
        self.assertEqual(transport.owner.state["revision"], 12)

    async def test_stale_command_remains_rejected_and_changed_payload_cannot_reuse_id(self):
        namespace = source_contracts()
        transport = MemoryTransport(namespace)
        before = deepcopy(transport.owner.state)
        stale = {"command_id": "stale-control", "expected_revision": 9, "reason": "cancel"}
        headers = {"X-Devflow-CSRF": "memory-token"}
        result = await transport.post("/api/runs/run-1/cancel", json=stale, headers=headers)
        self.assertEqual(result.status_code, 409)
        self.assertIn("stale run revision", result.text)
        self.assertEqual(transport.updates, 1)
        repeated = await transport.post("/api/runs/run-1/cancel", json=stale, headers=headers)
        self.assertEqual(repeated.status_code, 409)
        self.assertIn("stale run revision", repeated.text)
        self.assertEqual(transport.updates, 1)
        changed = await transport.post("/api/runs/run-1/cancel", json={**stale,
                                       "expected_revision": 10}, headers=headers)
        self.assertEqual(changed.status_code, 409)
        self.assertIn("command ID already belongs", changed.text)
        self.assertEqual(transport.updates, 1)
        self.assertEqual(transport.owner.state, before)
        self.assertFalse(transport.owner.cancel_requested)
        self.assertEqual(transport.acknowledgements, [])
        await namespace["submit"](transport, {"protocol_revision": 9}, transport)
        self.assertEqual(len(transport.acknowledgements), 1)
        replay = await transport.post("/api/runs/run-1/cancel",
                                      json=transport.acknowledgements[0], headers=headers)
        self.assertEqual(replay.status_code, 200)
        self.assertEqual(transport.updates, 2)
        self.assertEqual(len(transport.acknowledgements), 1)

    async def test_transition_after_live_query_is_still_rejected_without_acknowledgement(self):
        namespace = source_contracts()
        transport = MemoryTransport(namespace, advance_after_query=True)
        with self.assertRaisesRegex(AssertionError, "stale run revision"):
            await namespace["submit"](transport, {"protocol_revision": 9}, transport)
        self.assertFalse(transport.owner.cancel_requested)
        self.assertEqual(transport.acknowledgements, [])
