"""Source-bound retry handoff contracts without starting a Temporal fixture."""
import ast
import asyncio
import copy
import unittest
from pathlib import Path
from types import SimpleNamespace


def readiness_block():
    source = Path(__file__).with_name("test_delivery_tracker_retries.py")
    tree = ast.parse(source.read_text())
    fixture = next(
        node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "test_real_terminal_retry_survives_worker_replacement_without_operator"
    )
    blocks = [node for node in ast.walk(fixture) if isinstance(node, ast.AsyncWith)
              and ast.unparse(node.items[0].context_expr) == "asyncio.timeout(15)"]
    assert len(blocks) == 1
    block = copy.deepcopy(blocks[0])
    allowed = {
        "asyncio.timeout", "asyncio.sleep", "calls.count", "handle.fetch_history",
        "any", "event.HasField",
    }
    assert all(ast.unparse(node.func) in allowed for node in ast.walk(block)
               if isinstance(node, ast.Call))
    function = ast.parse("async def observe(handle, calls):\n    pass\n").body[0]
    function.body = [block]
    module = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
    scope = {"asyncio": asyncio}
    exec(compile(module, str(source), "exec"), scope)
    return scope["observe"]


def timer(seconds):
    return SimpleNamespace(
        HasField=lambda name: name == "timer_started_event_attributes",
        timer_started_event_attributes=SimpleNamespace(
            start_to_fire_timeout=SimpleNamespace(seconds=seconds)),
    )


class TrackerRetryHandoffContract(unittest.IsolatedAsyncioTestCase):
    async def test_third_call_waits_for_durable_automatic_timer(self):
        reads = []

        async def history():
            reads.append(True)
            return SimpleNamespace(events=[timer(4 if len(reads) == 1 else 30)])

        calls = ["cleanup", "tracker", "tracker", "tracker"]
        await readiness_block()(SimpleNamespace(fetch_history=history), calls)
        self.assertEqual(len(reads), 2)
        self.assertEqual(calls, ["cleanup", "tracker", "tracker", "tracker"])

    async def test_history_failure_aborts_handoff(self):
        error = RuntimeError("history read unavailable")

        async def history():
            raise error

        with self.assertRaises(RuntimeError) as caught:
            await readiness_block()(SimpleNamespace(fetch_history=history), ["tracker"] * 3)
        self.assertIs(caught.exception, error)
