"""Replay unchanged unmarked histories on their compatible retained artifact."""

import asyncio
import gzip
import hashlib
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer
from temporalio.workflow import NondeterminismError

from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
from devflow_temporal.delivery_workflow import DeliveryWorkflow

FIXTURES = Path(__file__).parent / "fixtures"
RETAINED_HASHES = {
    "delivery-after-check-order-history.json":
        "4ac296984435a84c455215b7e65d69cdd79dc403ff19bdfd2b4031d1285b6bab",
    "delivery-published-checkpoint-history.json":
        "d2c702bb9d8199ff2e8dfbf52eb477a7338d9c7c951e47136ab685962d8ff65f",
    "order/c04-gates-only-history.json":
        "8bf1d3f40406ad87b5f062614f7b7caffb7bf0f80668d826654b86b39fef635e",
    "check-slots-legacy-history.json":
        "5c84aba1d81ff5347db0cae8890ad057aca23156829c8236f1735017c9dd8928",
    "delivery-failure-c04-history.json":
        "e3a4339f8ca42e6f6f2ec1b862f9d2e1ccbc3b2d55c585bea874ef7609ac5170",
    # Captured on #78, not c04: full replay must prove retained c04 compatibility.
    "delivery-failure-budget-history.json":
        "7ede50477c99ed33a68d20f58841248249067ad21505648f656c480fb2d44ae3",
    "technical-c04-checks-completed-history.json":
        "c8dfe4bacb5991d892a3e1829ce73da3458e7d5fffcaa9f7ebdd98fbb73fdb63",
    "technical-c04-suspended-history.json":
        "a8ca9e65b5ccb90c0907a4b6f8ff6a6d7c1f7ce874833d707e9d55f4fa77bd28",
    "technical-c04-review-completed-history.json":
        "b00568a5ff471d21d04139862fbc13b281c231bd99399999ad33e145144595e9",
    "required_ci/c04-ci-previous-history.json":
        "d1e127551f3abca95197b87a0de11699d736c94f82b14da1f5f3ddf1971d38f5",
    "policy-c04-completed-history.json.gz":
        "d3307333a39a715fc59c982ab7790d5ebc31ec31876e049db27062601cf36253",
    "policy-c04-suspended-history.json.gz":
        "77e69e3c2179ddaf39e8e3e8654fbb242f100ddfd63c60edffbbfe822f839ac9",
}


async def replay_designated_history(path, tmp_path, workflow_id="delivery-replay"):
    original = path.read_bytes()
    content = gzip.decompress(original) if path.suffix == ".gz" else original
    history = WorkflowHistory.from_json(workflow_id, content.decode())
    name = path.relative_to(FIXTURES).as_posix()
    current = Replayer(workflows=[DeliveryWorkflow], data_converter=DELIVERY_DATA_CONVERTER)
    if name not in RETAINED_HASHES:
        await current.replay_workflow(history)
        assert path.read_bytes() == original
        return
    assert hashlib.sha256(original).hexdigest() == RETAINED_HASHES[name]
    if name == "policy-c04-suspended-history.json.gz":
        # This genuine history stops before the ordering boundary; both artifacts replay it.
        await current.replay_workflow(history)
    else:
        with pytest.raises(NondeterminismError, match="TMPRL1100.*delivery_checks.*delivery_role"):
            await current.replay_workflow(history)
    archive = FIXTURES / "order/c04-source.tar.gz"
    provenance = json.loads((FIXTURES / "order/provenance.json").read_text())
    assert provenance["source_commit"] == "c04f00eb43eb225728b63c82026ffe97a41cafc2"
    assert provenance["source_archive_sha256"] == (
        "cc9b1479c7c568d04970b0a610bd24db28f08f850aad3d83644f2894ed960958")
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == provenance["source_archive_sha256"]
    retained = tmp_path / "retained-c04"
    retained.mkdir()
    with tarfile.open(archive) as artifact:
        artifact.extractall(retained, filter="data")
    source = retained / "runtime/src"
    before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in source.rglob("*.py")}
    result = await asyncio.to_thread(subprocess.run, [sys.executable, "-B", "-c", '''
import asyncio, gzip, sys
from pathlib import Path
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer
from devflow_temporal import delivery_workflow
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
assert Path(delivery_workflow.__file__).resolve().is_relative_to(Path(sys.argv[3]).resolve())
path = Path(sys.argv[1])
content = gzip.decompress(path.read_bytes()) if path.suffix == ".gz" else path.read_bytes()
asyncio.run(Replayer(workflows=[delivery_workflow.DeliveryWorkflow],
    data_converter=DELIVERY_DATA_CONVERTER).replay_workflow(
        WorkflowHistory.from_json(sys.argv[2], content.decode())))
''', str(path.resolve()), workflow_id, str(source.resolve())],
        env={**os.environ, "PYTHONPATH": str(source.resolve())}, cwd=retained,
        capture_output=True, text=True, check=False, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    after = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in source.rglob("*.py")}
    assert after == before
    assert path.read_bytes() == original
