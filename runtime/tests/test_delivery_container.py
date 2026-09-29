"""Optional real-Docker regression for detached descendants and replay."""

from __future__ import annotations

import hashlib
import os
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

from devflow_temporal.delivery_container import Bind, OwnedContainer


@pytest.mark.skipif(
    not os.environ.get("DEVFLOW_TEST_CONTAINER_IMAGE"),
    reason="requires an explicitly built local candidate image",
)
def test_owned_container_reaps_detached_child_and_replays_one_execution():
    image_id = os.environ["DEVFLOW_TEST_CONTAINER_IMAGE"]
    docker = Path("/usr/local/bin/docker")
    seccomp = Path(__file__).resolve().parents[1] / "docker/moby-56be731-codex-bwrap-seccomp.json"
    test_root = Path.home() / ".local/state/devflow"
    test_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="container-regression-", dir=test_root) as temporary:
        root = Path(temporary)
        state = root / "runs" / "detached"
        checkout = root / "checkouts" / "detached"
        state.mkdir(parents=True)
        checkout.mkdir(parents=True)
        heartbeat = checkout / "heartbeat.txt"
        child = (
            "import pathlib,time\n"
            "p=pathlib.Path('/work/heartbeat.txt')\n"
            "for n in range(300):\n"
            " p.write_text(str(n))\n"
            " time.sleep(0.1)\n"
        )
        launcher = (
            "import subprocess,time\n"
            f"subprocess.Popen(['/usr/bin/python3','-c',{child!r}],start_new_session=True)\n"
            "time.sleep(0.3)\n"
            "import sys; print('detached launcher complete',file=sys.stderr,flush=True)\n"
        )
        spec = {
            "run_id": "detached",
            "state_dir": str(state),
            "checkout": str(checkout),
            "policy_digest": "container-detached-regression",
            "policy": {
                "container": {
                    "docker_bin": str(docker),
                    "image_id": image_id,
                    "seccomp_profile": str(seccomp),
                    "seccomp_sha256": hashlib.sha256(seccomp.read_bytes()).hexdigest(),
                }
            },
        }
        container = OwnedContainer(
            spec,
            kind="check",
            identity={"id": "detached-child"},
            evidence_dir=state / "check" / "container",
            binds=(Bind(checkout, "/work"),),
            command=("/usr/bin/python3", "-c", launcher),
            cwd="/work",
            environment={"HOME": "/tmp", "PATH": "/usr/bin:/bin"},
            network="none",
            timeout_seconds=30,
        )
        container_id = None
        try:
            first = container.run()
            container_id = first.container_id
            assert first.exit_code == 0 and first.cleanup == "confirmed"
            assert "[stderr]" in first.log.read_text()
            assert heartbeat.is_file()
            stopped_at = heartbeat.read_bytes()
            time.sleep(0.4)
            assert heartbeat.read_bytes() == stopped_at
            second = container.run()
            assert second.container_id == container_id
            assert second.log_sha256 == first.log_sha256
            assert heartbeat.read_bytes() == stopped_at
        finally:
            if container_id:
                subprocess.run([str(docker), "rm", container_id], capture_output=True, check=False)
