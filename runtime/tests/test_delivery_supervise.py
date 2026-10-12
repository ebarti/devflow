"""The supervised start honours an owner's stop until the next explicit start."""
from __future__ import annotations

import json
import os
import subprocess
import sys

from test_delivery_startup import config as config  # noqa: F401

from devflow_temporal import delivery_control as control


def test_supervise_leaves_an_owner_stopped_stack_alone_until_the_next_start(config, monkeypatch):
    launches = []
    monkeypatch.setattr(control, "_ports", lambda _config: None)
    monkeypatch.setattr(control, "_start",
                        lambda *_args, **kwargs: launches.append(kwargs) or {"processes": {}})
    # An explicit stop records the owner's intent, even with nothing running.
    assert control.service_stop(config) == {}
    marker = control._stopped_marker(config)
    assert json.loads(marker.read_text())["command"] == "stop"
    stopped = control.ensure_service_running(config, unless_stopped=True)
    assert stopped["stopped"] == str(marker)
    assert launches == []
    assert marker.exists()
    # The next explicit start, restart or write-triggered start clears the intent.
    control.ensure_service_running(config)
    assert launches == [{}]
    assert not marker.exists()
    control.ensure_service_running(config, unless_stopped=True)
    assert len(launches) == 2
    control.service_restart(config)
    assert len(launches) == 3
    assert not marker.exists()


def test_supervise_command_reports_a_stopped_stack_without_starting_it(config):
    control.service_stop(config)
    result = subprocess.run(
        [sys.executable, "-m", "devflow_temporal.delivery_control", "--config", str(config.path),
         "supervise"], text=True, capture_output=True,
        env={**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"})
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["stopped"] == str(control._stopped_marker(config))
    assert not control._read_manifest(config)
