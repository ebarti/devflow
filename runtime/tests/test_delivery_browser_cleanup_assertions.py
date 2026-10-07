"""Pure assertion cases; no native process, browser or Temporal fixture is executed."""
from copy import deepcopy

import pytest


def assert_interrupted_browser_cleanup(
    result, native_cleanup_confirmed, original, terminal, table, ports, *, failure,
):
    if failure == "exception":
        assert result["state"] == result["cleanup"] == "unknown"
        assert native_cleanup_confirmed is False
        return
    assert failure == "interrupted"
    assert result["cleanup"] == "confirmed"
    assert native_cleanup_confirmed is True
    assert terminal["intent"] == original["intent"]
    assert terminal["ports"] == original["ports"] == original["intent"]["ports"]
    assert terminal["monitor"] == original["monitor"]
    assert terminal["phase"] == "finished" and terminal["monitoring_complete"] is True
    native = result["native_process"]
    assert native == terminal["result"]
    assert native["cleanup"] == "observed-native-confirmed"
    assert native["monitoring_complete"] is True and native["stdio_drained"] is True
    owned = terminal["owned"]
    assert owned and original["owned"].keys() <= owned.keys()
    for pid, actor in original["owned"].items():
        assert owned[pid]["identity"] == actor["identity"]
    monitor = original["monitor"]
    assert owned[str(monitor["pid"])]["identity"] == monitor["identity"]
    assert native["observed_owned_pids"] == sorted(map(int, owned))
    for pid, actor in owned.items():
        observed = table.get(int(pid), {})
        assert (observed.get("identity") != actor["identity"]
                or observed["stat"].startswith("Z"))
    assert set(ports) == set(original["ports"]) and not any(ports.values())


def completed_monitor():
    monitor = {"pid": 20, "identity": "owned monitor"}
    original = {
        "intent": {"run_id": "owned", "policy_digest": "frozen", "ports": [14001, 14002]},
        "monitor": monitor,
        "ports": [14001, 14002],
        "owned": {"21": {"identity": "owned child"}},
    }
    native = {
        "cleanup": "observed-native-confirmed", "monitoring_complete": True,
        "stdio_drained": True, "observed_owned_pids": [20, 21],
    }
    terminal = {
        **deepcopy(original), "phase": "finished", "monitoring_complete": True,
        "owned": {"20": monitor, **original["owned"]}, "result": native,
    }
    result = {"state": "passed", "cleanup": "confirmed", "native_process": native}
    return [result, True, original, terminal, {}, {14001: set(), 14002: set()}]


def test_complete_detached_monitor_can_confirm_browser_cleanup():
    assert_interrupted_browser_cleanup(*completed_monitor(), failure="interrupted")


def test_unrelated_reused_pid_does_not_change_owned_cleanup():
    values = completed_monitor()
    values[4][21] = {"identity": "unrelated new start", "stat": "S"}
    assert_interrupted_browser_cleanup(*values, failure="interrupted")


def test_readback_exception_remains_unknown_after_physical_completion():
    values = completed_monitor()
    values[:2] = [{"state": "unknown", "cleanup": "unknown", "reason": "RuntimeError"}, False]
    assert_interrupted_browser_cleanup(*values, failure="exception")


def test_readback_exception_cannot_confirm_browser_cleanup():
    with pytest.raises(AssertionError):
        assert_interrupted_browser_cleanup(*completed_monitor(), failure="exception")


@pytest.mark.parametrize("violation", [
    "unknown-result", "broker-unconfirmed", "unknown-native", "unfinished", "unmonitored",
    "undrained", "changed-intent", "changed-ports", "changed-monitor", "lost-child", "lost-monitor",
    "changed-child-identity", "lost-result-inventory", "live-child", "live-monitor", "live-port",
])
def test_confirmation_rejects_incomplete_or_live_resources(violation):
    values = completed_monitor()
    result, _, original, terminal, table, ports = values
    if violation == "unknown-result":
        result["cleanup"] = "unknown"
    elif violation == "broker-unconfirmed":
        values[1] = False
    elif violation == "unknown-native":
        terminal["result"]["cleanup"] = "unknown"
    elif violation == "unfinished":
        terminal["phase"] = "authorized"
    elif violation == "unmonitored":
        terminal["monitoring_complete"] = False
    elif violation == "undrained":
        terminal["result"]["stdio_drained"] = False
    elif violation == "changed-intent":
        terminal["intent"]["run_id"] = "foreign"
    elif violation == "changed-ports":
        terminal["ports"] = [14001]
    elif violation == "changed-monitor":
        terminal["monitor"] = {"pid": 20, "identity": "foreign monitor"}
    elif violation == "lost-child":
        del terminal["owned"]["21"]
    elif violation == "lost-monitor":
        terminal["owned"]["20"] = {"identity": "foreign monitor"}
    elif violation == "changed-child-identity":
        terminal["owned"]["21"] = {"identity": "foreign child"}
    elif violation == "lost-result-inventory":
        terminal["result"]["observed_owned_pids"] = [20]
    elif violation in {"live-child", "live-monitor"}:
        pid = 21 if violation == "live-child" else 20
        table[pid] = {**terminal["owned"][str(pid)], "stat": "S"}
    elif violation == "live-port":
        ports[14001] = {21}
    assert original["intent"]["run_id"] == "owned"
    with pytest.raises(AssertionError):
        assert_interrupted_browser_cleanup(*values, failure="interrupted")
