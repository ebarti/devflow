"""Fixed, disposable native boundary probes. Never read credential contents."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
from pathlib import Path


def attempt(action) -> str:
    try:
        action()
        return "ALLOWED"
    except OSError as exc:
        return f"{type(exc).__name__}:{exc.errno}"


def open_only(path: str) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    os.close(descriptor)


def connect(host: str, port: int) -> None:
    with socket.create_connection((host, port), timeout=2):
        pass


def bind(port: int) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", port))


def checks(mode: str, protected: str, label: str, ports: list[int]) -> dict:
    role = mode.startswith("role")
    allowed = Path("/rolehome/tmp") if role else Path("/work")
    result = {
        "allowed_write": attempt(lambda: (allowed / f"allowed-{label}").write_text("SAFE"))
        == "ALLOWED",
        "workspace_write": attempt(lambda: Path(f"/work/write-{label}").write_text("SAFE")),
        "git_read": attempt(lambda: open_only("/work/.git")),
        "host_credential_read": attempt(lambda: open_only(protected + "/credential")),
        "state_read": attempt(lambda: open_only("/attempt/protected-state")),
        "state_write": attempt(lambda: Path("/attempt/protected-state").write_text("BREACH")),
        "outside_write": attempt(lambda: Path(protected + "/outside").write_text("BREACH")),
        "docker_socket_read": attempt(lambda: open_only("/var/run/docker.sock")),
        "unrelated_host_connect": attempt(lambda: connect("1.1.1.1", 443)),
    }
    if role:
        result.update(
            {
                "copied_auth_read": attempt(lambda: open_only("/rolehome/codex/auth.json")),
                "loopback": attempt(lambda: connect("127.0.0.1", 18931)),
            }
        )
    else:
        # No host state or credentials are mounted into broker checks. Git is
        # intentionally readable but mounted read-only, as in production.
        result["state_read"] = attempt(lambda: open_only(protected + "/state"))
        result["state_write"] = attempt(lambda: Path(protected + "/state").write_text("BREACH"))
        denied_port = next(port for port in range(18933, 19000) if port not in ports)
        result.update(
            {
                "unrelated_port_bind": attempt(lambda: bind(denied_port)),
                "unrelated_port_connect": attempt(lambda: connect("127.0.0.1", denied_port)),
            }
        )
        if ports:
            listeners = []
            try:
                for port in ports:
                    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    listener.bind(("127.0.0.1", port))
                    listener.listen(1)
                    listeners.append(listener)
                    connect("127.0.0.1", port)
                result["owned_ports"] = ports
            finally:
                for listener in listeners:
                    listener.close()
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["role-write", "role-read", "check", "browser-qa"])
    parser.add_argument("--protected", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--port", type=int, action="append", default=[])
    parser.add_argument("--child", action="store_true")
    args = parser.parse_args()
    observed = checks(args.mode, args.protected, "child" if args.child else "parent", args.port)
    if args.child:
        print(json.dumps(observed, sort_keys=True))
        return
    child = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:], "--child"],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    observed["child_returncode"] = child.returncode
    observed["child"] = json.loads(child.stdout) if child.returncode == 0 else {}
    Path(args.output).write_text(json.dumps(observed, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
