"""Restrict candidate commands to declared loopback ports before exec.

Docker's private, network-none namespace removes host and Internet reachability.
Landlock ABI 4+ additionally denies undeclared TCP bind/connect, including
other ports inside that namespace. The rules are inherited by all descendants.
"""

from __future__ import annotations

import argparse
import ctypes
import os
import sys


class Ruleset(ctypes.Structure):
    _fields_ = [("handled_access_fs", ctypes.c_uint64), ("handled_access_net", ctypes.c_uint64)]


class NetPort(ctypes.Structure):
    _fields_ = [("allowed_access", ctypes.c_uint64), ("port", ctypes.c_uint64)]


def _syscall(number: int, *args: object) -> int:
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.syscall(number, *args)
    if result < 0:
        raise OSError(ctypes.get_errno(), f"Landlock syscall {number} failed")
    return result


def restrict_ports(ports: tuple[int, ...]) -> None:
    if any(type(port) is not int or port < 1024 or port > 65535 for port in ports):
        raise ValueError("owned TCP ports must be distinct non-system port numbers")
    if len(ports) != len(set(ports)):
        raise ValueError("owned TCP ports are duplicated")
    abi = _syscall(444, 0, 0, 1)
    if abi < 4:
        raise RuntimeError("Landlock TCP-port restrictions are unavailable")
    rights = 1 | 2  # LANDLOCK_ACCESS_NET_BIND_TCP | CONNECT_TCP
    attributes = Ruleset(0, rights)
    descriptor = _syscall(444, ctypes.byref(attributes), ctypes.sizeof(attributes), 0)
    try:
        for port in ports:
            rule = NetPort(rights, port)
            _syscall(445, descriptor, 2, ctypes.byref(rule), 0)
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(38, 1, 0, 0, 0):  # PR_SET_NO_NEW_PRIVS
            raise OSError(ctypes.get_errno(), "no-new-privileges failed")
        _syscall(446, descriptor, 0)
    finally:
        os.close(descriptor)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--allow-port", action="append", type=int, default=[])
    parser.add_argument("command", nargs=argparse.REMAINDER)
    parsed = parser.parse_args()
    command = parsed.command[1:] if parsed.command[:1] == ["--"] else parsed.command
    if not command:
        parser.error("candidate command is required")
    restrict_ports(tuple(parsed.allow_port))
    os.execvpe(command[0], command, os.environ)


if __name__ == "__main__":
    main()
