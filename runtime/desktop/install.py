#!/usr/bin/env python3
"""Install the local delivery skill and stdio MCP entry without replacing names."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

NAME = "devflow-local-delivery"
SOURCE_SKILL = Path(__file__).parent / NAME / "SKILL.md"


def fail(message: str) -> None:
    raise SystemExit(message)


def codex_json(codex: str, home: Path, *args: str) -> object:
    result = subprocess.run(
        [codex, "mcp", *args],
        env={**os.environ, "CODEX_HOME": str(home)},
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        fail(f"codex mcp {' '.join(args)} failed: {result.stderr.strip()}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        fail("Codex did not return valid MCP configuration JSON")


def matching_entry(entry: dict, executable: Path, config: Path) -> bool:
    transport = entry.get("transport") or {}
    return (
        transport.get("type") == "stdio"
        and transport.get("command") == str(executable)
        and transport.get("args") == ["--config", str(config)]
        and transport.get("env") in (None, {})
        and transport.get("env_vars") in (None, [])
        and transport.get("cwd") is None
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--codex-home", required=True, type=Path)
    parser.add_argument(
        "--replace-owned-skill-sha256",
        help="replace only a previously inspected Devflow skill with this exact SHA-256",
    )
    owned = parser.add_mutually_exclusive_group()
    owned.add_argument('--repoint-owned-request', type=Path,
                       help='private sealed request for an inspected mode-only MCP/skill update')
    owned.add_argument('--rollback-owned-manifest', type=Path,
                       help='private pointer-update receipt to restore through the public CLI')
    owned.add_argument('--activate-owned-plugin-request', type=Path,
                       help='private inspected inventory request to make the plugin primary')
    owned.add_argument('--rollback-owned-plugin-manifest', type=Path,
                       help='private primary-plugin receipt to restore the direct entry/skill')
    args = parser.parse_args()

    runtime = args.runtime_dir.expanduser().resolve(strict=True)
    config = args.config.expanduser().resolve(strict=True)
    home = args.codex_home.expanduser().resolve()
    if not runtime.is_dir() or not config.is_file():
        fail("runtime directory and service config file are required")
    executable = runtime / ".venv" / "bin" / "devflow-delivery-mcp"
    cli = runtime / ".venv" / "bin" / "devflow-delivery"
    if not all(path.is_file() and os.access(path, os.X_OK) for path in (executable, cli)):
        fail(
            "build the runtime first; expected executable devflow-delivery "
            "and devflow-delivery-mcp in runtime/.venv/bin"
        )
    executable = executable.resolve(strict=True)
    codex = shutil.which("codex")
    if codex is None:
        fail("Codex CLI is required to register the MCP server")
    if args.activate_owned_plugin_request or args.rollback_owned_plugin_manifest:
        from owned_plugin import activate, rollback

        try:
            result = (rollback(codex, home, args.rollback_owned_plugin_manifest)
                      if args.rollback_owned_plugin_manifest else
                      activate(codex, home, runtime, config, args.activate_owned_plugin_request))
        except (ValueError, OSError, subprocess.SubprocessError) as exc:
            fail(str(exc))
        print(json.dumps(result, sort_keys=True))
        return
    if args.repoint_owned_request or args.rollback_owned_manifest:
        from owned_upgrade import rollback, upgrade

        try:
            result = (rollback(codex, home, args.rollback_owned_manifest)
                      if args.rollback_owned_manifest else
                      upgrade(codex, home, executable, config, SOURCE_SKILL,
                              args.repoint_owned_request))
        except (ValueError, OSError, subprocess.SubprocessError) as exc:
            fail(str(exc))
        print(json.dumps(result, sort_keys=True))
        return

    target = home / "skills" / NAME
    if target.is_symlink():
        fail(f"same-name skill is a symlink; refusing to replace {target}")
    skill_installed = target.exists()
    if skill_installed:
        if not target.is_dir() or sorted(item.name for item in target.iterdir()) != ["SKILL.md"]:
            fail(f"same-name skill differs; refusing to replace {target}")
        installed_skill = target / "SKILL.md"
        if installed_skill.is_symlink():
            fail(f"same-name skill differs; refusing to replace {target}")
        installed_bytes = installed_skill.read_bytes()
        if installed_bytes != SOURCE_SKILL.read_bytes():
            if hashlib.sha256(installed_bytes).hexdigest() != args.replace_owned_skill_sha256:
                fail(f"same-name skill differs; refusing to replace {target}")

    home.mkdir(parents=True, exist_ok=True)
    configured = codex_json(codex, home, "list", "--json")
    if not isinstance(configured, list) or not all(isinstance(entry, dict) for entry in configured):
        fail("Codex returned an unexpected MCP listing")
    existing = next((entry for entry in configured if entry.get("name") == NAME), None)
    if existing is not None:
        if not matching_entry(existing, executable, config):
            fail(f"same-name MCP entry differs; refusing to replace {NAME}")
        if not existing.get("enabled", True):
            fail("same-name MCP entry is disabled; preserving its current setting")

    created = False
    if not skill_installed:
        target.mkdir(parents=True)
        (target / "SKILL.md").write_bytes(SOURCE_SKILL.read_bytes())
        created = True
    elif (target / "SKILL.md").read_bytes() != SOURCE_SKILL.read_bytes():
        previous = (target / "SKILL.md").read_bytes()
        if hashlib.sha256(previous).hexdigest() != args.replace_owned_skill_sha256:
            fail("owned skill changed before its guarded update")
        backup = (home / "skills" / ".devflow-local-delivery-backups"
                  / args.replace_owned_skill_sha256)
        backup.mkdir(mode=0o700, parents=True, exist_ok=True)
        backup_file = backup / "SKILL.md"
        if backup_file.exists() and backup_file.read_bytes() != previous:
            fail("owned skill backup differs; refusing to update")
        backup_file.write_bytes(previous)
        temporary = target / ".SKILL.md.tmp"
        temporary.write_bytes(SOURCE_SKILL.read_bytes())
        os.replace(temporary, target / "SKILL.md")
    if existing is None:
        result = subprocess.run(
            [codex, "mcp", "add", NAME, "--", str(executable), "--config", str(config)],
            env={**os.environ, "CODEX_HOME": str(home)},
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode:
            if created:
                (target / "SKILL.md").unlink()
                target.rmdir()
            fail(f"Codex MCP registration failed: {result.stderr.strip()}")

    verified = codex_json(codex, home, "get", NAME, "--json")
    if not isinstance(verified, dict) or not matching_entry(verified, executable, config):
        fail("Codex MCP readback did not match the requested runtime and config")
    print(f"Installed {NAME} skill and MCP entry in {home}")
    print("Open a fresh Codex Desktop thread or restart the app to check tool discovery.")


if __name__ == "__main__":
    main()
