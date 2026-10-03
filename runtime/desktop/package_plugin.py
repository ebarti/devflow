#!/usr/bin/env python3
"""Package the existing local delivery entry point in an explicit marketplace root."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from pathlib import Path

SOURCE = Path(__file__).resolve().parent
NAME = "devflow"
SKILL = "devflow-local-delivery"
ENTRY = {
    "name": NAME,
    "source": {"source": "local", "path": "./plugins/devflow"},
    "policy": {"installation": "AVAILABLE", "authentication": "ON_INSTALL"},
    "category": "Productivity",
}


def json_bytes(value: dict) -> bytes:
    return (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode()


def check_path(path: Path) -> None:
    """Never follow a symlink while writing the user-selected destination."""
    for part in (*reversed(path.parents), path):
        if part.is_symlink():
            raise ValueError(f"destination is a symlink: {part}")
        if part.exists() and part != path and not part.is_dir():
            raise ValueError(f"destination parent is not a directory: {part}")


def check_plugin(target: Path, files: dict[str, bytes]) -> bool:
    check_path(target)
    if not target.exists():
        return False
    if not target.is_dir():
        raise ValueError(f"same-name plugin differs: {target}")
    expected = {Path(name) for name in files}
    expected.update(
        parent for name in files for parent in Path(name).parents if parent != Path(".")
    )
    actual = set()
    for item in target.rglob("*"):
        check_path(item)
        actual.add(item.relative_to(target))
    if actual != expected or any(
        (target / name).read_bytes() != data for name, data in files.items()
    ):
        raise ValueError(f"same-name plugin differs; preserved {target}")
    return True


def package(marketplace_root: Path, runtime_dir: Path, config_path: Path) -> Path:
    runtime = runtime_dir.expanduser().resolve(strict=True)
    config = config_path.expanduser().resolve(strict=True)
    executable = runtime / ".venv" / "bin" / "devflow-delivery-mcp"
    if not runtime.is_dir() or not config.is_file():
        raise ValueError("existing runtime directory and service config file are required")
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise ValueError(
            "build the runtime first; expected executable runtime/.venv/bin/devflow-delivery-mcp"
        )
    root = Path(os.path.abspath(marketplace_root.expanduser()))
    target = root / "plugins" / NAME
    catalog_path = root / ".agents" / "plugins" / "marketplace.json"
    check_path(root)
    check_path(catalog_path)
    files = {
        "plugin.json": (SOURCE / "plugin.json").read_bytes(),
        "mcp.json": json_bytes({
            "$schema": "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json",
            "mcpServers": {SKILL: {
                "type": "stdio", "command": str(executable.resolve(strict=True)),
                "args": ["--config", str(config)],
            }},
        }),
        f"skills/{SKILL}/SKILL.md": (SOURCE / SKILL / "SKILL.md").read_bytes(),
    }
    installed = check_plugin(target, files)
    previous = catalog_path.read_bytes() if catalog_path.exists() else None
    catalog = json.loads(previous) if previous is not None else {
        "name": "devflow-local", "interface": {"displayName": "Devflow Local"}, "plugins": [],
    }
    if (not isinstance(catalog, dict) or not isinstance(catalog.get("name"), str)
            or not isinstance(catalog.get("plugins"), list)
            or not all(isinstance(entry, dict) for entry in catalog["plugins"])):
        raise ValueError("marketplace needs a name and plugins array; preserved existing catalog")
    matches = [entry for entry in catalog["plugins"] if entry.get("name") == NAME]
    if matches and matches != [ENTRY]:
        raise ValueError("same-name marketplace entry differs; preserved existing catalog")
    if not matches:
        catalog["plugins"].append(ENTRY)
    updated = previous if matches else json_bytes(catalog)
    if installed and updated == previous:
        return target

    # Validate both destinations before staging anything. Publish only a complete
    # plugin directory; undo its addition if the catalog write cannot complete.
    root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".devflow-package-", dir=root) as temporary:
        stage = Path(temporary)
        plugin_stage = stage / NAME
        for name, data in files.items():
            path = plugin_stage / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        catalog_stage = stage / "marketplace.json"
        catalog_stage.write_bytes(updated)
        if check_plugin(target, files) != installed:
            raise ValueError("plugin destination changed during packaging")
        check_path(catalog_path)
        if (catalog_path.read_bytes() if catalog_path.exists() else None) != previous:
            raise ValueError("marketplace changed during packaging; preserved it")
        target.parent.mkdir(parents=True, exist_ok=True)
        catalog_path.parent.mkdir(parents=True, exist_ok=True)
        created = False
        try:
            if not installed:
                plugin_stage.rename(target)
                created = True
            if updated != previous:
                os.replace(catalog_stage, catalog_path)
        except OSError:
            if created:
                shutil.rmtree(target)
            raise
    return target


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--marketplace-root", required=True, type=Path)
    parser.add_argument("--runtime-dir", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args()
    try:
        target = package(args.marketplace_root, args.runtime_dir, args.config)
    except (ValueError, OSError) as exc:
        parser.exit(1, f"Packaging failed: {exc}\n")
    print(f"Packaged {target}; no host registration or service changes performed")


if __name__ == "__main__":
    main()
