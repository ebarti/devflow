"""Authenticate only uv's independently pinned startup patch.

The fixture is upstream uv 0.12.17 commit 635500036e1705961315e86447f0fab0a8ddb309,
crates/uv-virtualenv/src/_virtualenv.py, Git blob c4af24d43878f22da47efa43a3ef7899c6a7c8aa.
Its SHA-256 is cfb3db86aaa53bb62b5ff764970bec2d71c9228590a0ebec57f6ec926cc0bf1a.
The official uv code writes the pth as the exact bytes b"import _virtualenv".
"""

import hashlib
import importlib.util
import marshal
import os
import py_compile
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from devflow_temporal import delivery_runtime_installation as installation

UPSTREAM = Path(__file__).parent / "fixtures/uv-0.12.17/_virtualenv.py.txt"
SHA = "cfb3db86aaa53bb62b5ff764970bec2d71c9228590a0ebec57f6ec926cc0bf1a"


@pytest.fixture
def owned(tmp_path, monkeypatch):
    root = tmp_path / "root"
    source = root / "runtime/src"
    source.mkdir(parents=True)
    site = tmp_path / "site-packages"
    site.mkdir()
    (site / "_editable_impl_devflow_temporal.pth").write_text(str(source) + "\n")
    return root, site, monkeypatch


def _install_patch(site, monkeypatch):
    assert hashlib.sha256(UPSTREAM.read_bytes()).hexdigest() == SHA
    module_path = site / "_virtualenv.py"
    shutil.copyfile(UPSTREAM, module_path)
    (site / "_virtualenv.pth").write_bytes(b"import _virtualenv")
    _load_patch(site, monkeypatch)
    return module_path


def _load_patch(site, monkeypatch):
    module_path = site / "_virtualenv.py"
    monkeypatch.syspath_prepend(str(site))
    # Exercise a real source-file import and keep the resulting loaded module,
    # not a forged module object standing in for successful authentication.
    monkeypatch.setattr(sys, "meta_path", list(sys.meta_path))
    spec = importlib.util.spec_from_file_location("_virtualenv", module_path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "_virtualenv", module)
    spec.loader.exec_module(module)
    py_compile.compile(str(module_path), doraise=True)


def test_legacy_single_editable_registration_is_unchanged(owned):
    root, site, _ = owned
    installation._import_registrations(root, site)
    (site / "extra.pth").write_text("/foreign/path\n")
    with pytest.raises(ValueError, match="unacknowledged"):
        installation._import_registrations(root, site)


def test_pinned_upstream_patch_and_real_bytecode_are_accepted(owned):
    root, site, monkeypatch = owned
    _install_patch(site, monkeypatch)
    installation._import_registrations(root, site)


@pytest.mark.parametrize(
    "mutation", ["pth", "source", "extra", "package", "legacy_pyc", "bytecode"]
)
def test_mutations_and_import_shadows_are_refused(owned, mutation):
    root, site, monkeypatch = owned
    source = _install_patch(site, monkeypatch)
    if mutation == "pth":
        (site / "_virtualenv.pth").write_bytes(b"import _virtualenv; import os")
    elif mutation == "source":
        source.write_text("# forged startup patch\n")
    elif mutation == "extra":
        (site / "foreign.pth").write_text("import os\n")
    elif mutation == "package":
        (site / "_virtualenv").mkdir()
        (site / "_virtualenv/__init__.py").write_text("# shadow\n")
    elif mutation == "legacy_pyc":
        (site / "_virtualenv.pyc").write_bytes(b"not trusted")
    else:
        compiled = next((site / "__pycache__").glob("_virtualenv.*.pyc"))
        header = compiled.read_bytes()[:16]
        compiled.write_bytes(header + marshal.dumps(compile("pass", str(source), "exec")))
    with pytest.raises(ValueError):
        installation._import_registrations(root, site)


def test_other_path_shadow_and_forged_loaded_origin_are_refused(owned, tmp_path):
    root, site, monkeypatch = owned
    _install_patch(site, monkeypatch)
    shadow = tmp_path / "shadow"
    shadow.mkdir()
    (shadow / "_virtualenv.py").write_text("# unrelated shadow\n")
    monkeypatch.syspath_prepend(str(shadow))
    with pytest.raises(ValueError, match="resolved outside"):
        installation._import_registrations(root, site)
    sys.path.remove(str(shadow))
    monkeypatch.setattr(sys.modules["_virtualenv"], "__file__", str(shadow / "_virtualenv.py"))
    with pytest.raises(ValueError, match="resolved outside"):
        installation._import_registrations(root, site)


def test_owned_pth_and_source_cannot_be_symlinks(owned, tmp_path):
    root, site, monkeypatch = owned
    source = _install_patch(site, monkeypatch)
    outside = tmp_path / "external.py"
    shutil.copyfile(source, outside)
    source.unlink()
    source.symlink_to(outside)
    with pytest.raises(ValueError, match="linked"):
        installation._import_registrations(root, site)


def test_real_uv_01217_generated_files_match_pinned_manifest(tmp_path, monkeypatch):
    uv = os.environ.get("DEVFLOW_TEST_UV_01217") or shutil.which("uv")
    if not uv:
        pytest.skip("uv 0.12.17 executable is not available")
    version = subprocess.check_output([uv, "--version"], text=True)
    if not version.startswith("uv 0.12.17 "):
        pytest.skip("the available uv is not the pinned 0.12.17 builder")
    env = tmp_path / "environment"
    subprocess.run([uv, "venv", "--python", sys.executable, str(env)], check=True,
                   capture_output=True, text=True)
    site = env / f"lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages"
    assert (site / "_virtualenv.pth").read_bytes() == b"import _virtualenv"
    assert hashlib.sha256((site / "_virtualenv.py").read_bytes()).hexdigest() == SHA
    root = tmp_path / "root"
    source = root / "runtime/src"
    source.mkdir(parents=True)
    (site / "_editable_impl_devflow_temporal.pth").write_text(str(source) + "\n")
    _load_patch(site, monkeypatch)
    installation._import_registrations(root, site)
