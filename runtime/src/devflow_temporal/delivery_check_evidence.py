"""Retain bounded synthetic pytest outputs before transient native check cleanup."""
from __future__ import annotations

import hashlib
import os
import stat
import xml.etree.ElementTree as ET
from pathlib import Path

from .delivery_resources import (
    _identity,
    private_directory,
    read_private,
    remove_directory,
    write_private,
)

EXTENSIONS = {'.pdf', '.png', '.html', '.json', '.xml', '.txt'}


MAX_ARTIFACT_BYTES = 64 * 1024 * 1024


def _reference(path: Path, value: dict) -> dict:
    return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'candidate_id': value['candidate_id'], 'count': value['count'], 'bytes': value['bytes']}


def _owned_bytes(path: Path) -> bytes:
    if path.is_symlink() or path.resolve(strict=True) != path:
        raise ValueError('synthetic check artifact is not an owned regular file')
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_nlink != 1 or info.st_size > 50 * 1024 * 1024):
            raise ValueError('synthetic check artifact is not a bounded owned regular file')
        with os.fdopen(descriptor, 'rb', closefd=False) as stream:
            content = stream.read(50 * 1024 * 1024 + 1)
        if len(content) != info.st_size:
            raise ValueError('synthetic check artifact changed while retaining it')
        return content
    finally:
        os.close(descriptor)


def retain_artifacts(folder: Path, candidate: dict) -> dict:
    failure = folder / 'artifacts-failure.json'
    if os.path.lexists(failure):
        value = read_private(failure)
        if value.get('candidate_id') != candidate['id']:
            raise ValueError('synthetic artifact failure belongs to another candidate')
        raise ValueError(value['reason'])
    try:
        return _retain_artifacts(folder, candidate)
    except (ValueError, OSError) as exc:
        # Disposal must not turn an overflowing/failed retention into an empty,
        # apparently successful report when the finished native process replays.
        if not os.path.lexists(folder / 'artifacts.json'):
            write_private(failure, {'candidate_id': candidate['id'], 'reason': str(exc)[:300]})
        raise


def _retain_artifacts(folder: Path, candidate: dict) -> dict:
    manifest = folder / 'artifacts.json'
    source = folder / 'pytest-artifacts'
    if os.path.lexists(manifest):
        reference = _reference(manifest, read_private(manifest))
        if source.exists():
            for base, dirs, names in os.walk(source, followlinks=False):
                dirs[:] = [name for name in dirs if not Path(base, name).is_symlink()]
                for name in names:
                    path = Path(base, name)
                    if path.suffix.lower() in EXTENSIONS:
                        _owned_bytes(path)
        retained = verify_manifest(reference, candidate['id'], folder)
        # Replay validates the durable verdict and never manufactures empty raw
        # output. If the raw generation still exists, require unchanged inputs.
        if source.exists():
            for item in retained['artifacts']:
                if hashlib.sha256(_owned_bytes(source / item['relative_path'])).hexdigest() \
                        != item['sha256']:
                    raise ValueError('retained synthetic check artifact changed')
        return reference
    inventory, total = [], 0
    if source.is_symlink():
        raise ValueError('synthetic check artifact root was replaced')
    if source.exists():
        _identity(source)
    for base, dirs, names in os.walk(source, followlinks=False):
        # pytest navigation links never grant custody of their targets.
        dirs[:] = sorted(name for name in dirs if not Path(base, name).is_symlink())
        for name in sorted(names):
            path = Path(base, name)
            if path.suffix.lower() not in EXTENSIONS:
                continue
            info = path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_nlink != 1):
                raise ValueError('synthetic check artifact is not an owned regular file')
            total += info.st_size
            if (len(inventory) >= 4096 or info.st_size > 50 * 1024 * 1024
                    or total > MAX_ARTIFACT_BYTES):
                raise ValueError('synthetic check artifacts exceed the finite evidence limit')
            inventory.append((path, path.relative_to(source), info.st_size))
    # Inventory and bounds precede every copy. A new target belongs solely to
    # this invocation and is removed on any incomplete publication. A target
    # left by process loss can resume only with unchanged, inventoried bytes.
    target = folder / 'artifacts'
    private_directory(folder)
    created = not os.path.lexists(target)
    if created:
        target.mkdir(mode=0o700)
    identity = _identity(target)
    files, completed = [], False
    try:
        expected = {relative for _, relative, _ in inventory}
        existing = {p.relative_to(target) for p in target.rglob('*') if not p.is_dir()}
        if not existing.issubset(expected):
            raise ValueError('retained synthetic check artifact changed')
        for path, relative, size in inventory:
            content = _owned_bytes(path)
            if len(content) != size:
                raise ValueError('synthetic check artifact changed while retaining it')
            output = target / relative
            private_directory(output.parent)
            if os.path.lexists(output):
                if _owned_bytes(output) != content:
                    raise ValueError('retained synthetic check artifact changed')
            else:
                descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                     0o600)
                with os.fdopen(descriptor, 'wb') as stream:
                    stream.write(content)
            files.append({'path': str(output), 'relative_path': str(relative),
                          'sha256': hashlib.sha256(content).hexdigest(), 'size': size})
        value = {'candidate_id': candidate['id'], 'artifacts': files,
                 'count': len(files), 'bytes': total}
        write_private(manifest, value)
        completed = True
        return _reference(manifest, value)
    finally:
        if not completed and created:
            remove_directory(target, identity)


def verify_manifest(reference: dict, candidate_id: str, state_dir: Path) -> dict:
    from .delivery_resources import read_private

    path = Path(reference['path'])
    if (path.is_symlink() or not path.resolve(strict=True).is_relative_to(state_dir)
            or hashlib.sha256(path.read_bytes()).hexdigest() != reference['sha256']):
        raise ValueError('synthetic artifact manifest changed or escaped its run')
    manifest = read_private(path)
    if (manifest.get('candidate_id') != candidate_id or reference['candidate_id'] != candidate_id
            or manifest['count'] != len(manifest['artifacts']) or manifest['count'] > 4096):
        raise ValueError('synthetic artifacts belong to another candidate or exceed their bound')
    total, seen = 0, set()
    for item in manifest['artifacts']:
        file = Path(item['path'])
        relative = Path(item['relative_path'])
        if (relative.is_absolute() or '..' in relative.parts or str(relative) in seen
                or file != path.parent / 'artifacts' / relative
                or file.is_symlink() or file.resolve(strict=True) != file):
            raise ValueError('retained synthetic check artifact changed')
        content = _owned_bytes(file)
        if (len(content) != item['size']
                or hashlib.sha256(content).hexdigest() != item['sha256']):
            raise ValueError('retained synthetic check artifact changed')
        seen.add(str(relative))
        total += len(content)
    if total != manifest['bytes'] or total > 1024 ** 3:
        raise ValueError('synthetic artifacts exceed their authenticated bound')
    return manifest


def junit_counts(reference: dict, candidate_id: str, state_dir: Path) -> dict:
    """Count actual cases in an authenticated retained report, never log summaries."""
    manifest = verify_manifest(reference, candidate_id, state_dir)
    reports = [item for item in manifest['artifacts'] if item['relative_path'] == 'junit.xml']
    if len(reports) != 1:
        raise ValueError('required owned JUnit report is missing')
    content = Path(reports[0]['path']).read_bytes()
    if (len(content) > 50 * 1024 * 1024 or b'<!DOCTYPE' in content.upper()
            or b'<!ENTITY' in content.upper()):
        raise ValueError('JUnit report contains unsupported declarations or exceeds its bound')
    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        raise ValueError('JUnit report is malformed') from exc
    if root.tag not in {'testsuites', 'testsuite'}:
        raise ValueError('JUnit report has no test suite')
    cases = list(root.iter('testcase'))
    counts = {'tests': len(cases), 'failures': 0, 'errors': 0, 'skipped': 0, 'passed': 0}
    for case in cases:
        outcome = next((tag for tag in ('failure', 'error', 'skipped')
                        if case.find(tag) is not None), None)
        counts[{'failure': 'failures', 'error': 'errors', 'skipped': 'skipped'}.get(
            outcome, 'passed')] += 1
    return counts
