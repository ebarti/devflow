"""Retain bounded synthetic pytest outputs before transient native check cleanup."""
from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path

from .delivery_resources import private_directory, write_private

EXTENSIONS = {'.pdf', '.png', '.html', '.json', '.xml', '.txt'}


def retain_artifacts(folder: Path, candidate: dict) -> dict:
    source = folder / 'pytest-artifacts'
    private_directory(source)
    files = []
    total = 0
    for base, dirs, names in os.walk(source, followlinks=False):
        # pytest creates <prefix>current directory links beside its numbered
        # fixture roots. Prune links without following them; their owned target
        # directories are visited directly and their regular outputs retained.
        dirs[:] = [name for name in dirs if not Path(base, name).is_symlink()]
        for name in sorted(names):
            path = Path(base, name)
            if path.suffix.lower() not in EXTENSIONS:
                continue
            info = path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_nlink != 1):
                raise ValueError('synthetic check artifact is not an owned regular file')
            total += info.st_size
            if len(files) >= 4096 or info.st_size > 50 * 1024 * 1024 or total > 1024 ** 3:
                raise ValueError('synthetic check artifacts exceed the finite evidence limit')
            relative = path.relative_to(source)
            target = folder / 'artifacts' / relative
            private_directory(target.parent)
            content = path.read_bytes()
            if target.exists():
                if target.is_symlink() or target.read_bytes() != content:
                    raise ValueError('retained synthetic check artifact changed')
            else:
                descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(descriptor, 'wb') as stream:
                    stream.write(content)
            files.append({'path': str(target), 'relative_path': str(relative),
                          'sha256': hashlib.sha256(content).hexdigest(), 'size': len(content)})
    manifest = folder / 'artifacts.json'
    write_private(manifest, {'candidate_id': candidate['id'], 'artifacts': files,
                             'count': len(files), 'bytes': total})
    return {'path': str(manifest), 'sha256': hashlib.sha256(manifest.read_bytes()).hexdigest(),
            'candidate_id': candidate['id'], 'count': len(files), 'bytes': total}


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
    for item in manifest['artifacts']:
        file = Path(item['path'])
        if (file.is_symlink()
                or not file.resolve(strict=True).is_relative_to(path.parent / 'artifacts')
                or hashlib.sha256(file.read_bytes()).hexdigest() != item['sha256']):
            raise ValueError('retained synthetic check artifact changed')
    return manifest
