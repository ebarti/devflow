"""Explicit role-owned probe output and read-only, authenticated receipt handoffs."""
from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path

from .contracts import canonical_json
from .delivery_resources import _ancestors, private_directory

# Only evidence data is copied. Transport, sessions, credentials, request payloads and
# process-control journals are never handed to a source-editing role.
FIELDS = {
    'environment_digest', 'policy_digest', 'revision', 'candidate', 'candidate_id',
    'previous_iterations', 'iteration', 'checks', 'baseline', 'prepublish', 'local',
    'implementation_preparation', 'browser_qa', 'review', 'qa', 'ci', 'state', 'detail',
    'summary', 'results', 'id',
    'argv', 'cwd', 'exit_code', 'passed', 'test_count', 'junit', 'tests', 'failures',
    'errors', 'skipped', 'rejected_output', 'rejection_causes', 'cleanup', 'diagnostic',
    'log', 'log_sha256', 'receipt', 'receipt_sha256', 'artifacts', 'sha256', 'path',
    'count', 'bytes', 'head', 'content_sha256', 'base_sha', 'baseline_candidate',
    'role_artifacts', 'relative_path', 'size', 'source_candidate', 'input_candidate',
    'plan_provenance', 'metadata', 'recipe', 'recipe_sha256', 'accepted_plan_sha256',
    'test_paths', 'input_hashes', 'source_input_hashes', 'dependency_preparation',
    'checks_identity', 'started_at', 'finished_at', 'measurements', 'observations',
}


def _root(request):
    spec = request['spec']
    root = Path(spec['state_dir'])
    if not root.is_absolute() or root.name != spec['run_id'] or root.resolve() != root:
        raise ValueError('role evidence left its owned run')
    private_directory(root)
    return root


def _immutable(path: Path, content: bytes) -> None:
    private_directory(path.parent)
    if path.exists() or path.is_symlink():
        _bytes(path, path.parent)
        if path.read_bytes() != content:
            raise ValueError('retained role evidence changed across an attempt')
        return
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(content)


def _bytes(path: Path, root: Path) -> bytes:
    _ancestors(path)
    info = path.lstat()
    if (not path.is_absolute() or not path.resolve().is_relative_to(root)
            or not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
            or info.st_nlink != 1 or info.st_size > 50 * 1024 * 1024):
        raise ValueError('role evidence file escaped its run or is not owned regular data')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        return stream.read()


def _copy_receipts(value, root, folder):
    if isinstance(value, list):
        return [_copy_receipts(item, root, folder) for item in value]
    if not isinstance(value, dict):
        return value
    if {'candidate_id', 'count', 'bytes', 'path', 'sha256'} <= value.keys():
        from .delivery_check_evidence import verify_manifest

        manifest = verify_manifest(value, value['candidate_id'], root)
        clean = {key: item for key, item in value.items() if key in FIELDS}
        clean['retained_files'] = [_copy_receipts(item, root, folder)
                                   for item in manifest['artifacts']]
    else:
        clean = {key: (_copy_receipts(item, root, folder) if key not in {
                     'metadata', 'input_hashes', 'source_input_hashes'} else item)
                 for key, item in value.items() if key in FIELDS}
    for field, checksum in [('log', 'log_sha256'), ('receipt', 'receipt_sha256'),
                            ('path', 'sha256')]:
        if field not in clean:
            continue
        path = Path(clean[field])
        if not path.is_absolute():
            raise ValueError('receipt reference is not an owned absolute file')
        content = _bytes(path, root)
        observed = hashlib.sha256(content).hexdigest()
        if clean.get(checksum) != observed:
            raise ValueError('broker receipt or full log changed before role handoff')
        target = folder / 'files' / (observed + path.suffix)
        _immutable(target, content)
        clean[field] = str(target)
    return clean


def allocate(request: dict, job_key: str) -> dict:
    if not re.fullmatch(r'[0-9a-f]{64}', job_key):
        raise ValueError('role evidence requires a controller attempt identity')
    root = _root(request)
    role = request['role']
    if role not in {'intake', 'implement', 'review', 'verify'}:
        raise ValueError('unknown evidence role')
    folder = root / 'role-evidence' / job_key
    private_directory(folder)
    enriched = {**request, 'role_evidence_key': job_key}
    if role == 'implement':
        # Stable profile root supports the same session on bounded later repairs.
        # Each attempt still has a distinct output directory and sealed snapshot.
        authoring = root / 'role-artifacts' / 'authoring' / 'implement'
        if request['spec'].get('role_home_generation'):
            authoring /= request['spec']['role_home_generation']
        target = authoring / job_key
        private_directory(target)
        enriched['artifact_directory'] = str(target)
        enriched['artifact_write_root'] = str(authoring)
    context = request.get('evidence_context')
    if context and role in {'review', 'verify'}:
        for reference in context.get('role_artifacts', []):
            manifest = validate_handoff(reference, root)
            if (manifest['source_candidate']['content_sha256']
                    != request['candidate']['content_sha256']):
                raise ValueError('implementation artifacts describe another candidate content')
    if context:
        clean = _copy_receipts(context, root, folder)
        content = (json.dumps(clean, sort_keys=True, indent=2) + '\n').encode()
        path = folder / 'receipts.json'
        _immutable(path, content)
        enriched['receipt_handoff'] = {'path': str(path),
            'sha256': hashlib.sha256(content).hexdigest()}
    return enriched


def read_context(request):
    reference = request['receipt_handoff']
    root = _root(request)
    path = Path(reference['path'])
    if not path.is_relative_to(root / 'role-evidence'):
        raise ValueError('receipt handoff escaped its owned namespace')
    content = _bytes(path, root)
    if hashlib.sha256(content).hexdigest() != reference['sha256']:
        raise ValueError('receipt handoff changed')
    return json.loads(content)


def seal(request: dict) -> dict:
    if request['role'] != 'implement' or 'artifact_directory' not in request:
        return {}
    root = _root(request)
    key = request['role_evidence_key']
    source = Path(request['artifact_directory'])
    expected = Path(request['artifact_write_root']) / key
    if source != expected or not expected.is_relative_to(root / 'role-artifacts/authoring'):
        raise ValueError('role artifact directory escaped its allocation')
    _ancestors(source / 'unused')
    folder = root / 'role-artifacts/sealed' / key
    files = []
    total = 0
    for base, dirs, names in os.walk(source, followlinks=False):
        if any(Path(base, name).is_symlink() for name in dirs):
            raise ValueError('role artifact directory contains a symbolic link')
        dirs.sort()
        for name in sorted(names):
            path = Path(base, name)
            content = _bytes(path, source)
            total += len(content)
            if len(files) >= 4096 or total > 1024 ** 3:
                raise ValueError('role artifact evidence exceeds its storage bound')
            relative = path.relative_to(source)
            target = folder / 'artifacts' / relative
            _immutable(target, content)
            files.append({'path': str(target), 'relative_path': str(relative),
                          'size': len(content), 'sha256': hashlib.sha256(content).hexdigest()})
    from .candidate import candidate_for

    candidate = candidate_for(Path(request['workspace']))
    manifest = {'candidate_id': candidate['id'], 'source_candidate': candidate,
                'input_candidate': request['candidate'], 'artifacts': files,
                'count': len(files), 'bytes': total}
    path = folder / 'artifacts.json'
    _immutable(path, (canonical_json(manifest) + '\n').encode())
    return {'role_artifacts': {'path': str(path),
            'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'candidate_id': candidate['id'], 'count': len(files), 'bytes': total}}


def validate_handoff(reference: dict, root: Path):
    from .delivery_check_evidence import verify_manifest

    path = Path(reference['path'])
    if not path.is_relative_to(root / 'role-artifacts/sealed'):
        raise ValueError('role artifact handoff escaped its sealed namespace')
    return verify_manifest(reference, reference['candidate_id'], root)


def historical_context(store, spec):
    """Keep prior sealed recovery receipts available after current checks reset."""
    with store._connect() as db:
        row = db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                         (spec['run_id'],)).fetchone()
    recovery = json.loads(row[0]) if row and row[0] else None
    records = []
    while recovery:
        state = recovery.get('state')
        if state and state.get('checks'):
            records.append({'candidate': state.get('candidate'),
                            'iteration': state.get('iteration'), 'checks': state['checks']})
        recovery = recovery.get('original_recovery')
    return {'previous_iterations': records} if records else {}
