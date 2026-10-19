"""Explicit role-owned probe output and read-only, authenticated receipt handoffs."""
from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path

from .contracts import canonical_json, digest
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
    'evidence_failure',
    'log', 'log_sha256', 'receipt', 'receipt_sha256', 'artifacts', 'sha256', 'path',
    'count', 'bytes', 'head', 'content_sha256', 'base_sha', 'baseline_candidate',
    'role_artifacts', 'relative_path', 'size', 'source_candidate', 'input_candidate',
    'plan_provenance', 'satisfies', 'metadata', 'recipe', 'recipe_sha256', 'accepted_plan_sha256',
    'test_paths', 'input_hashes', 'source_input_hashes', 'dependency_preparation',
    'checks_identity', 'started_at', 'finished_at', 'measurements', 'observations',
    'python_interpreters', 'project', 'interpreter',
    'node_toolchain', 'node_interpreter', 'corepack', 'absolute_path', 'realpath',
    'version', 'modules_ABI', 'architecture', 'platform', 'execPath', 'package_manager',
    'native_binding', 'environment', 'PATH', 'COREPACK_HOME',
    'npm_config_nodedir', 'npm_config_build_from_source', 'npm_config_python',
    'range_binding', 'origin_main_sha', 'merge_base_sha',
    'native_addon_preparation', 'native_addon_authority', 'target', 'integrity',
    'projects', 'registry_packages', 'registry_dependencies', 'dependency_links', 'native_builder',
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
                     'metadata', 'input_hashes', 'source_input_hashes', 'registry_packages',
                     'registry_dependencies', 'dependency_links'
                     } else item)
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


def _dependency_context(store, spec, folder):
    """Copy completed prerequisites through their source run's receipt boundary.

    A retained worker may still name a stopped parent. Its current assignment,
    adopted GitHub plan and verified checkpoints determine the source instead.
    These records are role context only; they never enter the worker's gates.
    """
    from .delivery_execution_registry import OwnershipConflict
    from .delivery_feature_execution import _worker_assignment, require_execution
    from .delivery_feature_pass import checkpoints
    from .delivery_feature_publication import current_record, publication_child_binding
    from .delivery_github_contract import ordered_chunks

    shared, current, _ = _worker_assignment(store, spec)
    parent = store.effective_spec(current['run_id'])
    if current['store_path'] != str(store.config.tracking_db):
        raise OwnershipConflict('dependency evidence belongs to another runtime store')
    require_execution(store, parent)
    record = current_record(parent)
    chunks = {chunk['id']: chunk for chunk in ordered_chunks(record['manifest']['plan'])}
    values = checkpoints(parent)

    def assigned(child, chunk_id, kind):
        worker = child['feature_worker']
        binding = publication_child_binding(record, chunk_id)
        return (child['feature_delivery']['registry'] == spec['feature_delivery']['registry']
                and child['feature_delivery']['owner']['issue_id'] == current['issue_id']
                and child['feature_delivery']['owner']['store_path'] == current['store_path']
                and child['github_repo'] == parent['github_repo']
                and worker['chunk_id'] == chunk_id and worker['kind'] == kind
                and worker['workstream_id'] == binding['workstream_id']
                and child['issue_url'] == binding['issue_url']
                and values.get(f'assignment:{chunk_id}:{kind}') == {
                    'run_id': child['run_id'], 'store_path': current['store_path'],
                    'chunk_id': chunk_id, 'kind': kind})

    worker = spec['feature_worker']
    if not assigned(spec, worker['chunk_id'], worker['kind']):
        raise OwnershipConflict('dependency receiver lost its accepted feature assignment')
    dependencies = set()
    pending = list(chunks[worker['chunk_id']]['depends_on'])
    while pending:
        chunk_id = pending.pop()
        if chunk_id not in dependencies:
            dependencies.add(chunk_id)
            pending.extend(chunks[chunk_id]['depends_on'])
    records = []
    for chunk_id in chunks:
        if chunk_id not in dependencies:
            continue
        proof = values.get('verified:' + chunk_id)
        members = [member for member in record['manifest']['publication']['members']
                   if member['chunk_id'] == chunk_id]
        if (not proof or len(members) != 1
                or any(proof.get(key) != members[0][key] for key in ('head', 'number'))
                or proof.get('store_path') != current['store_path']):
            raise OwnershipConflict('dependency has no completed published verification')
        source = store.effective_spec(proof['run_id'])
        if (not assigned(source, chunk_id, 'chunk')
                or source['branch'] != members[0]['branch']):
            raise OwnershipConflict('dependency source lost its accepted feature assignment')
        with store._connect() as db:
            row = db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                             (source['run_id'],)).fetchone()
            unfinished = db.execute(
                "SELECT 1 FROM delivery_attempts WHERE run_id=? "
                "AND (state!='finished' OR cleanup!='confirmed') LIMIT 1",
                (source['run_id'],)).fetchone()
            candidate = json.loads(row['candidate_json'] or 'null')
            checks = json.loads(row['checks_json'] or '{}')
            pr = json.loads(row['pr_json'] or 'null')
            published = [json.loads(effect['request_json']) for effect in db.execute(
                "SELECT request_json,observed_json FROM delivery_effects "
                "WHERE run_id=? AND kind='publish' AND state='complete'",
                (source['run_id'],)) if json.loads(effect['observed_json'] or 'null') == pr]
        if (row['outcome'] != 'delivered' or row['cleanup'] != 'confirmed' or unfinished
                or digest(checks) != proof['checks_digest'] or not published
                or not candidate or candidate['head'] != proof['head']
                or candidate['base_sha'] != source['base_sha']
                or candidate['policy_digest'] != source['policy_digest']
                or not pr or any(pr.get(key) != proof[key] for key in ('head', 'number'))
                or pr.get('url') != members[0]['url']
                or any(checks.get(stage, {}).get('state') != 'passed'
                       for stage in ('prepublish', 'local', 'review', 'qa'))
                or any(checks.get(stage, {}).get('candidate_id') != candidate['id']
                       for stage in ('local', 'review', 'qa'))
                or checks.get('prepublish', {}).get('candidate_id')
                != published[-1].get('input_candidate_id')):
            raise OwnershipConflict('completed dependency candidate or checks changed')
        source_root = Path(source['state_dir'])
        if (not source_root.is_absolute() or source_root.name != source['run_id']
                or source_root.resolve(strict=True) != source_root):
            raise ValueError('dependency evidence left its owned source run')
        evidence = _copy_receipts({'candidate': candidate, 'iteration': row['iteration'],
                                   'checks': checks}, source_root, folder)
        records.append({**evidence, 'provenance': {
            'kind': 'completed_feature_dependency', 'historical': True,
            'run_id': source['run_id'], 'chunk_id': chunk_id,
            'feature_issue_id': current['issue_id'],
            'accepted_plan_sha256': digest(record['manifest']['plan']),
            'source_spec_sha256': digest(source), 'checks_sha256': proof['checks_digest'],
            'publication': {key: pr[key] for key in ('head', 'number')},
        }})
    return records


def allocate(request: dict, job_key: str, *, store=None) -> dict:
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
    checked = request.get('check_evidence')
    if checked is not None and role in {'review', 'verify'}:
        context = dict(context or {})
        checks = dict(context.get('checks') or {})
        if 'local' in checks and checks['local'] != checked:
            raise ValueError('current check evidence conflicts with the role handoff')
        checks['local'] = checked
        context['checks'] = checks
    if context and role in {'review', 'verify'}:
        for reference in context.get('role_artifacts', []):
            manifest = validate_handoff(reference, root)
            if (manifest['source_candidate']['content_sha256']
                    != request['candidate']['content_sha256']):
                raise ValueError('implementation artifacts describe another candidate content')
    clean = _copy_receipts(context, root, folder) if context else {}
    if store is not None and request['spec'].get('feature_worker') and role != 'intake':
        dependencies = _dependency_context(store, request['spec'], folder)
        if dependencies:
            clean['historical_dependencies'] = dependencies
    if clean:
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


def repair_payload_progress(store, request: dict, result: dict, workspace: Path) -> bool:
    """An unchanged source repair must restore actual, attempt-bound evidence."""
    iteration = request['iteration']
    session = request.get('resume_session')
    reference = result.get('role_artifacts')
    if (iteration < 1 or not request.get('findings') or not session
            or result.get('session_id') != session or result.get('cleanup') != 'confirmed'
            or not isinstance(reference, dict)):
        return False
    root = Path(request['spec']['state_dir'])
    try:
        with store._connect() as db:
            rows = db.execute(
                """SELECT job_key,candidate_id,result_json FROM delivery_attempts
                   WHERE run_id=? AND role='implement' AND iteration=? AND session_id=?
                   AND state='finished' AND cleanup='confirmed'""",
                (request['spec']['run_id'], iteration, session),
            ).fetchall()
            prior = db.execute(
                """SELECT job_key,candidate_id,result_json FROM delivery_attempts
                   WHERE run_id=? AND role='implement' AND iteration<? AND session_id=?
                   AND state='finished' AND cleanup='confirmed'
                   ORDER BY iteration DESC,started_at DESC LIMIT 1""",
                (request['spec']['run_id'], iteration, session),
            ).fetchone()
        current = next((row for row in rows if json.loads(row['result_json'] or '{}').get(
            'role_artifacts') == reference), None)
        if current is None or prior is None:
            return False

        def bound(row, ref):
            expected = root / 'role-artifacts/sealed' / row['job_key'] / 'artifacts.json'
            if Path(ref['path']) != expected:
                raise ValueError('repair evidence left its controller attempt')
            manifest = validate_handoff(ref, root)
            if manifest['input_candidate']['id'] != row['candidate_id']:
                raise ValueError('repair evidence changed its input candidate')
            return manifest

        manifest = bound(current, reference)
        previous = bound(prior, json.loads(prior['result_json'])['role_artifacts'])
        from .candidate import candidate_for

        if (manifest['input_candidate'] != request['candidate']
                or manifest['source_candidate'] != candidate_for(workspace)
                or previous['source_candidate']['content_sha256']
                != manifest['source_candidate']['content_sha256']):
            return False
        old_hashes = {item['sha256'] for item in previous['artifacts'] if item['size']}
        return any(item['size'] and item['sha256'] not in old_hashes
                   for item in manifest['artifacts'])
    except (KeyError, TypeError, ValueError, OSError):
        return False


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
