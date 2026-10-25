"""Read-only custody of a completed reservation that never entered a provider."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
from contextlib import contextmanager
from pathlib import Path

from .contracts import digest

_ABSENT = ("native-process.json", "launch.json", "ready.json", "start.json",
           "result.json", "cancel")
_STAMP = ("st_dev", "st_ino", "st_mode", "st_uid", "st_nlink", "st_size",
          "st_mtime_ns", "st_ctime_ns")
_RESULT = {"status", "summary", "findings", "session_id", "cleanup", "usage", "finish_reason"}
_LIMIT = 4 * 1024 * 1024


def _stamp(info):
    return {key: getattr(info, key) for key in _STAMP}


@contextmanager
def completed_prelaunch(spec, attempt, executions, manifest, *, completed_intakes=()):
    """Authenticate the existing supervisor's typed pre-monitor failure, never settle it.

    The durable result is server-owned producer evidence. Neither a missing
    journal nor a null session suffices. Existing owner/lifecycle leases exclude
    an unfinished launch; this observer never creates a missing proof or lock.
    """
    from .supervisor import DeliverySupervisor

    required = {'job_key', 'run_id', 'role', 'iteration', 'candidate_id', 'state', 'cleanup',
                'finished_at', 'pid', 'process_identity', 'session_id', 'result_path',
                'result_json'}
    if not isinstance(attempt, dict) or not required.issubset(attempt):
        raise ValueError('completed reservation lacks its original typed attempt')
    if not isinstance(attempt['result_json'], str):
        raise ValueError('completed reservation lacks its original typed result')
    result = json.loads(attempt['result_json'])
    state = Path(spec['state_dir'])
    key = attempt['job_key']
    if (not isinstance(key, str) or len(key) != 64
            or any(char not in '0123456789abcdef' for char in key)):
        raise ValueError('completed reservation has an invalid attempt identity')
    folder = state / 'attempts' / key
    if (attempt['run_id'] != spec['run_id']
            or attempt['state'] != 'finished' or attempt['cleanup'] != 'confirmed'
            or not isinstance(attempt['finished_at'], str) or not attempt['finished_at']
            or any(attempt[name] is not None for name in ('pid', 'process_identity', 'session_id'))
            or attempt['result_path'] != str(folder / 'result.json')
            or not isinstance(result, dict) or set(result) != _RESULT
            or result.get('status') != 'blocked' or result.get('finish_reason') != 'prelaunch'
            or result.get('cleanup') != 'confirmed'
            or result.get('session_id') is not None or result.get('usage') is not None
            or not isinstance(result.get('summary'), str) or not result['summary']
            or not isinstance(result.get('findings'), list) or not result['findings']
            or any(not isinstance(item, str) for item in result['findings'])):
        raise ValueError('completed reservation has no authenticated prelaunch result')
    descriptors, pinned = [], []
    try:
        def directory(path, *, parent=None, modes=(0o700,)):
            fd = os.open(path if parent is None else path.name,
                         os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                         **({'dir_fd': parent} if parent is not None else {}))
            descriptors.append(fd)
            info = os.fstat(fd)
            if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) not in modes):
                raise ValueError('completed prelaunch directory custody changed')
            # Directory size/times can change during unrelated role preservation.
            pinned.append((path, fd, {k: getattr(info, k) for k in
                                     ('st_dev', 'st_ino', 'st_mode', 'st_uid')}))
            return fd

        root_fd = directory(state)
        attempts_fd = directory(state / 'attempts', parent=root_fd, modes=(0o700, 0o755))
        folder_fd = directory(folder, parent=attempts_fd)
        owners = state / 'attempt-owners'
        owners_fd = directory(owners, parent=root_fd)

        def lease(path, parent):
            fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            descriptors.append(fd)
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                    or info.st_size != 0):
                raise ValueError('completed prelaunch lease custody changed')
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            pinned.append((path, fd, _stamp(info)))
            return _stamp(info)

        locks = {'attempt-owner': lease(owners / key, owners_fd)}
        for name in ('native-monitor.lock', 'native-process.lock'):
            try:
                os.stat(name, dir_fd=folder_fd, follow_symlinks=False)
            except FileNotFoundError:
                locks[name] = None
            else:
                locks[name] = lease(folder / name, folder_fd)

        fd = os.open('request.json', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                     dir_fd=folder_fd)
        descriptors.append(fd)
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                or info.st_size > _LIMIT):
            raise ValueError('completed prelaunch request is not bounded private evidence')
        pinned.append((folder / 'request.json', fd, _stamp(info)))
        with os.fdopen(os.dup(fd), 'rb') as stream:
            raw = stream.read(_LIMIT + 1)
        if len(raw) != info.st_size:
            raise ValueError('completed prelaunch request changed during observation')
        request = json.loads(raw)
        if (not isinstance(request, dict) or not isinstance(request.get('spec'), dict)
                or not isinstance(request.get('candidate'), dict)
                or not isinstance(request['candidate'].get('id'), str)
                or not isinstance(request.get('role'), str)
                or type(request.get('iteration')) is not int
                or not isinstance(request.get('workspace'), str)):
            raise ValueError('completed prelaunch original request is malformed')
        execution = request['spec']
        workspace = request.get('workspace')
        registered = manifest['roots'].get(workspace, {})
        role, iteration = request.get('role'), request.get('iteration')
        admitted = execution in executions.values() or (
            role == 'intake' and execution in completed_intakes)
        expected_workspaces = ({str(root / 'gates' / str(iteration) / role)
                                for root, original in executions.items() if original == execution}
                               if role in {'review', 'verify'} else {execution.get('checkout')})
        if (not admitted or execution.get('provider') != 'codex'
                or execution.get('policy', {}).get('execution_backend') != 'native-macos'
                or execution.get('policy_digest') != digest(execution['policy'])
                or request.get('native_authorized') is not True
                or DeliverySupervisor._job_key(request) != key
                or request.get('role') != attempt['role']
                or request.get('iteration') != attempt['iteration']
                or request['candidate']['id'] != attempt['candidate_id']
                or request.get('result_path') != attempt['result_path']
                or request.get('start_path') != str(folder / 'start.json')
                or role not in execution['policy']['roles']
                or type(iteration) is not int or iteration < 0
                or workspace not in expected_workspaces
                or registered.get('kind') not in {'checkout', 'gate'}
                or (role in {'review', 'verify'}
                    and (registered['kind'] != 'gate'
                         or registered.get('head') != request['candidate'].get('head')))
                or not registered.get('identity')):
            raise ValueError('completed prelaunch lost its original admitted request')

        def unchanged():
            for path, descriptor, stamp in pinned:
                current, named = os.fstat(descriptor), path.lstat()
                if any(getattr(current, k) != v or getattr(named, k) != v
                       for k, v in stamp.items()):
                    raise ValueError('completed prelaunch evidence generation changed')
            for name in _ABSENT:
                try:
                    os.stat(name, dir_fd=folder_fd, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                raise ValueError('completed prelaunch retains possible launch evidence')
            for name in ('native-monitor.lock', 'native-process.lock'):
                if locks[name] is None and os.path.lexists(folder / name):
                    raise ValueError('completed prelaunch lifecycle lease appeared')

        unchanged()
        yield {'kind': 'completed_native_prelaunch', 'version': 1,
               'attempt_sha256': digest(attempt), 'result_sha256': digest(result),
               'request_sha256': hashlib.sha256(raw).hexdigest(), 'leases': locks}
        unchanged()
    finally:
        for fd in reversed(descriptors):
            os.close(fd)
