"""Execute named locked pytest evidence delegated by an accepted structured plan."""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import sys
from pathlib import Path

from .contracts import digest
from .delivery_broker import _git


def planned_checks(spec: dict, checkout: Path, evidence: Path) -> list[dict]:
    try:
        plan = json.loads(spec['accepted_plan'])
    except (ValueError, TypeError):
        return []  # Historical text plans do not grant a structured recipe.
    if not isinstance(plan, dict) or not isinstance(plan.get('verification'), list):
        return []
    names = sorted({name for step in plan['verification'] if isinstance(step, str)
                    for name in re.findall(r'\btest_[A-Za-z0-9_]+\.py\b', step)})
    if not names:
        return []
    files = _git(checkout, 'ls-files', '--', '*.py').splitlines()
    projects: dict[Path, list[Path]] = {}
    for name in names:
        matches = [checkout / f for f in files if Path(f).name == name]
        if len(matches) != 1:
            raise ValueError(f'planned pytest file must have one tracked owner: {name}')
        test = matches[0]
        if test.is_symlink() or test.resolve(strict=True) != test:
            raise ValueError('planned pytest source is not a fixed owned file')
        project = next((p for p in test.parents if p.is_relative_to(checkout)
                        and (p / 'pyproject.toml').is_file() and (p / 'uv.lock').is_file()), None)
        if project is None:
            raise ValueError(f'planned pytest source lacks a locked Python project: {name}')
        for filename in ('pyproject.toml', 'uv.lock'):
            path = project / filename
            if (path.is_symlink() or path.resolve(strict=True) != path
                    or _git(checkout, 'ls-files', '--', path.relative_to(checkout).as_posix())
                    != path.relative_to(checkout).as_posix()):
                raise ValueError('planned Python metadata escaped its fixed project')
        projects.setdefault(project, []).append(test)
    manager = shutil.which('uv')
    if not manager:
        raise ValueError('accepted locked pytest evidence requires the uv executable')
    manager = str(Path(manager).resolve(strict=True))
    result = []
    for project, tests in sorted(projects.items()):
        relative = project.relative_to(checkout).as_posix()
        provenance = {'accepted_plan_sha256': digest(plan), 'project': relative,
                      'uv_sha256': hashlib.sha256(Path(manager).read_bytes()).hexdigest(),
                      'metadata': {name: hashlib.sha256((project / name).read_bytes()).hexdigest()
                                   for name in ('pyproject.toml', 'uv.lock')},
                      'test_paths': [test.relative_to(project).as_posix() for test in tests]}
        key = digest(provenance)[:16]
        result.append({'id': 'planned-python-dependencies-' + key,
                       'cwd': relative, 'timeout_seconds': 600,
                       'argv': [manager, 'sync', '--locked', '--no-install-project',
                                '--extra', 'dev', '--python', str(Path(sys.executable).resolve())],
                       'generated_directories': [
                           (project / '.venv').relative_to(checkout).as_posix()],
                       'plan_provenance': provenance})
        check_id = 'planned-pytest-' + key
        report = evidence / check_id / 'pytest-artifacts' / 'junit.xml'
        result.append({'id': check_id, 'kind': 'test', 'cwd': relative,
                       'timeout_seconds': 900,
                       'argv': [str(project / '.venv/bin/python'), '-m', 'pytest',
                                *provenance['test_paths'], '-q',
                                *(['-o', 'pythonpath=src'] if (project / 'src').is_dir() else []),
                                '--junitxml=' + str(report)],
                       'test_count_regex': r'(\d+) passed', 'min_tests': 1,
                       'plan_provenance': provenance})
    return result
