"""Execute named locked pytest evidence delegated by an accepted structured plan."""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

from .contracts import digest
from .delivery_broker import _git


def selected_tests(spec: dict, checkout: Path) -> list[Path]:
    """Concrete existing test selectors, sealed by the controller admission."""
    paths = spec.get('verification_test_paths', [])
    if (not isinstance(paths, list) or len(paths) > 32
            or any(not isinstance(p, str) for p in paths) or len(set(paths)) != len(paths)):
        raise ValueError('verification selectors must be a bounded unique path list')
    if not paths:
        return []
    plan = json.loads(spec['accepted_plan'])
    if (not isinstance(plan, dict) or not isinstance(plan.get('verification'), list)
            or not any(isinstance(step, str) and re.search(
                r'\b(test|tests|regressions|pytest|vitest)\b', step, re.I)
                       for step in plan['verification'])):
        raise ValueError('verification selectors require an accepted structured test step')
    result = []
    for raw in paths:
        path = Path(raw)
        if (path.is_absolute() or '..' in path.parts or path.as_posix() != raw
                or not (re.fullmatch(r'test_[A-Za-z0-9_]+\.py', path.name)
                        or re.fullmatch(r'[A-Za-z0-9_.-]+\.test\.[cm]?[jt]sx?', path.name))):
            raise ValueError('verification selector is not an owned test filename')
        test = checkout / path
        if (test.is_symlink() or test.resolve(strict=True) != test or not test.is_file()
                or _git(checkout, 'ls-files', '--', raw) != raw):
            raise ValueError('verification selector must be a fixed tracked test')
        result.append(test)
    return result


def _future_test(spec: dict, checkout: Path, name: str) -> Path:
    """Resolve future or partial tests only for locked dependency preparation."""
    from .delivery_source_scope import require_authorized

    hints = (spec.get("expected_paths", []) if spec.get("gate_selections_version") == 2
             else spec.get('policy', {}).get('allowed_paths', []))
    paths = [raw for raw in hints if isinstance(raw, str) and Path(raw).name == name]
    if len(paths) != 1:
        raise ValueError(f'future planned test must have one authorized owner: {name}')
    raw = paths[0]
    require_authorized(spec.get("policy", {}), [raw])
    relative = Path(raw)
    test = checkout / relative
    if (relative.is_absolute() or '..' in relative.parts or relative.as_posix() != raw
            or test.is_symlink() or (test.exists() and not test.is_file())
            or test.resolve() != test):
        raise ValueError('future planned test is not a fixed authorized file path')
    return test


def planned_projects(spec: dict, checkout: Path, *,
                     preparation: bool = False) -> dict[Path, list[Path]]:
    try:
        plan = json.loads(spec['accepted_plan'])
    except (ValueError, TypeError):
        return {}  # Historical text plans do not grant a structured recipe.
    if not isinstance(plan, dict) or not isinstance(plan.get('verification'), list):
        return {}
    names = sorted({name for step in plan['verification'] if isinstance(step, str)
                    for name in re.findall(r'\btest_[A-Za-z0-9_]+\.py\b', step)})
    if spec.get("gate_selections_version") == 2:
        names = []  # Versioned selections replace prose inference, not authority.
    files = _git(checkout, 'ls-files', '--', '*.py').splitlines()
    projects: dict[Path, list[Path]] = {}
    chosen = [p for p in selected_tests(spec, checkout) if p.suffix == '.py']
    future = set()
    if spec.get("gate_selections_version") == 2:
        from .delivery_feature_gates import resolve_required_selectors, selector_path
        from .delivery_source_scope import require_authorized

        paths = sorted({raw for gate in spec.get("feature_gate_selections", [])
                        if gate["stage"] == "checks" for raw in gate["selectors"]
                        if Path(raw).suffix == ".py"})
        for raw in paths:
            selector_path(raw)
            test = checkout / raw
            if not test.exists() and preparation:
                if raw not in spec.get("expected_paths", []):
                    raise ValueError("future selected Python test has no current chunk owner")
                require_authorized(spec["policy"], [raw])
                if test.resolve() != test:
                    raise ValueError("future selected Python test escaped its source")
                future.add(test)
            else:
                resolve_required_selectors({"required_selectors": [raw],
                                            "selector_evidence_version": 1}, checkout, spec=spec)
            chosen.append(test)
    for name in names:
        matches = [checkout / f for f in files if Path(f).name == name]
        if not matches and preparation:
            test = _future_test(spec, checkout, name)
            chosen.append(test)
            future.add(test)
            continue
        if len(matches) != 1:
            raise ValueError(f'planned pytest file must have one tracked owner: {name}')
        chosen.append(matches[0])
    for test in sorted(set(chosen)):
        if test not in future and (test.is_symlink() or test.resolve(strict=True) != test):
            raise ValueError('planned pytest source is not a fixed owned file')
        project = next((p for p in test.parents if p.is_relative_to(checkout)
                        and (p / 'pyproject.toml').is_file() and (p / 'uv.lock').is_file()), None)
        if project is None:
            raise ValueError(f'planned pytest source lacks a locked Python project: {test.name}')
        for filename in ('pyproject.toml', 'uv.lock'):
            path = project / filename
            if (path.is_symlink() or path.resolve(strict=True) != path
                    or _git(checkout, 'ls-files', '--', path.relative_to(checkout).as_posix())
                    != path.relative_to(checkout).as_posix()):
                raise ValueError('planned Python metadata escaped its fixed project')
        projects.setdefault(project, []).append(test)
    return projects


def _node_script_covered(test: Path, checkout: Path, recipe: dict) -> bool:
    """Authenticate file operands of the known built-in Node/JUnit recipe form."""
    if recipe['argv'][:2] != ['node', '--test']:
        return False
    operands = []
    for arg in recipe['argv'][2:]:
        if arg == '--test-only' or (arg.startswith('--test-') and '=' in arg):
            continue
        # Unknown flags may consume the next argument. They cannot prove that
        # an apparent filename is a positional file executed by this recipe.
        if arg.startswith('-') or Path(arg).is_absolute():
            return False
        operands.append(arg)
    return any((checkout / recipe['cwd'] / arg).resolve() == test for arg in operands)


def planned_node_tests(spec: dict, checkout: Path, junit_recipes: list[dict], *,
                       preparation: bool = False) -> list[Path]:
    """Resolve named Node tests from the same sealed plan used for Python."""
    try:
        plan = json.loads(spec['accepted_plan'])
    except (ValueError, TypeError):
        return []
    if not isinstance(plan, dict) or not isinstance(plan.get('verification'), list):
        return []
    names = sorted({name for step in plan['verification'] if isinstance(step, str)
                    for name in re.findall(r'\b[A-Za-z0-9_.-]+\.test\.[cm]?[jt]sx?\b', step)})
    if spec.get("gate_selections_version") == 2:
        names = []  # Concrete selected recipes carry their own per-file evidence.
    if len(names) > 32:
        raise ValueError('planned Node tests require a bounded name list')
    chosen = [p for p in selected_tests(spec, checkout) if p.suffix != '.py']
    future = set()
    files = _git(checkout, 'ls-files', '--', '*.test.*').splitlines()
    for name in names:
        matches = [checkout / f for f in files if Path(f).name == name]
        if not matches and preparation:
            test = _future_test(spec, checkout, name)
            chosen.append(test)
            future.add(test)
            continue
        if len(matches) != 1:
            raise ValueError(f'planned Node test must have one tracked owner: {name}')
        test = matches[0]
        if test.is_symlink() or test.resolve(strict=True) != test or not test.is_file():
            raise ValueError('planned Node test is not a fixed owned file')
        # A named built-in Node/JUnit script already has its authenticated argv.
        # Prose must not redirect it into a different package/test runner.
        covered = any(_node_script_covered(test, checkout, recipe) for recipe in junit_recipes)
        if not covered:
            chosen.append(test)
    result = sorted(set(chosen))
    if len(result) > 32:
        raise ValueError('planned Node tests require a bounded selection')
    for test in result:
        if test not in future and (test.is_symlink() or test.resolve(strict=True) != test
                                   or not test.is_file()):
            raise ValueError('planned Node test is not a fixed owned file')
    return result


def planned_junit_recipes(spec: dict, checkout: Path, evidence: Path, *,
                          static: bool = False) -> list[dict]:
    """Execute explicitly named, tracked recipes; prose never supplies an argv."""
    try:
        plan = json.loads(spec['accepted_plan'])
    except (ValueError, TypeError):
        return []
    if not isinstance(plan, dict) or not isinstance(plan.get('verification'), list):
        return []
    steps = [step for step in plan['verification'] if isinstance(step, str)
             and 'scripts/checks.toml' in step
             and (static or re.search(r'\bjunit\b', step, re.I))]
    if not steps and spec.get("gate_selections_version") != 2:
        return []
    if spec.get("gate_selections_version") == 2 and not any(
            gate["stage"] == "checks" and gate["recipe_id"].startswith("checks.")
            for gate in spec.get("feature_gate_selections", [])):
        return []
    content = _base_recipe_metadata(spec)
    metadata = tomllib.loads(content.decode('utf-8'))
    recipes = metadata.get('checks', {})
    if metadata.get('schema_version') != 1 or not isinstance(recipes, dict):
        raise ValueError('planned JUnit recipe schema is unsupported')
    # Match whole references, longest names first. Internal punctuation cannot
    # authorize a prefix/suffix recipe; trailing prose punctuation ends a token.
    # Complete natural names take precedence over qualified interpretations.
    names = '|'.join(re.escape(key) for key in sorted(recipes, key=len, reverse=True))
    closing = r'''[`'".,;:!?*)\]}]'''
    link = r'(?:\([^\n)]*\)|\[[^\n\]]*\])'
    reference = re.compile(
        r'''(?<!\S)[`'"*(\[{]*?(?:(''' + names + r''')\s+recipe\b|checks\.(''' + names
        + r')(?=$|\s|' + closing + r'+(?:' + link + closing + r'*)?(?:\s|$)))', re.I)
    mentioned = {match[1] or match[2] for step in steps for match in reference.finditer(step)}
    gates = spec.get("feature_gate_selections", [])
    if spec.get("gate_selections_version") == 2:
        selected = sorted(gate["recipe_id"].removeprefix("checks.") for gate in gates
                          if gate["stage"] == "checks" and gate["recipe_id"].startswith("checks."))
    else:
        selected = sorted(key for key in recipes if any(
            re.fullmatch(re.escape(key), name, re.I) for name in mentioned))
    if not selected or len(selected) > 32:
        raise ValueError('planned JUnit verification must name bounded repository recipes')
    result = []
    for key in selected:
        recipe = recipes[key]
        if isinstance(recipe, dict) and recipe.get('kind') != ('static' if static else 'junit'):
            if recipe.get('kind') in {'static', 'junit'}:
                continue
        argv = recipe.get('argv') if isinstance(recipe, dict) else None
        timeout = recipe.get('timeout_seconds', 600) if isinstance(recipe, dict) else None
        minimum = recipe.get('min_executed', 1) if isinstance(recipe, dict) else None
        relative = recipe.get('cwd', '.') if isinstance(recipe, dict) else None
        if (not isinstance(recipe, dict) or recipe.get('kind') != ('static' if static else 'junit')
                or not isinstance(argv, list) or not 1 <= len(argv) <= 64
                or any(not isinstance(arg, str) or not arg or len(arg) > 8192
                       or '\0' in arg for arg in argv)
                or sum(arg.count('{report_path}') for arg in argv) != (0 if static else 1)
                or type(timeout) is not int or not 1 <= timeout <= 2700
                or type(minimum) is not int or not 1 <= minimum <= 1000000
                or not isinstance(relative, str) or Path(relative).is_absolute()
                or '..' in Path(relative).parts
                or not (checkout / relative).resolve(strict=True).is_relative_to(checkout)):
            raise ValueError('planned JUnit recipe has invalid bounded execution authority')
        provenance = {'accepted_plan_sha256': digest(plan), 'recipe': 'checks.' + key,
                      'base_sha': spec['base_sha'],
                      'metadata': {'scripts/checks.toml': hashlib.sha256(
                          content).hexdigest()},
                      'recipe_sha256': digest(recipe)}
        check_id = ('planned-static-' if static else 'planned-junit-') + digest(provenance)[:16]
        report = evidence / check_id / 'pytest-artifacts/junit.xml'
        check = {'id': check_id, 'kind': 'static' if static else 'test', 'cwd': relative,
                 'argv': [arg.replace('{report_path}', str(report)) for arg in argv],
                 'timeout_seconds': timeout, 'min_tests': minimum,
                 'junit_required': not static, 'plan_provenance': provenance}
        if spec.get("gate_selections_version") == 2:
            from .delivery_feature_gates import selected_recipe

            gate = next(gate for gate in gates if gate["stage"] == "checks"
                        and gate["recipe_id"] == "checks." + key)
            check["tracked_recipe"] = True
            check = selected_recipe(check, gate, spec)
        result.append(check)
    return result


def _base_recipe_metadata(spec: dict) -> bytes:
    """Read the admitted source commit, never the candidate's recipe or Git refs."""
    base = spec.get('base_sha')
    source = Path(spec.get('source_path', ''))
    if (not isinstance(base, str) or not re.fullmatch(r'[0-9a-f]{40}|[0-9a-f]{64}', base)
            or not source.is_absolute() or source.resolve(strict=True) != source):
        raise ValueError('planned recipes require an admitted source commit')
    path = 'scripts/checks.toml'
    entry = _git(source, '--no-replace-objects', 'ls-tree', '--full-tree', base, '--', path)
    fields = entry.split()
    if (len(fields) != 4 or fields[0] not in {'100644', '100755'}
            or fields[1] != 'blob' or fields[3] != path):
        raise ValueError('planned recipes require fixed tracked metadata in the base commit')
    result = subprocess.run(
        ['git', '--no-replace-objects', '-c', 'core.hooksPath=/dev/null',
         '-c', 'core.fsmonitor=false', '-C', str(source), 'cat-file', 'blob', fields[2]],
        capture_output=True, timeout=120, check=False,
    )
    if result.returncode:
        raise ValueError('planned recipe base blob is unavailable')
    return result.stdout


def planned_checks(spec: dict, checkout: Path, evidence: Path, *,
                   preparation: bool = False) -> list[dict]:
    result = planned_junit_recipes(spec, checkout, evidence)
    result.extend(planned_junit_recipes(spec, checkout, evidence, static=True))
    projects = planned_projects(spec, checkout, preparation=preparation)
    # Approved recipes can create the same environments before the named-test
    # dependency step. Record their ownership before the first recipe launches.
    generated = [(project / '.venv').relative_to(checkout).as_posix()
                 for project in sorted(projects)]
    for check in result:
        if generated:
            check['generated_directories'] = generated
    node_tests = planned_node_tests(spec, checkout, result, preparation=preparation)
    if not projects and not node_tests:
        return result
    plan = json.loads(spec['accepted_plan'])
    manager = shutil.which('uv')
    if projects and not manager:
        raise ValueError('accepted locked pytest evidence requires the uv executable')
    manager = str(Path(manager).resolve(strict=True)) if manager else None
    for project, tests in sorted(projects.items()):
        relative = project.relative_to(checkout).as_posix()
        provenance = {'accepted_plan_sha256': digest(plan), 'project': relative,
                      'selection_sha256': digest(spec.get('verification_test_paths', [])),
                      'uv_sha256': hashlib.sha256(Path(manager).read_bytes()).hexdigest(),
                      'metadata': {name: hashlib.sha256((project / name).read_bytes()).hexdigest()
                                   for name in ('pyproject.toml', 'uv.lock')},
                      'test_paths': [test.relative_to(project).as_posix() for test in tests]}
        key = digest(provenance)[:16]
        dependency_id = 'planned-python-dependencies-' + key
        journal = evidence / dependency_id / 'native' / 'native-process.json'
        command = [manager, 'sync', '--locked', '--extra', 'dev',
                   '--python', str(Path(sys.executable).resolve())]
        legacy_command = [*command[:3], '--no-install-project', *command[3:]]
        if journal.exists() or journal.is_symlink():
            from .delivery_resources import read_private
            from .delivery_sandbox import native_check_argv

            # Select only a complete known recipe; never adopt arbitrary journal
            # argv. NativeProcess still authenticates every original intent field.
            recorded = read_private(journal)['intent']['argv']
            if recorded == native_check_argv(spec, 'devflow-check', project, legacy_command):
                command = legacy_command
            elif recorded != native_check_argv(spec, 'devflow-check', project, command):
                raise ValueError('retained Python preparation command is not admitted')
        result.append({'id': dependency_id,
                       'cwd': relative, 'timeout_seconds': 600,
                       'argv': command,
                       'generated_directories': [
                           (project / '.venv').relative_to(checkout).as_posix()],
                       'plan_provenance': provenance})
        if spec.get("gate_selections_version") == 2 and all(
                any(test.relative_to(checkout).as_posix() in check.get("required_selectors", [])
                    for check in result if check.get("junit_required")) for test in tests):
            continue  # Selected admitted recipes already prove these exact sources.
        check_id = 'planned-pytest-' + key
        report = evidence / check_id / 'pytest-artifacts' / 'junit.xml'
        result.append({'id': check_id, 'kind': 'test', 'cwd': relative,
                       'timeout_seconds': 900,
                       'argv': [str(project / '.venv/bin/python'), '-m', 'pytest',
                                *provenance['test_paths'], '-q',
                                *(['-o', 'pythonpath=src'] if (project / 'src').is_dir() else []),
                                '--junitxml=' + str(report)],
                       'test_count_regex': r'(\d+) passed', 'min_tests': 1,
                       'plan_provenance': provenance,
                       **({'required_selectors': [test.relative_to(checkout).as_posix()
                                                  for test in tests],
                           'selector_evidence_version': 1, 'selector_project': relative}
                          if spec.get("gate_selections_version") == 2 else {})})
    node_projects: dict[Path, list[Path]] = {}
    for test in node_tests:
        project = next((p for p in test.parents if p.is_relative_to(checkout)
                        and (p / 'package.json').is_file()), None)
        if project is None:
            raise ValueError('selected Vitest test has no tracked package owner')
        package = project / 'package.json'
        if (package.is_symlink() or package.resolve(strict=True) != package
                or _git(checkout, 'ls-files', '--', package.relative_to(checkout).as_posix())
                != package.relative_to(checkout).as_posix()
                or 'vitest' not in {**json.loads(package.read_text()).get('dependencies', {}),
                                   **json.loads(package.read_text()).get('devDependencies', {})}):
            raise ValueError('selected Node tests require a tracked Vitest package')
        for filename in ('package.json', 'pnpm-lock.yaml'):
            path = checkout / filename
            if (path.is_symlink() or path.resolve(strict=True) != path
                    or _git(checkout, 'ls-files', '--', filename) != filename):
                raise ValueError('selected Node tests require the frozen workspace lock')
        node_projects.setdefault(project, []).append(test)
    for project, tests in sorted(node_projects.items()):
        provenance = {'accepted_plan_sha256': digest(plan),
                      'selection_sha256': digest(spec.get('verification_test_paths', [])),
                      'project': project.relative_to(checkout).as_posix(),
                      'metadata': {str(p.relative_to(checkout)): hashlib.sha256(
                          p.read_bytes()).hexdigest() for p in (
                              project / 'package.json', checkout / 'package.json',
                              checkout / 'pnpm-lock.yaml')},
                      'test_paths': [str(test.relative_to(project)) for test in tests]}
        check_id = 'planned-vitest-' + digest(provenance)[:16]
        result.append({'id': check_id, 'kind': 'test', 'cwd': provenance['project'],
                       'timeout_seconds': 900, 'plan_provenance': provenance,
                       'argv': ['corepack', 'pnpm', 'exec', 'vitest', 'run',
                                *provenance['test_paths'], '--reporter=default',
                                '--reporter=junit', '--outputFile=' + str(
                                    evidence / check_id / 'pytest-artifacts/junit.xml')],
                       'test_count_regex': r'Tests\s+(\d+) passed', 'min_tests': 1,
                       **({'required_selectors': [test.relative_to(checkout).as_posix()
                                                  for test in tests],
                           'selector_evidence_version': 1,
                           'selector_project': provenance['project']}
                          if spec.get("gate_selections_version") == 2 else {})})
    return result
