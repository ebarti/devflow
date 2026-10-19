"""Ordinary and selected owned browser invocations retain real runner evidence."""
from pathlib import Path

import pytest

from devflow_temporal.delivery_browser_qa import _playwright_evidence


def recipe(**changes):
    return {'argv': ['corepack', 'pnpm', 'exec', 'playwright', 'test',
                     'e2e/materials.spec.ts', '--reporter=list'],
            'artifact_paths': ['dist/owned-browser'], **changes}


def test_ordinary_owned_playwright_emits_junit_without_broadening_test_selection(tmp_path):
    qa = recipe()
    argv, env = _playwright_evidence(qa, tmp_path)
    assert argv[:6] == qa['argv'][:6]
    assert '--reporter=list' not in argv
    assert '--reporter=line,junit' in argv
    assert env == {'PLAYWRIGHT_JUNIT_OUTPUT_FILE': str(tmp_path / 'dist/owned-browser/junit.xml')}
    assert '--output=' + str(tmp_path / 'dist/owned-browser/test-results') in argv
    assert not any(arg.startswith('--trace') for arg in argv)


def test_selected_playwright_keeps_existing_trace_and_reporter_contract(tmp_path):
    argv, env = _playwright_evidence(recipe(required_selectors=['e2e/materials.spec.ts']), tmp_path)
    assert '--reporter=line,json,junit' in argv and '--trace=on' in argv
    assert Path(env['PLAYWRIGHT_JUNIT_OUTPUT_FILE']).parent == tmp_path / 'dist/owned-browser'


@pytest.mark.parametrize(
    "qa", [recipe(argv=["node", "owned-browser.cjs"]), recipe(artifact_paths=[])]
)
def test_generic_browser_recipes_keep_their_configured_behavior(tmp_path, qa):
    assert _playwright_evidence(qa, tmp_path) == (qa['argv'], {})


def test_ordinary_reporter_refuses_a_replaced_owned_output_root(tmp_path):
    target = tmp_path / 'precious'
    target.mkdir()
    root = tmp_path / 'dist/owned-browser'
    root.parent.mkdir()
    root.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match='artifact path escaped'):
        _playwright_evidence(recipe(), tmp_path)


def test_old_completed_invocation_cannot_gain_new_reporter_evidence(tmp_path):
    qa = recipe()
    assert _playwright_evidence(qa, tmp_path, reconcile_argv=qa['argv']) == (qa['argv'], {})


def test_new_completed_invocation_keeps_its_real_owned_reporters(tmp_path):
    qa = recipe()
    real = _playwright_evidence(qa, tmp_path)
    assert _playwright_evidence(qa, tmp_path, reconcile_argv=real[0]) == real
