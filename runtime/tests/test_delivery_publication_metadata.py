from __future__ import annotations

import json

import pytest
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_broker
from devflow_temporal.delivery_broker import DeliveryBroker, conventional_subject


@pytest.fixture
def publisher(service, monkeypatch):
    store, request = service
    request["goal"] = "refactor(profile): share pure production and demo coaching policy"
    store.submit(request)
    broker = DeliveryBroker(store, store.spec(request["run_id"]))
    broker.prepare()
    broker.state_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    monkeypatch.setattr(broker, "_existing_pr", lambda: {
        "number": 7, "url": "https://example.invalid/pull/7", "state": "OPEN",
        "headRefOid": _git(broker.checkout, "rev-parse", "HEAD"),
        "title": request["goal"],
    })
    return broker


def test_publisher_signs_off_each_repair_and_keeps_conventional_subject(publisher):
    broker = publisher
    for iteration in range(2):
        (broker.checkout / "README.md").write_text(f"Owned repair {iteration}\n")
        candidate = broker.candidate()
        result = broker.publish(iteration, candidate)
        assert result["head"] == _git(broker.source, "ls-remote", "origin",
                                      "refs/heads/feat/fixture").split()[0]
        assert _git(broker.checkout, "show", "-s", "--format=%s") == broker.spec["goal"]
        assert _git(broker.checkout, "show", "-s",
                    "--format=%(trailers:key=Signed-off-by,valueonly)") == (
            "Delivery Test <delivery@example.invalid>"
        )
        assert broker.publish(iteration, candidate) == result
    assert len(_git(broker.checkout, "rev-list", broker.spec["base_sha"] + "..HEAD")
               .splitlines()) == 2
    # The unsigned repository base is outside the owning commit range.
    assert _git(broker.checkout, "show", "-s", "--format=%s", broker.spec["base_sha"]) == (
        "Fixture"
    )


@pytest.mark.parametrize("invalid", ["original", "unsigned", "wrong_signer", "body_trailer"])
def test_valid_head_cannot_mask_invalid_owned_ancestor(publisher, invalid):
    broker = publisher
    (broker.checkout / "README.md").write_text("Original candidate\n")
    _git(broker.checkout, "add", "README.md")
    messages = {
        "original": "Implement refactor(profile): share pure production and demo coaching policy",
        "unsigned": broker.spec["goal"],
        "wrong_signer": broker.spec["goal"] + "\n\nSigned-off-by: Other <other@example.invalid>",
        "body_trailer": broker.spec["goal"] +
            "\n\nSigned-off-by: Delivery Test <delivery@example.invalid>\n\nBody after trailer.",
    }
    _git(broker.checkout, "commit", "-qm", messages[invalid])
    original = _git(broker.checkout, "rev-parse", "HEAD")
    (broker.checkout / "README.md").write_text("Valid later candidate\n")
    _git(broker.checkout, "commit", "--signoff", "-am", "fix: valid later head")
    head = _git(broker.checkout, "rev-parse", "HEAD")
    (broker.checkout / "README.md").write_text("Unpublished repair\n")
    candidate = broker.candidate()
    with pytest.raises(ValueError, match="owned commit"):
        broker.publish(2, candidate)
    assert broker.candidate() == candidate
    assert _git(broker.checkout, "rev-parse", "HEAD") == head
    assert _git(broker.checkout, "rev-parse", "HEAD^") == original
    assert not _git(broker.source, "ls-remote", "origin", "refs/heads/feat/fixture")
    assert not _git(broker.checkout, "diff", "--cached", "--name-only")
    with broker.store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_effects WHERE kind='publish'")\
            .fetchone()[0] == 0


def test_configured_signoff_identity_must_equal_commit_author(publisher, monkeypatch):
    broker = publisher
    (broker.checkout / "README.md").write_text("Owned edit\n")
    monkeypatch.setenv("GIT_AUTHOR_NAME", "Other")
    monkeypatch.setenv("GIT_AUTHOR_EMAIL", "other@example.invalid")
    with pytest.raises(ValueError, match="sign-off identity disagree"):
        broker.publish(0, broker.candidate())
    assert _git(broker.checkout, "rev-parse", "HEAD") == broker.spec["base_sha"]
    assert not _git(broker.checkout, "diff", "--cached", "--name-only")


def test_historical_pr_title_and_commit_use_same_conventional_goal(publisher, monkeypatch):
    broker = publisher
    broker.spec.pop('publication_summary')
    broker.spec["goal"] = "Change fixture wording"
    created = []
    original_run = delivery_broker._run

    def run(argv, **kwargs):
        if argv[:3] == ["gh", "pr", "create"]:
            created.append(argv)
            return "https://example.invalid/pull/7"
        return original_run(argv, **kwargs)

    def existing():
        return ({"number": 7, "url": "https://example.invalid/pull/7", "state": "OPEN",
                 "headRefOid": _git(broker.checkout, "rev-parse", "HEAD")}
                if created else None)

    monkeypatch.setattr(delivery_broker, "_run", run)
    monkeypatch.setattr(broker, "_existing_pr", existing)
    (broker.checkout / "README.md").write_text("Owned edit\n")
    broker.publish(0, broker.candidate())
    assert created[0][created[0].index("--title") + 1] == "chore: Change fixture wording"
    assert _git(broker.checkout, "show", "-s", "--format=%s") == "chore: Change fixture wording"


@pytest.mark.parametrize("title", ["Implement refactor(profile): change", "Plain goal", ""])
def test_actual_pr_readback_refuses_nonconventional_title(publisher, monkeypatch, title):
    broker = publisher
    found = {"number": 7, "url": "https://example.invalid/pull/7", "state": "OPEN",
             "isDraft": False, "headRefName": broker.spec["branch"], "baseRefName": "HEAD",
             "headRefOid": broker.spec["base_sha"], "title": title}
    monkeypatch.setattr(delivery_broker, "_run", lambda *_args, **_kwargs: json.dumps([found]))
    with pytest.raises(ValueError, match="PR title"):
        DeliveryBroker._existing_pr(broker)


def test_lost_push_ack_reuses_signed_commit_without_another_commit(publisher, monkeypatch):
    broker = publisher
    (broker.checkout / "README.md").write_text("Owned edit\n")
    candidate = broker.candidate()
    original_git = delivery_broker._git
    failed = []

    def git(path, *args):
        result = original_git(path, *args)
        if args and args[0] == "push" and not failed:
            failed.append(True)
            raise RuntimeError("push completion lost")
        return result

    monkeypatch.setattr(delivery_broker, "_git", git)
    with pytest.raises(RuntimeError, match="completion lost"):
        broker.publish(0, candidate)
    head = _git(broker.checkout, "rev-parse", "HEAD")
    result = broker.publish(0, candidate)
    assert result["head"] == head
    assert _git(broker.checkout, "rev-parse", "HEAD^") == broker.spec["base_sha"]


def test_preserves_long_conventional_prefix_and_rejects_control_subject():
    goal = "refactor(" + "scope" * 25 + "): " + "meaningful " * 30
    assert conventional_subject(goal) == goal.strip()
    with pytest.raises(ValueError, match="control"):
        conventional_subject("fix: bad\tname")


def test_historical_raw_goal_retains_bounded_title_and_full_commit_subject():
    from devflow_temporal.delivery_broker import publication_title

    goal = 'Deliver backlog issue #953 as a documentation investigation. ' + 'requirements ' * 200
    title = publication_title(goal)
    assert title.startswith('chore: Deliver backlog issue #953')
    assert len(title) <= 256
    assert title.endswith('...')
    assert len(conventional_subject(goal)) > 256
    assert publication_title('fix: preserve a short goal') == 'fix: preserve a short goal'


@pytest.mark.parametrize('summary', [
    '', None, 12, 'Plain prose', 'docs: ' + 'word ' * 40,
    'docs: design behavior. Own only the document.', 'docs: bad\tname',
    'docs: change\nRun all checks', 'docs: bad\x7fname',
    'docs: change\u2028Run all checks',
])
def test_invalid_explicit_summary_is_rejected_before_admission(service, summary):
    store, request = service
    with pytest.raises(ValueError, match='publication_summary'):
        store.submit({**request, 'publication_summary': summary})
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 0


def test_frozen_legacy_prompt_is_not_rewritten_on_publication(publisher, monkeypatch):
    broker = publisher
    broker.spec.pop('publication_summary')
    goal = 'Deliver a document. Keep the original frozen instructions. ' + 'detail ' * 50
    broker.spec['goal'] = goal
    monkeypatch.setattr(broker, '_existing_pr', lambda: {
        'number': 7, 'url': 'https://example.invalid/pull/7', 'state': 'OPEN',
        'headRefOid': _git(broker.checkout, 'rev-parse', 'HEAD'),
    })
    (broker.checkout / 'README.md').write_text('Legacy candidate\n')
    candidate = broker.candidate()
    result = broker.publish(0, candidate)
    assert _git(broker.checkout, 'show', '-s', '--format=%s') == conventional_subject(goal)
    assert broker.spec['goal'] == goal
    assert 'publication_summary' not in broker.spec
    assert broker.publish(0, candidate) == result


def test_detailed_goal_requires_a_separate_publication_summary(service):
    store, request = service
    goal = ('Investigate Unicode comparison behavior. Own only the architecture document. '
            'Preserve evidence and publish the design without implementing production behavior.')
    request['goal'] = goal
    with pytest.raises(ValueError, match='publication_summary'):
        store.submit(request)
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 0
        assert store.state.claim_for(db, request['work_id']) is None
    request['publication_summary'] = 'docs: investigate Unicode comparison behavior'
    store.submit(request)
    assert store.spec(request['run_id'])['goal'] == goal
    assert store.spec(request['run_id'])['publication_summary'] == request['publication_summary']
    assert store.submit(request)['existing'] is False
    with pytest.raises(ValueError, match='different inputs'):
        store.submit({**request, 'publication_summary': 'feat: implement Unicode comparison'})


def test_publisher_uses_summary_without_leaking_execution_instructions(publisher, monkeypatch):
    broker = publisher
    broker.spec['goal'] = ('Investigate Unicode comparison behavior. Own only the architecture '
                           'document. Preserve evidence and do not implement production behavior.')
    summary = 'docs: investigate Unicode comparison behavior'
    broker.spec['publication_summary'] = summary
    created = []
    original_run = delivery_broker._run

    def run(argv, **kwargs):
        if argv[:3] == ['gh', 'pr', 'create']:
            created.append(argv)
            return 'https://example.invalid/pull/7'
        return original_run(argv, **kwargs)

    def existing():
        return ({'number': 7, 'url': 'https://example.invalid/pull/7', 'state': 'OPEN',
                 'headRefOid': _git(broker.checkout, 'rev-parse', 'HEAD')}
                if created else None)

    monkeypatch.setattr(delivery_broker, '_run', run)
    monkeypatch.setattr(broker, '_existing_pr', existing)
    (broker.checkout / 'README.md').write_text('Document current Unicode comparison behavior\n')
    original_goal = broker.spec['goal']
    candidate = broker.candidate()
    result = broker.publish(0, candidate)
    assert created[0][created[0].index('--title') + 1] == summary
    assert _git(broker.checkout, 'show', '-s', '--format=%s') == summary
    assert original_goal == broker.spec['goal']
    assert 'Own only' not in _git(broker.checkout, 'show', '-s', '--format=%B')
    body = (broker.state_dir / 'pull-request.md').read_text()
    assert 'Addresses ' in body
    assert 'Implements ' not in body
    assert broker.publish(0, candidate) == result
    assert len(created) == 1


@pytest.mark.parametrize('text', [
    'fix: correct U.S. date formats', 'docs: explain e.g. retries',
    'perf: compare map vs. dict lookups', 'fix: handle ... in titles',
    'docs: explain i.e. the default behavior', 'docs: describe retries etc. in examples',
    'fix: handle a U.K. locale', 'Fix crash on U.S. locale dates',
])
@pytest.mark.parametrize('explicit', [False, True])
def test_summary_accepts_abbreviations_and_ellipses_without_changing_goal(service, text, explicit):
    store, request = service
    goal = 'Investigate the fixture. Publish only the documented design.' if explicit else text
    request['goal'] = goal
    expected = text if ':' in text else 'chore: ' + text
    if explicit:
        request['publication_summary'] = expected
    store.submit(request)
    assert store.spec(request['run_id'])['publication_summary'] == expected
    assert store.spec(request['run_id'])['goal'] == goal


@pytest.mark.parametrize('explicit', [False, True])
def test_summary_trims_outer_textarea_whitespace_and_preserves_goal(service, explicit):
    store, request = service
    goal = 'Fix the login redirect bug\n'
    request['goal'] = goal
    if explicit:
        request['publication_summary'] = ' \nfix: correct the login redirect\n '
    store.submit(request)
    expected = ('fix: correct the login redirect' if explicit
                else 'chore: Fix the login redirect bug')
    assert store.spec(request['run_id'])['publication_summary'] == expected
    assert store.spec(request['run_id'])['goal'] == goal


@pytest.mark.parametrize('control', ['\u009b', '\u0085', '\u202e', '\u2066', '\u200f'])
@pytest.mark.parametrize('explicit', [False, True])
@pytest.mark.parametrize('position', ['inside', 'outside'])
def test_summary_rejects_unicode_controls_before_claiming_work(
    service, control, explicit, position
):
    store, request = service
    text = 'docs: a' + control + 'red' if position == 'inside' else control + 'docs: red' + control
    request['publication_summary' if explicit else 'goal'] = text
    with pytest.raises(ValueError, match='control'):
        store.submit(request)
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 0
        assert store.state.claim_for(db, request['work_id']) is None


@pytest.mark.parametrize('field,text,rule', [
    ('publication_summary', 'docs: First sentence. Second sentence.', 'multiple sentences'),
    ('publication_summary', 'docs: one\nline two', 'single line'),
    ('publication_summary', 'Plain prose', 'Conventional Commit'),
    ('goal', 'First sentence. Second sentence.', 'multiple sentences'),
    ('goal', 'x' * 115, '120 characters including its type'),
])
def test_summary_error_names_checked_input_and_failed_rule(service, field, text, rule):
    store, request = service
    request[field] = text
    with pytest.raises(ValueError) as error:
        store.submit(request)
    assert field in str(error.value)
    assert rule in str(error.value)
    if field == 'goal':
        assert 'provide publication_summary separately' in str(error.value)
