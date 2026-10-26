"""Owner preflight reads an isolated tracking snapshot; execution uses the real writer."""
from test_delivery_store import _scope_amendment_fixture

pytest_plugins = ["test_delivery_store"]


def test_scope_preflight_bypasses_state_writer_and_execution_still_commits(service, monkeypatch):
    store, request = service
    _original, state, command, _closed = _scope_amendment_fixture(store, request, monkeypatch)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("preflight must not open the state writer")

    with monkeypatch.context() as scoped:
        scoped.setattr(store.state, "connect", forbidden)
        preview = store.amend_scope("run-1", command, preflight=True)
        assert preview["lifecycle_assessment"]["eligible"]
    response = store.amend_scope("run-1", command)
    assert response["authorized_through_iteration"] == state["iteration"] + 1


def test_ordinary_persisted_status_keeps_lightweight_read(service, monkeypatch):
    from devflow_temporal import delivery_store

    store, request = service
    store.submit(request)

    def forbidden(_path):
        raise AssertionError("ordinary status must not copy the tracking database")

    monkeypatch.setattr(delivery_store, "_observation_tracking_connection", forbidden)
    assert store.persisted_status(request["run_id"])["run_id"] == request["run_id"]
