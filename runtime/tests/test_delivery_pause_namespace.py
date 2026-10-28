"""Namespace consumer accepts only the concrete installation-owned scope."""
import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType

import pytest

from devflow_temporal import delivery_pause_namespace as namespace


def test_scope_checks_on_both_success_and_exception(monkeypatch):
    events = []
    module = ModuleType('devflow_temporal.delivery_installation_owner')

    class Scope:
        def verify(self):
            events.append('verified')
            return {'source_root': str(Path(namespace.__file__).resolve().parents[3])}

    @contextmanager
    def open_scope(_config):
        events.append('acquired')
        try:
            yield Scope()
        finally:
            events.append('released')

    module.FreshNamespaceScope = Scope
    module.fresh_namespace_scope = open_scope
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(namespace, 'require_intake_dispatcher_cutover', lambda _config: None)
    with pytest.raises(RuntimeError, match='controlled'):
        with namespace.intake_namespace_scope(object()):
            events.append('operation')
            raise RuntimeError('controlled')
    assert events == ['acquired', 'verified', 'operation', 'verified', 'released']


def test_cutover_refuses_a_different_source_generation(monkeypatch):
    from devflow_temporal import delivery_pause_context

    module = ModuleType('devflow_temporal.delivery_installation_owner')

    class Scope:
        def verify(self):
            return {'source_root': '/foreign/source'}

    @contextmanager
    def open_scope(_config):
        yield Scope()

    module.FreshNamespaceScope = Scope
    module.fresh_namespace_scope = open_scope
    monkeypatch.setitem(sys.modules, module.__name__, module)
    with pytest.raises(ValueError, match='outside the witnessed source'):
        delivery_pause_context.require_intake_dispatcher_cutover(object())
