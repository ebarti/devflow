"""Hold the authenticated fresh-installation namespace for intake control.

The qualification gate remains closed until the installation implementation is
composed and independently verified. No caller-supplied proof is accepted.
"""
from contextlib import contextmanager
from pathlib import Path

from .delivery_pause_context import require_intake_dispatcher_cutover


@contextmanager
def intake_namespace_scope(config):
    require_intake_dispatcher_cutover(config)
    try:
        from .delivery_installation_owner import FreshNamespaceScope, fresh_namespace_scope
    except ImportError as exc:
        raise ValueError('fresh namespace custody is unavailable') from exc
    with fresh_namespace_scope(config) as scope:
        if type(scope) is not FreshNamespaceScope:
            raise ValueError('fresh namespace scope is not installation-owned')
        witness = FreshNamespaceScope.verify(scope)
        if witness.get('source_root') != str(Path(__file__).resolve().parents[3]):
            raise ValueError('pause custody is outside the witnessed source generation')
        try:
            yield scope
        finally:
            FreshNamespaceScope.verify(scope)
