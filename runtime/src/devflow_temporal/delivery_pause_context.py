"""Recognize only the queued-before-start pause protocol."""
from pathlib import Path


def enabled(spec):
    if "pause_control_version" not in spec:
        return False
    version = spec["pause_control_version"]
    if type(version) is not int or version != 1:
        raise ValueError("invalid pause control version")
    return True


def require_intake_dispatcher_cutover(config):
    """Require the current process's authenticated fresh-generation witness."""
    from .delivery_installation_owner import FreshNamespaceScope, fresh_namespace_scope

    with fresh_namespace_scope(config) as scope:
        if type(scope) is not FreshNamespaceScope:
            raise ValueError("fresh namespace scope is not installation-owned")
        witness = FreshNamespaceScope.verify(scope)
        if witness.get("source_root") != str(Path(__file__).resolve().parents[3]):
            raise ValueError("pause admission is outside the witnessed source generation")


def intake_enabled(spec):
    if "pause_intake_version" not in spec:
        return False
    if type(spec["pause_intake_version"]) is not int or spec["pause_intake_version"] != 1:
        raise ValueError("invalid intake pause version")
    if "pause_control_version" in spec:
        raise ValueError("intake pause cannot share a workflow pause protocol")
    return True


def dispatch_enabled(spec):
    if enabled(spec):
        raise ValueError("running-workflow pause is not supported")
    return intake_enabled(spec)
