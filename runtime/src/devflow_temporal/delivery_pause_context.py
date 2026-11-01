"""Recognize admitted pause protocols and carry workflow handoff identity."""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PauseContext:
    run_id: str
    workflow_id: str
    execution_run_id: str
    epochs: dict[str, int]


_CURRENT: ContextVar[PauseContext | None] = ContextVar("delivery_pause_context", default=None)


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


def split_activity_request(value, info):
    """Remove the workflow's epoch carrier before stable effect digests are made."""
    if not isinstance(value, dict) or not isinstance(value.get("spec"), dict):
        return value, None
    spec = value["spec"]
    if not enabled(spec) or "_pause_epochs" not in value:
        return value, None
    epochs = value["_pause_epochs"]
    if (not isinstance(epochs, dict) or not epochs
            or any(not isinstance(root, str) or not root or type(epoch) is not int or epoch < 0
                   for root, epoch in epochs.items())
            or not isinstance(spec.get("run_id"), str) or not spec["run_id"]
            or not isinstance(info.workflow_id, str) or not info.workflow_id
            or not isinstance(info.workflow_run_id, str) or not info.workflow_run_id):
        raise ValueError("invalid pause activity authority")
    request = dict(value)
    del request["_pause_epochs"]
    return request, PauseContext(spec["run_id"], info.workflow_id,
                                 info.workflow_run_id, dict(epochs))


@contextmanager
def activate(context):
    token = _CURRENT.set(context)
    try:
        yield
    finally:
        _CURRENT.reset(token)


def publication_handoff(owner, step):
    """Fence an ordinary publication subprocess at its actual launch boundary."""
    from .delivery_execution_authority import activity_authority, require_current
    from .delivery_pause import launch_admission_graph

    spec, store = owner.broker.spec, owner.broker.store
    binding = _CURRENT.get()
    if (not enabled(spec) or binding is None or binding.run_id != spec["run_id"]
            or spec.get("feature_delivery") or spec.get("feature_worker")
            or set(binding.epochs) != {spec["run_id"]}):
        raise ValueError("ordinary publication lacks its complete pause actor binding")
    authority = activity_authority()
    if (authority["run_id"] != binding.run_id
            or authority["workflow_id"] != binding.workflow_id
            or authority["execution_run_id"] != binding.execution_run_id):
        raise ValueError("publication pause actor differs from its SDK authority")

    def validate(db, epochs, workflow_id, execution_run_id, generation):
        if (set(epochs) != {spec["run_id"]} or workflow_id != authority["workflow_id"]
                or execution_run_id != authority["execution_run_id"]
                or generation != authority["generation"]):
            raise ValueError("publication pause graph changed")
        require_current(db, spec, authority)
        actor = db.execute("""SELECT actor_run_id FROM delivery_pause_actors
            WHERE root_id=? AND workflow_id=?""", (spec["run_id"], workflow_id)).fetchone()
        if actor is None or actor["actor_run_id"] != spec["run_id"]:
            raise ValueError("publication pause actor changed")

    return launch_admission_graph(
        store, binding.epochs, owner.key, step, workflow_id=binding.workflow_id,
        execution_run_id=binding.execution_run_id, owner_generation=authority["generation"],
        validate_graph=validate,
    )
