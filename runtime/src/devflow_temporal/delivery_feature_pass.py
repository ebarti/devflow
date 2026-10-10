"""Immutable integration passes over one retained GitHub stack."""

from __future__ import annotations

from .delivery_feature_execution import registry


def checkpoints(spec):
    values = registry(spec).checkpoints(spec["feature_delivery"]["owner"]["issue_id"])
    passes = [value for key, value in values.items() if key.startswith("integration-pass:")]
    current = max(passes, key=lambda value: value["number"]) if passes else None
    if current:
        # A previous pass remains evidence, but cannot qualify a changed base.
        values = {key: value for key, value in values.items()
                  if not key.startswith("verified:") and not (
                      key.startswith("assignment:") and key.endswith(":chunk"))}
        prefix = f"pass:{current['number']}:"
        values.update({key[len(prefix):]: value for key, value in list(values.items())
                       if key.startswith(prefix)})
    values["integration-pass"] = current
    if spec.get("feature_plan_revision"):
        from .delivery_feature_revisions import revision_checkpoints

        values = revision_checkpoints(spec, values)
    return values


def checkpoint_key(spec, key):
    current = spec.get("feature_worker", {}).get("integration_pass")
    if current is None:
        active = checkpoints(spec)["integration-pass"]
        current = active["number"] if active else 0
    if current and (key.startswith("verified:") or (
            key.startswith("assignment:") and key.endswith(":chunk"))):
        key = f"pass:{current}:{key}"
    if spec.get("feature_plan_revision"):
        from .delivery_feature_revisions import revision_checkpoint_key

        key = revision_checkpoint_key(spec, key)
    return key
