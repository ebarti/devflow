"""Pure source-identical assessment applicability for deterministic workflow replay."""

from .contracts import digest


def evidence_applicability(recovery):
    """Preserve provenance: assessment stays on its old head, identical tree is explicit."""
    old = recovery["state"]["candidate"]
    new = recovery["candidate"]
    if old["content_sha256"] != new["content_sha256"]:
        raise ValueError("metadata evidence applicability requires identical source content")
    results = {}
    for role, key in (("review", "review"), ("verify", "qa")):
        assessment = next(
            (
                item
                for item in reversed(recovery["state"]["roles"])
                if item.get("role") == role
                and item.get("candidate") == old
                and item.get("iteration") == recovery["state"]["iteration"]
            ),
            None,
        )
        checked = recovery["state"]["checks"].get(key)
        if (
            not assessment
            or assessment.get("status") != "pass"
            or assessment.get("cleanup") != "confirmed"
            or not isinstance(checked, dict)
            or checked.get("state") != "passed"
            or checked.get("candidate_id") != old["id"]
        ):
            continue
        results[key] = {
            "state": "passed",
            "candidate_id": new["id"],
            "detail": "Existing assessment of identical source; metadata applicability",
            "applicability": {
                "assessment_candidate_id": old["id"],
                "assessment_sha256": digest(assessment),
                "old_head": old["head"],
                "new_head": new["head"],
                "mapping_sha256": digest(recovery["mapping"]),
                "new_provider_turn": False,
            },
        }
    return results
