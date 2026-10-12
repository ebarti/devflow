"""Bounded GitHub transport compatibility preserves the complete protected contract."""
from __future__ import annotations

import ast
import sys
from copy import deepcopy
from pathlib import Path

import pytest
import test_delivery_feature_revision_preparation as preparation_tests
from test_delivery_feature_review_context import context_repository, git
from test_delivery_feature_revision_preparation import (
    feature as feature,
)
from test_delivery_feature_revision_preparation import (
    findings_reconsideration as findings_reconsideration,
)
from test_delivery_feature_revision_preparation import (
    intake_fixture as intake_fixture,
)
from test_delivery_feature_revision_preparation import (
    native_configuration as native_configuration,
)
from test_delivery_feature_revision_preparation import (
    pending_preparation as pending_preparation,
)
from test_delivery_feature_revision_preparation import (
    service as service,
)

from devflow_temporal import delivery_feature_review_context as context
from devflow_temporal import delivery_feature_revisions as revisions

exercise_public_retry = (
    preparation_tests
    .test_public_retry_recovers_partial_claim_and_same_revision_then_charges_seven_and_eight
)


def legacy_github_contract(source):
    """Build the original transport/encoder shape as source DATA, never execute it."""
    tree = ast.parse(source)
    helper = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                  and n.name == "manifest_body")
    encoder = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                   and n.name == "encode_manifest")
    encoder.body = [*deepcopy(helper.body[1:-1]),
                    ast.parse('body = "\\n".join(lines)').body[0], *encoder.body[1:]]
    delivery = next(n for n in tree.body if isinstance(n, ast.ClassDef)
                    and n.name == "GitHubDelivery")
    api = next(n for n in delivery.body if isinstance(n, ast.FunctionDef) and n.name == "api")
    failure = next(n for n in api.body if isinstance(n, ast.If)
                   and ast.unparse(n.test) == "result.returncode")
    failure.body = [failure.body[0], ast.parse(
        "raise GitHubContractError((result.stderr or result.stdout)[:1000], "
        "int(match[1]) if match else None)").body[0]]
    excluded = {"manifest_body", "github_transient", "GitHubTransientError"}
    tree.body = [n for n in tree.body if not (
        isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in excluded
        or isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in {
            "_GH_CONNECTION", "_GH_CERTIFICATE", "PARENT_INDEX_LIMIT"} for t in n.targets))]
    class Literal(ast.NodeTransformer):
        def visit_Name(self, node):
            return ast.Constant(value=60000) if node.id == "PARENT_INDEX_LIMIT" else node
    return ast.unparse(ast.fix_missing_locations(Literal().visit(tree)))


def test_transport_projection_refuses_authority_payload_and_classifier_side_effects():
    current = (Path(context.__file__).parent / "delivery_github_contract.py").read_text()
    expected = context._github_projection(legacy_github_contract(current))
    assert context._github_projection(current) == expected
    refused = (
        current.replace('"--method", method, endpoint', '"--method", "POST", endpoint'),
        current.replace('input=canonical_json(body)', 'input=json.dumps({})'),
        current.replace('MARKER = "<!-- devflow-delivery:v1 -->"', 'MARKER = "wrong"'),
        current.replace('PARENT_INDEX_LIMIT = 60000', 'PARENT_INDEX_LIMIT = 60001'),
        current.replace('    validate_plan,', '    validate_plan as different_validator,'),
        current.replace('def github_transient(', '@exec("mutate()")\ndef github_transient('),
        current.replace('        text = stderr or ""', '        text = globals()'),
        current.replace('    if status is None:',
                        '    re.search = _GH_CONNECTION.search\n    if status is None:'),
        current.replace('def github_transient(status:',
                        'def github_transient[T](status:'),
        current.replace('    if status is None:',
                        '    def nested():\n        return True\n    if status is None:'),
        current.replace('    """A GitHub server fault', '    setattr(re, "search", None)\n'
                        '    """A GitHub server fault'),
        current.replace('r"error connecting to|', 'exec("mutate()") or r"error connecting to|'),
        current.replace('body = manifest_body(manifest)', 'body = manifest_body({})'),
        current + '\ndef arbitrary_extra():\n    return True\n',
    )
    for source in refused:
        assert source != current
        try:
            assert context._github_projection(source) != expected
        except ValueError:
            pass


def test_compatibility_authenticates_payloads_and_keeps_post_receipt_guard_fencing(
    tmp_path, monkeypatch,
):
    root, sources, _, current = context_repository(tmp_path, monkeypatch)
    path = root / context.PREFIX / "delivery_github_contract.py"
    path.write_text(legacy_github_contract(sources[path.name]))
    git(root, "add", ".")
    git(root, "commit", "-qm", "Original protected guard with legacy transport")
    legacy = context.current_runtime_context()
    assert legacy["context_sha256"] == current["context_sha256"]
    assert legacy["guards"] != current["guards"]
    assert context.compatible_external_guards(legacy, current)
    # Once a retry receipt exists, the engine deliberately still fences guard
    # drift in exact replay/result comparisons; compatibility erases no digest.
    old = revisions._without_runtime_provenance({"current_runtime": legacy})
    new = revisions._without_runtime_provenance({"current_runtime": current})
    assert old != new and old["current_runtime"]["guards"] == legacy["guards"]
    forged = {**current, "runtime_payload_sha256": legacy["runtime_payload_sha256"]}
    with pytest.raises(ValueError, match="exact authenticated runtime"):
        context.compatible_external_guards(legacy, forged)
    drift = deepcopy(current)
    drift["guards"]["delivery_source_scope.py"] = "0" * 64
    assert not context.compatible_external_guards(legacy, drift)
    path.write_text(sources[path.name].replace('PARENT_INDEX_LIMIT = 60000',
                                             'PARENT_INDEX_LIMIT = 60001'))
    git(root, "add", ".")
    git(root, "commit", "-qm", "Changed protected limit")
    with pytest.raises(ValueError, match="manifest size limit"):
        context.compatible_external_guards(legacy, context.current_runtime_context())


@pytest.mark.skipif(sys.platform != "darwin", reason="native preparation required")
def test_public_pending_review_survives_transport_refactor_with_same_budget_and_custody(
    pending_preparation, monkeypatch,
):
    p = pending_preparation
    root = context._protected_root()
    path = root / context.PREFIX / "delivery_github_contract.py"
    path.write_text(legacy_github_contract(path.read_text()))
    git(root, "add", ".")
    git(root, "commit", "-qm", "Equivalent protected contract with legacy transport")
    p["runtime"] = context.current_runtime_context()
    assert p["runtime"]["context_sha256"] == p["old_runtime"]["context_sha256"]
    assert p["runtime"]["guards"] != p["old_runtime"]["guards"]
    assert context.compatible_external_guards(p["old_runtime"], p["runtime"])
    # Reuse the real public-entry/partial-claim/native-consumption/adoption
    # rehearsal. Closed Temporal, GitHub and completed roles are fixtures;
    # historical Python is DATA and no provider is invoked.
    exercise_public_retry(p, monkeypatch)
