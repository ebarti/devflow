import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest

from devflow.domain.rules import scope_hash
from devflow.errors import WorkflowError
from devflow.validation import canonical_json, validate_record

ROOT = Path(__file__).resolve().parents[2]


def test_published_contract_examples_validate_and_packaged_schema_matches():
    for path in (ROOT / "docs/design").glob("*.example.json"):
        validate_record(json.loads(path.read_text()))
    assert json.loads((ROOT / "docs/design/contracts.schema.json").read_text()) == json.loads(
        (ROOT / "src/devflow/schemas/contracts.schema.json").read_text()
    )


@pytest.mark.parametrize(
    "value",
    [
        {},
        {"schema_version": 2, "record_type": "work_contract"},
        {"schema_version": 1, "record_type": "invented_proof"},
    ],
)
def test_unknown_records_and_versions_rejected(value):
    with pytest.raises(WorkflowError):
        validate_record(value)


def test_source_issue_cannot_be_an_authority():
    with pytest.raises(WorkflowError):
        validate_record(
            {"schema_version": 1, "record_type": "authority", "source_kind": "github_issue"}
        )


def test_scope_hash_ignores_presentation_but_not_acceptance():
    contract = json.loads((ROOT / "docs/design/work-contract.example.json").read_text())
    display = {**contract, "title": "Changed only display title", "scope_revision": 9}
    assert scope_hash(display) == scope_hash(contract)
    changed = deepcopy(contract)
    changed["acceptance"][0]["expected"] = "A different outcome"
    assert scope_hash(changed) != scope_hash(contract)


def test_canonical_json_preserves_arbitrary_decimal_precision():
    value = {"price": Decimal("0.12345678901234567890123456789"), "small": Decimal("1e-28")}
    serialized = canonical_json(value)
    assert json.loads(serialized, parse_float=Decimal) == value
    assert '"price":0.12345678901234567890123456789' in serialized
