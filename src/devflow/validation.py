"""Versioned record validation at every persistence boundary."""

import hashlib
import json
from decimal import Decimal
from functools import lru_cache
from importlib.resources import files

from jsonschema import Draft202012Validator, FormatChecker

from devflow.errors import WorkflowError


def canonical_json(value):
    """Canonical JSON preserving Decimal as an exact JSON number, never a string."""
    if value is None or isinstance(value, (str, bool, int)):
        return json.dumps(value, ensure_ascii=False, allow_nan=False)
    if isinstance(value, (Decimal, float)):
        number = Decimal(str(value))
        if not number.is_finite():
            raise WorkflowError("invalid_number", "JSON cannot encode a nonfinite number")
        return format(number, "f")
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(canonical_json(item) for item in value) + "]"
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return (
            "{"
            + ",".join(
                json.dumps(key, ensure_ascii=False) + ":" + canonical_json(value[key])
                for key in sorted(value)
            )
            + "}"
        )
    raise WorkflowError("invalid_json", "Unsupported value in exact JSON serialization")


def digest(value):
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


@lru_cache(maxsize=1)
def schema():
    return json.loads(files("devflow").joinpath("schemas/contracts.schema.json").read_text())


def validate_record(record, record_type=None):
    if not isinstance(record, dict):
        raise WorkflowError("invalid_record", "Record must be an object")
    kind = record.get("record_type")
    if record_type and kind != record_type:
        raise WorkflowError("invalid_record", f"Expected {record_type}, got {kind}")
    definitions = schema()["$defs"]
    matching = [
        value
        for value in definitions.values()
        if value.get("properties", {}).get("record_type", {}).get("const") == kind
    ]
    if not matching:
        raise WorkflowError("unknown_record", f"Unknown record type: {kind}")
    document = {**matching[0], "$defs": definitions}
    error = next(
        iter(Draft202012Validator(document, format_checker=FormatChecker()).iter_errors(record)),
        None,
    )
    if error:
        raise WorkflowError("invalid_record", f"{'.'.join(map(str, error.path))}: {error.message}")
    return record
