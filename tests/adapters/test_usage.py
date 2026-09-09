"""All accounting inputs are synthetic; no user sessions are opened."""
from decimal import Decimal
from types import SimpleNamespace

from devflow.adapters.usage import PARTITIONS, CcusageAdapter, price_usage


def snapshot(**overrides):
    return {"snapshot_id": "synthetic-price", "rate_version": "fixture-1",
            "model_id": "fixture-model", "service_tier": "fixture-tier",
            "effective_from": "2026-01-01T00:00:00Z",
            "usd_per_million": dict(zip(PARTITIONS, ("2", ".2", "3", "8"), strict=True)),
            "credits_per_million": dict.fromkeys(PARTITIONS, "1.25"), **overrides}


def test_exact_disjoint_prices_and_reasoning_subset():
    tokens = dict(zip(PARTITIONS, (100, 20, 10, 30), strict=True))
    tokens["reasoning_output_tokens"] = 25
    result = price_usage(tokens, snapshot(), model_id="fixture-model",
                         service_tier="fixture-tier", recorded_at="2026-09-01T00:00:00Z")
    assert result["api_equivalent_usd"] == Decimal(".000474")
    assert result["estimated_codex_credits"] == Decimal(".0002")


def test_partial_rates_and_wrong_context_remain_unknown():
    tokens = dict.fromkeys(PARTITIONS, 100)
    result = price_usage(tokens, snapshot(credits_per_million={}), model_id="fixture-model",
                         service_tier="fixture-tier", recorded_at="2026-09-01T00:00:00Z")
    assert result["api_equivalent_usd"] > 0
    assert result["estimated_codex_credits"] is None
    assert result["pricing_status"] == "missing_rate"
    result = price_usage(tokens, snapshot(max_input_tokens=100), model_id="fixture-model",
                         service_tier="fixture-tier", recorded_at="2026-09-01T00:00:00Z")
    assert result["api_equivalent_usd"] is None


def test_ccusage_pins_resolved_latest_and_scopes_data(tmp_path):
    calls = []

    def runner(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(returncode=0, stdout="99.2.1\n" if len(calls) in {1, 3} else '{"session":[]}')

    result = CcusageAdapter(runner).collect(data_root=tmp_path)
    assert result["ccusage_version"] == "99.2.1"
    assert calls[0][0] == ["npx", "ccusage@latest", "--version"]
    assert calls[1][0] == ["npx", "ccusage@latest", "codex", "session", "--json"]
    assert calls[1][1]["env"]["CODEX_HOME"] == str(tmp_path)


def test_exact_json_roundtrip():
    import json

    from devflow.adapters.usage import decimal_json_dumps

    value = {"cost": Decimal("0.000000000000000000000000000001"), "text": "a\"b"}
    assert json.loads(decimal_json_dumps(value), parse_float=Decimal) == value


def test_latest_version_change_is_ambiguous(tmp_path):
    import pytest

    from devflow.errors import WorkflowError

    outputs = iter(["99.1.0", "{}", "99.1.1"])

    def runner(*args, **kwargs):
        return SimpleNamespace(returncode=0, stdout=next(outputs))

    with pytest.raises(WorkflowError, match="version changed"):
        CcusageAdapter(runner).collect(data_root=tmp_path)


def test_reconciliation_never_uses_summary_for_response_attribution():
    from devflow.adapters.usage import reconcile_usage

    assert reconcile_usage([], {"session": []})["status"] == "unsupported_format"
    summary = {"totals": {"inputTokens": 0, "cachedInputTokens": 0, "outputTokens": 0,
                           "reasoningOutputTokens": 0}}
    assert reconcile_usage([], summary)["status"] == "matched"
    summary["totals"]["outputTokens"] = 1
    assert reconcile_usage([], summary)["status"] == "mismatch"
