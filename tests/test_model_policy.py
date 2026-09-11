import copy
import json

import pytest

from devflow.errors import WorkflowError
from devflow.model_policy import resolve_role_policy, validate_role_policy
from devflow.validation import digest


@pytest.fixture
def config(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text('model = "saved-coordinator-model"\nmodel_reasoning_effort = "max"\n')
    (tmp_path / "agents").mkdir()
    return path


@pytest.mark.parametrize("role,name", [
    ("implementation_worker", "implementer"), ("review", "reviewer"), ("qa", "qa")
])
def test_independent_role_choice_over_saved_coordinator(config, role, name):
    (config.parent / "agents" / f"{name}.toml").write_text(
        f'model = "synthetic-{name}"\nmodel_reasoning_effort = "xhigh"\n'
    )
    policy = resolve_role_policy(role, config_path=config)
    assert (policy["role_name"], policy["model"], policy["reasoning_effort"]) == (
        name, f"synthetic-{name}", "xhigh"
    )
    assert policy["agent_type"] == "default"
    config.write_text('model = "changed-coordinator"\nmodel_reasoning_effort = "low"\n')
    assert resolve_role_policy(role, config_path=config) == policy


def test_configured_alternative_role_file_and_partial_override(config):
    config.write_text(config.read_text() + '[agents.pr-reviewer]\nconfig_file = "custom.toml"\n')
    (config.parent / "custom.toml").write_text(
        'model = "custom-review-model"\nmodel_reasoning_effort = "ultra"\n'
    )
    result = resolve_role_policy(
        "review", config_path=config, role_name="pr-reviewer", overrides={"reasoning_effort": "high"}
    )
    assert (result["model"], result["reasoning_effort"]) == ("custom-review-model", "high")
    assert result["sources"][0]["settings"] == {"config_file": str(config.parent / "custom.toml")}
    assert any(source["reference"] == "explicit-user-overrides" for source in result["sources"])


def test_per_field_fallback_prefers_subagent_defaults(config):
    config.write_text(config.read_text() + (
        '[agents]\ndefault_subagent_model = "subagent-default"\n'
        'default_subagent_reasoning_effort = "high"\n'
    ))
    role_file = config.parent / "agents" / "qa.toml"
    role_file.write_text('model_reasoning_effort = "xhigh"\n')
    result = resolve_role_policy("qa", config_path=config)
    assert (result["model"], result["reasoning_effort"]) == ("subagent-default", "xhigh")
    role_file.write_text('model = "qa-model"\n')
    result = resolve_role_policy("qa", config_path=config)
    assert (result["model"], result["reasoning_effort"]) == ("qa-model", "high")


def test_saved_defaults_and_explicit_model_override(config):
    result = resolve_role_policy("qa", config_path=config, overrides={"model": "session-model"})
    assert (result["model"], result["reasoning_effort"]) == ("session-model", "max")
    assert resolve_role_policy("qa", config_path=config)["model"] == "saved-coordinator-model"


def test_secrets_and_instructions_are_absent_and_do_not_affect_hash(config):
    role_file = config.parent / "agents" / "reviewer.toml"
    role_file.write_text('model = "review-model"\nmodel_reasoning_effort = "high"\n')
    before = resolve_role_policy("review", config_path=config)
    config.write_text(config.read_text() + 'api_key = "secret-one"\ndeveloper_instructions = "unsafe"\n')
    role_file.write_text(role_file.read_text() + 'secret = "secret-two"\ndeveloper_instructions = "text"\n')
    after = resolve_role_policy("review", config_path=config)
    assert before == after
    assert all(value not in json.dumps(after) for value in (
        '"secret"', "secret-one", "secret-two", "developer_instructions", "unsafe"
    ))


@pytest.mark.parametrize("content", [
    '', 'model = "model-only"', 'model_reasoning_effort = "high"',
    'model = ""\nmodel_reasoning_effort = "high"',
    'model = 42\nmodel_reasoning_effort = "high"',
    'model = "model"\nmodel_reasoning_effort = "invented"',
    'model = "model"\nmodel_reasoning_effort = ["high"]',
    'agents = "not-a-table"', 'model = [malformed-secret',
])
def test_missing_or_invalid_settings_fail_explicitly(config, content):
    config.write_text(content)
    with pytest.raises(WorkflowError) as error:
        resolve_role_policy("qa", config_path=config)
    assert error.value.code.startswith("role_policy_")
    assert "malformed-secret" not in str(error.value)


def test_missing_explicit_role_file_is_not_silent_fallback(config):
    config.write_text(config.read_text() + '[agents.qa]\nconfig_file = "missing.toml"\n')
    with pytest.raises(WorkflowError, match="unavailable"):
        resolve_role_policy("qa", config_path=config)


@pytest.mark.parametrize("override", [
    {"secret": "no-copy"}, {"model": None}, {"reasoning_effort": "invented"},
    {"reasoning_effort": "high", "model_reasoning_effort": "low"}, [],
])
def test_invalid_explicit_overrides_do_not_fall_back(config, override):
    with pytest.raises(WorkflowError):
        resolve_role_policy("qa", config_path=config, overrides=override)


@pytest.mark.parametrize("field,value", [
    ("policy_hash", "a" * 64), ("model", "tampered"), ("agent_type", "qa"),
    ("reasoning_effort", []), ("sources", []), ("role", "coordinator"),
])
def test_policy_tampering_rejected(config, field, value):
    policy = resolve_role_policy("qa", config_path=config)
    policy[field] = value
    with pytest.raises(WorkflowError):
        validate_role_policy(policy)


def test_rehashed_outer_policy_does_not_hide_tampered_source(config):
    policy = resolve_role_policy("qa", config_path=config)
    policy["sources"][0]["hash"] = "b" * 64
    policy["policy_hash"] = digest({k: v for k, v in policy.items() if k != "policy_hash"})
    with pytest.raises(WorkflowError, match="source hash"):
        validate_role_policy(policy)


def test_rehashed_policy_rejects_secrets_and_unsubstantiated_settings(config):
    original = resolve_role_policy("qa", config_path=config)
    for replacement in ({"model": "wrong-model", "reasoning_effort": "max"}, {"secret": "value"}):
        policy = copy.deepcopy(original)
        source = policy["sources"][0]
        source["settings"] = replacement
        source["hash"] = digest({k: v for k, v in source.items() if k != "hash"})
        policy["policy_hash"] = digest({k: v for k, v in policy.items() if k != "policy_hash"})
        with pytest.raises(WorkflowError):
            validate_role_policy(policy)
