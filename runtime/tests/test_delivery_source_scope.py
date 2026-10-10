import pytest

from devflow_temporal.delivery_source_scope import authority, outside_scope, require_authorized


def test_legacy_allowlist_never_becomes_a_directory_grant():
    policy = {"allowed_paths": ["src", "README.md"]}
    assert authority(policy)["version"] == 0
    assert outside_scope(policy, ["src", "src/new.py", "README.md"]) == {"src/new.py"}
    with pytest.raises(ValueError, match="outside allowed paths"):
        require_authorized(policy, ["src/new.py"])


def test_versioned_roots_are_bounded_by_protected_files_and_fixed_paths(tmp_path):
    policy = {
        "source_scope": {
            "version": 1,
            "allowed_roots": ["src"],
            "allowed_files": ["README.md"],
            "protected_paths": ["src/security", "src/locked.py"],
        }
    }
    src = tmp_path / "src"
    src.mkdir()
    (src / "external").symlink_to(tmp_path.parent, target_is_directory=True)
    paths = [
        "src/new.py",
        "README.md",
        "src/security/key.py",
        "src/locked.py",
        "src2/other.py",
        "src/external/other.py",
        "src/.git/config",
    ]
    assert outside_scope(policy, paths, checkout=tmp_path) == {
        "src/security/key.py",
        "src/locked.py",
        "src2/other.py",
        "src/external/other.py",
        "src/.git/config",
    }
    require_authorized(policy, ["src/unexpected.py"], checkout=tmp_path)


@pytest.mark.parametrize(
    "scope",
    [
        {"version": 2, "allowed_roots": ["src"], "allowed_files": [], "protected_paths": []},
        {
            "version": 1,
            "allowed_roots": ["src/../secrets"],
            "allowed_files": [],
            "protected_paths": [],
        },
        {"version": 1, "allowed_roots": [".agents"], "allowed_files": [], "protected_paths": []},
        {"version": 1, "allowed_roots": ["src", "src"], "allowed_files": [], "protected_paths": []},
    ],
)
def test_source_scope_cannot_smuggle_authority(scope):
    with pytest.raises(ValueError):
        authority({"source_scope": scope})


def test_source_scope_requires_explicit_policy_adoption():
    scope = {"version": 1, "allowed_roots": ["src"], "allowed_files": [], "protected_paths": []}
    with pytest.raises(ValueError, match="nonempty legacy allowlist"):
        authority({"allowed_paths": ["src/a.py"], "source_scope": scope})


def test_repo_root_authority_can_protect_control_paths_and_sensitive_source():
    policy = {
        "source_scope": {
            "version": 1,
            "allowed_roots": ["."],
            "allowed_files": [],
            "protected_paths": [".git", ".codex", ".agents", "secrets"],
        }
    }
    assert authority(policy)["protected_paths"] == [".git", ".codex", ".agents", "secrets"]
    assert outside_scope(
        policy,
        ["src/new.py", ".git/config", ".codex/config.toml", ".agents/AGENTS.md", "secrets/token"],
    ) == {".git/config", ".codex/config.toml", ".agents/AGENTS.md", "secrets/token"}
