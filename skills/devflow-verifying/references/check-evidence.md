# Inspect retained check evidence

A registered check's `artifact_hash` identifies its immutable JSON artifact at
`<state-root>/artifacts/<artifact_hash>`. Use the explicit state root supplied by
the coordinator (`--state-dir`/`DEVFLOW_STATE_DIR`, default
`~/.local/state/devflow`); do not guess it from a checkout or temporary path.

Check the evidence record's candidate/input binding, then verify the artifact's
SHA-256 before inspecting its content. Standard file, hash and JSON tools suffice;
there is no `artifact read` CLI command. With the supplied root and hash in
`devflow_state_root` and `devflow_artifact_hash`:

```sh
python3 - "$devflow_state_root" "$devflow_artifact_hash" <<'PY'
import hashlib, json, pathlib, sys
key = sys.argv[2]
if len(key) != 64 or any(c not in "0123456789abcdef" for c in key):
    raise ValueError("artifact key must be SHA-256")
raw = (pathlib.Path(sys.argv[1]).expanduser() / "artifacts" / key).read_bytes()
if hashlib.sha256(raw).hexdigest() != key:
    raise ValueError("artifact hash mismatch")
artifact = json.loads(raw)
print(json.dumps({k: v for k, v in artifact.items()
                  if k not in {"output", "junit"}}, indent=2))
PY
```

Inspect `output` for original command stdout/stderr. JUnit artifacts also retain
`junit` report text and `report_sha256`, the source report's byte digest. Parse the
retained `junit` string directly, or extract it to an owned private file and label
that file as a derived copy. Inspect the relevant test cases without flooding the
conversation with a whole report. Static checks can omit `junit`; failed checks
may omit it too. Keep their original status and observations.

The runner archives these fields before deleting its owned temporary directory.
The expanded argv and its report path describe past execution. A CLI response
envelope contains evidence metadata, not the raw test output. Never recreate a
removed report by rerunning a passing check when its candidate and inputs match.
Missing/corrupt retained evidence remains a blocker; preserve the failure.
