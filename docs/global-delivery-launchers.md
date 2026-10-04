# Global delivery launchers

Run the scoped installer from the canonical installed checkout after its source
upgrade has passed review/CI and reached a safe boundary for its owning runs.
It reuses `runtime/.venv/bin/devflow-delivery` and `devflow-delivery-mcp` and the
existing service configuration. It creates no runtime, environment, controller
or service actor. The migrated `devflow` command aliases `devflow-delivery`;
it does not preserve the removed legacy CLI API.

Prepare a private (0600) JSON request with these exact fields:

```json
{
  "schema_version": 1,
  "command_id": "global-delivery-1",
  "bin_dir": "/home/user/.local/bin",
  "runtime_dir": "/canonical/devflow/runtime",
  "source_revision": "<exact clean installed Git commit>",
  "source_tree": "<exact installed Git tree>",
  "config_path": "/private/service/config.json",
  "config_sha256": "<SHA256 of unchanged private config bytes>",
  "entry_sha256": {
    "devflow-delivery": "<SHA256 of canonical executable>",
    "devflow-delivery-mcp": "<SHA256 of canonical executable>"
  },
  "expected_before": {
    "devflow": {"type": "symlink", "target": "/home/user/.local/share/devflow/releases/3c1363bd9ff54c4a2a8da1c52fa2662fa34ca4a0/scripts/devflow"},
    "devflow-delivery": {"type": "absent"},
    "devflow-delivery-mcp": {"type": "absent"}
  }
}
```

`devflow` may instead be absent. Existing canonical names must be absent on a
new operation. Recognition binds the old release marker, revision/tree and
exact launcher/project bytes, owner and private ancestry; matching link text
alone is insufficient. Foreign files/links, aliases, changed hashes, source,
config or execution entries refuse before installation.

```sh
sh scripts/install.sh --delivery-launchers preflight --request /private/request.json --sha256 REQUEST_SHA256
sh scripts/install.sh --delivery-launchers apply --request /private/request.json --sha256 REQUEST_SHA256
sh scripts/install.sh --delivery-launchers status --manifest MANIFEST_PATH --sha256 MANIFEST_SHA256
sh scripts/install.sh --delivery-launchers rollback --manifest MANIFEST_PATH --sha256 MANIFEST_SHA256
```

Add the selected bin directory to PATH yourself if it is not already present.
The wrappers invoke the canonical command with `--config` and forward arguments
without shell evaluation. `devflow --help` shows the current delivery contract;
`devflow status` reads the existing service status. The MCP command uses its
existing stdio contract, rather than the delivery status subcommand.

An immutable operation manifest, generated launcher bytes, original legacy
bytes and append-only apply/rollback receipts live under the bin directory's
sibling `share/devflow/delivery-launchers/COMMAND_ID`. Exact replay resumes an
interrupted partial operation or reads an applied result; it creates no extra
environment or grant. Rollback authenticates both old and new owned states,
restores the exact old link and removes only this operation's canonical links.
Drift refuses before further effects. Keep the manifest and hash for rollback.
A rolled-back command cannot be reapplied. Source/runtime bindings are pinned
to the inspected installation; a later upgrade requires fresh applicability
assessment rather than treating an older receipt as new-runtime proof.

The scoped operation preserves classic skill links, role TOMLs, hooks,
reconciler registrations, desktop plugins/settings, service/run proofs and
foreign work. Stopping an owning run's transient actors is separate from a
safe service upgrade; shared services remain healthy at completion.
