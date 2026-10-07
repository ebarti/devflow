# Previous-code metadata replay fixtures

These are complete SDK histories captured against the actual previous workflow
and activity registrations on an owned, disposable test Temporal server. History
JSON was not edited to make replay pass. Fixture repository/provider boundaries
supply retained test data; these histories do not prove production GitHub effects
or native implementation sessions.

| Fixture | Source commit | Events | SHA256 |
| --- | --- | --- | --- |
| `c04-metadata-history.json` | `c04f00eb43eb225728b63c82026ffe97a41cafc2` | 41 | `61b7c9a5401d9ea90a9fc209a1406633dc0412618dfdf3d23cc6de5fd7c13b3b` |
| `parent96-metadata-history.json` | `05baa3e653f40cf87fd8454a08cd60e5ecf9045c` | 41 | `5b381a6e431fc8bf584ad30a5389e951e22b5ccbb7c128693ba496fd1c2432d3` |

The test-only `restore_admitted_metadata` helper constructs retained rows for legacy
validation tests. It is a validation fixture, not a recorded workflow history or
a replacement production writer.
