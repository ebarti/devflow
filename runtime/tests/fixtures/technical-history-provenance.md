# Original technical recovery histories

Captured from untouched c04f00eb43eb225728b63c82026ffe97a41cafc2 on a disposable, random-port Temporal server with a temporary database. Activities used explicit fixture stubs; no live service, provider or GitHub effects occurred. Original `fetch_history().to_json()` bytes are retained without editing events.

| File | Workflow | State | Events | SHA-256 |
| --- | --- | --- | --- | --- |
| technical-c04-checks-completed-history.json | technical-checks | completed | 97 | c8dfe4bacb5991d892a3e1829ce73da3458e7d5fffcaa9f7ebdd98fbb73fdb63 |
| technical-c04-suspended-history.json | technical-review | suspended at review activity | 43 | a8ca9e65b5ccb90c0907a4b6f8ff6a6d7c1f7ce874833d707e9d55f4fa77bd28 |
| technical-c04-review-completed-history.json | technical-review | completed after resuming that activity | 91 | b00568a5ff471d21d04139862fbc13b281c231bd99399999ad33e145144595e9 |

Both resume stages are covered. Replay does not run the stub activities. Admission writers can be retired while these already-admitted workflows and their retained readers remain registered.
