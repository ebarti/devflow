The compressed policy histories contain the exact UTF-8 bytes captured from
`c04f00eb43eb225728b63c82026ffe97a41cafc2` on a disposable SDK-owned Temporal
server. No events or payloads were edited. Compression preserves those bytes;
the replay tests verify their SHA-256 before passing them to the SDK Replayer.

The previous release's real policy admission created the recorded row and grant
from an owned native fixture. Fake controller activities then exercised the
unchanged workflow: the completed history passes preserved-candidate gates,
independent review/verification, CI and terminal cleanup/readback; the suspended
history stops at a pending repair preflight with a genuine open timer. The open
history was captured before the fixture execution was terminated for cleanup.

The row fixture also retains the exact original and amended configuration bytes.
Its test substitutes filesystem transport for those historical paths while the
production digest, grant and mode-only validators run unchanged.

Decompressed history hashes:

- completed: `29e4e592ab39e77a410ca6dd55177b975d0b173969e0fd9ca35d996ff25ee4bc`
- suspended: `ff5dd1eb650799a79c814296322f2236a5abc9103b817f235a51fbbdab6c1739`
