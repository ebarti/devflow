# Original delivery failure histories

Both histories were captured unchanged from an isolated Temporal test server,
using the activities in `original_run` in `test_delivery_failure_classification.py`.
They finish blocked after observing pending CI. No event or workflow input was edited.

- `delivery-failure-c04-history.json`: source commit
  `c04f00eb43eb225728b63c82026ffe97a41cafc2`, workflow source SHA256
  `1d6c0ffa05e0e73f7052f4c550d3c0dbb83aafbb97955f48a63e601a49f3f015`;
  input has neither retry version marker.
- `delivery-failure-budget-history.json`: source commit
  `7aefd5ae0abf5c20e0d72a87830ccab003797e76`, workflow source SHA256
  `cf58c8dba094b42edcc1fb7d4d28c7165d4558e7ec6e30fd2ea053ada774143f`;
  input has the original budget marker, without an automatic retry marker.

Each capture has 127 actual events and synthetic client and worker identities.
