---
name: devflow-planning
description: Plan architecture, dependencies, implementation slices and verification for a consequential development change or a plan-only request.
---

# Plan the change

## Steps

1. **Inspect the affected paths.** Use the accepted outcome and inspect the affected production paths, contracts and dependencies.

2. **Compare only decisive alternatives.** Compare alternatives only where they change correctness, complexity, compatibility or operational risk. Prefer the smallest coherent design.

3. **Slice the work.** Split consequential work into slices with one observable outcome, owned files or modules, dependencies and concise verification steps with expected results.

4. **Map acceptance to evidence.** Map the user's acceptance conditions to observable results, including each required mode and relevant failure and recovery path. For product-use claims, identify the actual entry point to exercise and label simulated dependencies; simulation cannot satisfy a requirement to exercise the real path. Follow the target project's verification policy; Devflow adds no package-wide gate.

5. **Keep it proportional.** A small fix may need only a few sentences; a migration needs ordering and recovery. Preserve accepted decisions and mark unresolved facts explicitly. A plan-only request produces the plan without initiating implementation or external work.

6. **Hand off.** For ongoing work, save the plan or its durable reference in the [work record](../devflow/references/state.md). When implementation is already authorized, proceed through [coordinating](../devflow-coordinating/SKILL.md) without another approval step; each slice becomes the brief for the [implementation worker](../devflow/references/implementation-worker.md), which is dispatched only once the slice's verification steps are exact.

## Example slice

```text
Slice 2 of 3: chunked upload client.
Outcome: files over 8 MB are sent in 4 MB chunks and reassembled by the existing endpoint.
Owns: src/upload/chunker.py, tests/test_chunker.py. Depends on slice 1 (endpoint accepts chunk headers).
Verify: `pytest tests/test_chunker.py -q` passes; a 50 MB upload succeeds against the local server (real path); the proxy is simulated.
```
