---
name: devflow-planning
description: Plan architecture, dependencies, implementation slices and verification for a consequential development change or a plan-only request.
---

# Plan the change

## Steps

1. **Use the inspection.** Plan in the main task, using the accepted outcome and inspected production paths, contracts and dependencies. Fill material evidence gaps before assigning implementation.

2. **Choose the owning layer.** State where the behavior belongs, the interfaces and invariants that must hold, and why. Compare alternatives only where they change correctness, complexity, compatibility or operational risk. Prefer the smallest coherent design. Settle user decisions directly in the main task.

3. **Slice the work.** Split consequential work into slices with one observable outcome, owned files or modules, dependencies, branch/base and concise verification steps with expected results. Each coherent slice becomes a PR opened after its first meaningful commit. Sequential slices and features developed before prior work merges form one gh stack even when logically independent; follow [PR workflow](../devflow/references/pr-workflow.md).

4. **Map acceptance to evidence.** Map the user's acceptance conditions to observable results, including each required mode and relevant failure and recovery path. For product-use claims, identify the actual entry point to exercise and label simulated dependencies; simulation cannot satisfy a requirement to exercise the real path. Follow the target project's verification policy; Devflow adds no package-wide gate.

5. **Keep it proportional.** A small fix may need only a few sentences; a migration needs ordering and recovery. Preserve accepted decisions and mark unresolved facts explicitly. A plan-only request produces the plan without initiating implementation or external work.

6. **Hand off once.** Save the plan or its durable reference in the [work record](../devflow/references/state.md). Include inspection evidence, design rationale, acceptance conditions, ordered slices, exact checks and expected results, current PR/stack state, explicit limits and the authorized endpoint. When implementation is authorized, [dispatch one execution coordinator](../devflow-coordinating/SKILL.md) for the bounded request or batch. It owns worker dispatch and repair loops; return to planning only for a material design or scope decision.

## Example slice

```text
Slice 2 of 3: chunked upload client.
Outcome: files over 8 MB are sent in 4 MB chunks and reassembled by the existing endpoint.
Owns: src/upload/chunker.py, tests/test_chunker.py. Branch: feat/upload-client; base: feat/upload-endpoint (unmerged slice 1).
Verify: `pytest tests/test_chunker.py -q` passes; a 50 MB upload succeeds against the local server (real path); the proxy is simulated.
```
