# Assemble consumed source from an issue capture

The agent prepares the source, using the confirmed capture result (`envelope["result"]`)
and the observations of material actually consumed. Keep the capture and its
original bytes as evidence. `capture["issue"]["consumed_digest"]` hashes that
observed issue's title/body; it is the issue observation's `content_digest`, not
the aggregate source digest.

Use the current captured IDs and revision without substituting issue numbers,
timestamps or guessed provenance. GitHub capture reports `origin: unknown`;
preserve it even for an issue created by the agent. Retain every inherited
`capture["payload"]["source_lineage"]` observation and every other consumed
comment, attachment, issue or PR observation with its own exact identity,
revision, digest and origin. Missing observation fields require the actual
source readback; do not invent them or drop that material to pass preparation.

Given `capture` and `other_consumed_observations` (an empty list only when no
other material was consumed), run this mapping with the executing release's
Python environment:

```python
from copy import deepcopy

from devflow.validation import digest

issue = capture["issue"]
lineage = deepcopy(capture["payload"].get("source_lineage", []))
lineage.extend(deepcopy(other_consumed_observations))
lineage.append({
    "kind": "issue",
    "repository_id": str(issue["repository_id"]),
    "source_id": str(issue["id"]),
    "creator_id": str(issue["creator_id"]),
    "revision": issue["revision"],
    "content_digest": issue["consumed_digest"],
    "origin": issue["origin"],
})
source = {
    "kind": "github_issue",
    "reference": issue["url"],
    "stable_id": issue["node_id"],
    "lineage": lineage,
    "consumed_digest": digest(lineage),
}
```

Set the contract's `source` to this result and run `work prepare`. The aggregate
uses canonical `devflow.validation.digest(lineage)` over the complete ordered
list; copying the issue content hash into it is incorrect. Preparation reports
missing source prerequisites without changing the source or creating work or
authority. `work ready` still needs the actual conversational `user_request`
and validates the exact source binding.

A confirmed capture replay retains the previously observed content; it is not
a fresh read of later edits. If newer material is consumed, observe its actual
revision/content and preserve the earlier observations rather than relabeling
old hashes as current. An existing work records that authorized delta through
`work amend`. Historical source records remain schema-readable; missing
execution lineage still blocks admission.
