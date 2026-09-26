#!/bin/sh
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
role_hash=$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$repo_root/runtime/src/devflow_temporal/role_runner.py")
payload_hash=$(python3 "$repo_root/runtime/src/devflow_temporal/payload.py" "$repo_root/runtime/src/devflow_temporal" "$repo_root/runtime/docker/landlock_exec.py")
codex_hash=9cbc3cdcc18ca336523ffa7d64207a1ae1f5991f823081d0a37bcb3a748de093
image_tag=${1:-devflow-runtime-candidate:local}
docker build --platform linux/arm64 \
  --build-arg "ROLE_RUNNER_SHA256=$role_hash" \
  --build-arg "RUNTIME_PAYLOAD_SHA256=$payload_hash" \
  --build-arg "CODEX_BIN_SHA256=$codex_hash" \
  -f "$repo_root/runtime/docker/Dockerfile" -t "$image_tag" "$repo_root"
docker image inspect "$image_tag" --format '{{.Id}} {{.Os}}/{{.Architecture}}'
