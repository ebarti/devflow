#!/bin/sh
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
role_hash=$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$repo_root/runtime/src/devflow_temporal/role_runner.py")
payload_hash=$(python3 "$repo_root/runtime/src/devflow_temporal/payload.py" "$repo_root/runtime/src/devflow_temporal" "$repo_root/runtime/docker/landlock_exec.py")
dockerfile_hash=$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$repo_root/runtime/docker/Dockerfile")
codex_hash=50b06603bdcdac39b714f5c3e68583c002b8ad8779ebfdaaf4932ff016b379c0
image_tag=${1:-devflow-runtime-candidate:local}
docker build --platform linux/arm64 \
  --build-arg "ROLE_RUNNER_SHA256=$role_hash" \
  --build-arg "RUNTIME_PAYLOAD_SHA256=$payload_hash" \
  --build-arg "CODEX_BIN_SHA256=$codex_hash" \
  --build-arg "DOCKERFILE_SHA256=$dockerfile_hash" \
  -f "$repo_root/runtime/docker/Dockerfile" -t "$image_tag" "$repo_root"
docker image inspect "$image_tag" --format '{{.Id}} {{.Os}}/{{.Architecture}}'
