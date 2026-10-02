#!/bin/sh
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
role_hash=$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$repo_root/runtime/src/devflow_temporal/role_runner.py")
payload_hash=$(python3 "$repo_root/runtime/src/devflow_temporal/payload.py" "$repo_root/runtime/src/devflow_temporal" "$repo_root/runtime/docker/landlock_exec.py")
dockerfile_hash=$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$repo_root/runtime/docker/Dockerfile")
image_tag=${1:-devflow-runtime-candidate:local}
context=$(mktemp -d)
trap 'rm -rf "$context"' EXIT HUP INT TERM
mkdir -p "$context/runtime/docker"
cp -R "$repo_root/runtime/src" "$context/runtime/src"
cp "$repo_root/runtime/docker/landlock_exec.py" "$context/runtime/docker/landlock_exec.py"
cp "$repo_root/runtime/uv.lock" "$context/runtime/uv.lock"
"$repo_root/runtime/.venv/bin/python" -m devflow_temporal.runtime_dependencies \
  --export "$context/runtime/requirements.txt" --build-args "$context/build-args"
set -- --platform linux/arm64 \
  --build-arg "ROLE_RUNNER_SHA256=$role_hash" \
  --build-arg "RUNTIME_PAYLOAD_SHA256=$payload_hash" \
  --build-arg "DOCKERFILE_SHA256=$dockerfile_hash"
while IFS= read -r build_arg; do
  set -- "$@" --build-arg "$build_arg"
done < "$context/build-args"
docker build "$@" \
  -f "$repo_root/runtime/docker/Dockerfile" -t "$image_tag" "$context"
docker image inspect "$image_tag" --format '{{.Id}} {{.Os}}/{{.Architecture}}'
