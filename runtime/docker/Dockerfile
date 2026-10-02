FROM mcr.microsoft.com/playwright@sha256:b0ab6f3cb99aa7803adbc14d9027ec1785fc6e433b97e134e0f8fe61683b6b53

ENV COREPACK_HOME=/usr/local/share/corepack \
    COREPACK_ENABLE_NETWORK=0 \
    PYTHONPATH=/opt/devflow-runtime/src \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    make g++ python3-venv python3-pip \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p "$COREPACK_HOME" \
    && COREPACK_ENABLE_NETWORK=1 corepack prepare pnpm@10.24.0 --activate \
    && chmod -R a+rX "$COREPACK_HOME"
ARG RUNTIME_LOCK_SHA256
ARG KIT_VERSION
ARG KIT_PACKAGE_SHA256
ARG CODEX_SDK_VERSION
ARG CODEX_CLI_VERSION
COPY runtime/uv.lock /opt/devflow-runtime/uv.lock
COPY runtime/requirements.txt /opt/devflow/requirements.txt
RUN echo "$RUNTIME_LOCK_SHA256  /opt/devflow-runtime/uv.lock" | sha256sum -c - \
    && python3 -m venv /opt/devflow-venv \
    && /opt/devflow-venv/bin/pip install --no-cache-dir --require-hashes \
      --index-url https://pypi.org/simple -r /opt/devflow/requirements.txt
RUN mkdir -p /store /deps && chown 501:20 /store /deps

COPY runtime/src /opt/devflow-runtime/src
COPY runtime/docker/landlock_exec.py /opt/devflow/landlock_exec.py
RUN chmod -R a+rX /opt/devflow-runtime/src /opt/devflow
ARG ROLE_RUNNER_SHA256
ARG RUNTIME_PAYLOAD_SHA256
ARG CODEX_BIN_SHA256
ARG DOCKERFILE_SHA256
RUN test -n "$ROLE_RUNNER_SHA256" && test -n "$RUNTIME_PAYLOAD_SHA256" && test -n "$CODEX_BIN_SHA256" \
    && echo "$ROLE_RUNNER_SHA256  /opt/devflow-runtime/src/devflow_temporal/role_runner.py" | sha256sum -c - \
    && test "$(python3 /opt/devflow-runtime/src/devflow_temporal/payload.py /opt/devflow-runtime/src/devflow_temporal /opt/devflow/landlock_exec.py)" = "$RUNTIME_PAYLOAD_SHA256" \
    && echo "$CODEX_BIN_SHA256  /opt/devflow-venv/lib/python3.12/site-packages/codex_cli_bin/bin/codex" | sha256sum -c - \
    && /opt/devflow-venv/bin/python -c 'import os; from importlib.metadata import version; from devflow_temporal.runtime_dependencies import locked_dependency_identity, dependency_build_args; expected = locked_dependency_identity(); assert all(os.environ[key] == value for key, value in dependency_build_args(expected).items()); assert [version(name) for name in ("agent-runtime-kit", "openai-codex", "openai-codex-cli-bin")] == [expected[key] for key in ("kit_version", "codex_sdk_version", "codex_cli_version")]'
LABEL devflow.role_runner_sha256="$ROLE_RUNNER_SHA256" \
      devflow.runtime_payload_sha256="$RUNTIME_PAYLOAD_SHA256" \
      devflow.codex_bin_sha256="$CODEX_BIN_SHA256" \
      devflow.dockerfile_sha256="$DOCKERFILE_SHA256" \
      devflow.runtime_lock_sha256="$RUNTIME_LOCK_SHA256" \
      devflow.kit_version="$KIT_VERSION" \
      devflow.kit_package_sha256="$KIT_PACKAGE_SHA256" \
      devflow.codex_sdk_version="$CODEX_SDK_VERSION" \
      devflow.codex_cli_version="$CODEX_CLI_VERSION"
