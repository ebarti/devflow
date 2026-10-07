#!/usr/bin/env python3
"""Owner-operated Temporal deployment adapter for an unchanged retained runtime."""

from __future__ import annotations

import argparse
import asyncio
import functools
import re
import sys
from pathlib import Path

from temporalio.common import VersioningBehavior
from temporalio.worker import WorkerDeploymentConfig, WorkerDeploymentVersion


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-src", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--deployment-name", required=True)
    parser.add_argument("--deployment-build-id", required=True)
    args = parser.parse_args()
    source = args.runtime_src.resolve(strict=True)
    if not (source / "devflow_temporal/delivery_control.py").is_file():
        parser.error("--runtime-src must contain the retained runtime package")
    if (not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", args.deployment_name)
            or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", args.deployment_build_id)):
        parser.error("deployment name and build ID must be bounded identifiers")
    sys.path.insert(0, str(source))
    from devflow_temporal import delivery_control as retained
    from devflow_temporal.delivery_config import DeliveryConfig

    deployment = WorkerDeploymentConfig(
        WorkerDeploymentVersion(args.deployment_name, args.deployment_build_id),
        use_worker_versioning=True, default_versioning_behavior=VersioningBehavior.PINNED,
    )
    # Only inject the supported SDK bootstrap option. No artifact file, frozen spec,
    # activity registration, workflow implementation or execution option is changed.
    retained.Worker = functools.partial(retained.Worker, deployment_config=deployment)
    asyncio.run(retained.worker(DeliveryConfig.load(args.config.resolve(strict=True))))


if __name__ == "__main__":
    main()
