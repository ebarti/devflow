"""Owned SDK probes use only the executable paths supplied by native preparation."""

import asyncio
import os
from contextlib import asynccontextmanager

from temporalio.testing import WorkflowEnvironment


@asynccontextmanager
async def _environment(start, **kwargs):
    env = await asyncio.wait_for(start(**kwargs), timeout=30)
    try:
        yield env
    finally:
        await asyncio.wait_for(env.shutdown(), timeout=30)


def time_skipping_env():
    return _environment(
        WorkflowEnvironment.start_time_skipping,
        test_server_existing_path=os.environ["JOBCTRL_TEMPORAL_TEST_SERVER_PATH"],
    )


def local_env():
    return _environment(
        WorkflowEnvironment.start_local,
        dev_server_existing_path=os.environ["JOBCTRL_TEMPORAL_CLI_PATH"],
    )
