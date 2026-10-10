"""Balance the complete runtime suite across isolated CI hosts and verify coverage."""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import time
from collections import Counter
from pathlib import Path


def assign(nodes: list[str], timings: dict[str, float], count: int) -> dict[str, int]:
    if (type(count) is not int or count < 1 or not nodes or len(set(nodes)) != len(nodes)
            or any(not isinstance(node, str) or not node for node in nodes)):
        raise ValueError('expected a nonempty unique collection and a positive shard count')
    modules = Counter(node.split('::', 1)[0] for node in nodes)
    costs = {name: timings.get(name, max(1.0, cases * 0.5)) for name, cases in modules.items()}
    if any(type(cost) not in (int, float) or not math.isfinite(cost) or cost < 0
           for cost in costs.values()):
        raise ValueError('module timings must be finite nonnegative seconds')
    totals = [0.0] * count
    owners = {}
    for name in sorted(modules, key=lambda name: (-costs[name], name)):
        index = min(range(count), key=lambda index: (totals[index], index))
        owners[name] = index
        totals[index] += max(costs[name], 0.001)
    return {node: owners[node.split('::', 1)[0]] for node in nodes}


class Partition:
    def __init__(self, index, count, timings, revision):
        self.index, self.count, self.timings, self.revision = index, count, timings, revision
        self.report = None
        self.finished = []

    def pytest_collection_modifyitems(self, config, items):
        nodes = [item.nodeid for item in items]
        owners = assign(nodes, self.timings, self.count)
        selected = [item for item in items if owners[item.nodeid] == self.index]
        rejected = [item for item in items if owners[item.nodeid] != self.index]
        if not selected:
            raise ValueError('CI shard has no tests')
        self.report = {'schema': 1, 'revision': self.revision, 'index': self.index,
                       'count': self.count, 'collected': sorted(nodes),
                       'selected': sorted(item.nodeid for item in selected)}
        config.hook.pytest_deselected(items=rejected)
        items[:] = selected

    def pytest_runtest_logreport(self, report):
        if report.when == "teardown":
            self.finished.append(report.nodeid)


def verify(reports: list[dict], timings: dict[str, float], count: int, revision: str):
    if len(reports) != count or {report.get('index') for report in reports} != set(range(count)):
        raise ValueError('missing or duplicate shard results')
    collected = reports[0]['collected']
    owners = assign(collected, timings, count)
    for report in reports:
        expected = sorted(node for node in collected if owners[node] == report['index'])
        if (report.get('schema') != 1 or report.get('count') != count
                or report.get('revision') != revision or report.get('exit_code') != 0
                or report.get('collected') != collected or not expected
                or report.get('selected') != expected or report.get('finished') != expected):
            raise ValueError('shard did not pass its exact complete partition on this revision')
    return {'tests': len(collected), 'shards': count, 'revision': revision}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--count', type=int, default=4)
    parser.add_argument('--index', type=int)
    parser.add_argument('--verify', type=Path)
    parser.add_argument('--output', type=Path, default=Path('ci-results'))
    parser.add_argument('--revision', default=os.environ.get('GITHUB_SHA', ''))
    args = parser.parse_args()
    if not re.fullmatch(r'[0-9a-f]{40}', args.revision) or not 1 <= args.count <= 16:
        parser.error('an exact Git revision and 1–16 shards are required')
    timings = json.loads(Path(__file__).with_name('ci_timings.json').read_text())['seconds']
    if args.verify is not None:
        if args.index is not None:
            parser.error('--verify cannot also run a shard')
        reports = [json.loads(path.read_text())
                   for path in sorted(args.verify.glob('shard-*.json'))]
        print(json.dumps(verify(reports, timings, args.count, args.revision)), flush=True)
        return 0
    if args.index is None or not 0 <= args.index < args.count:
        parser.error('a valid --index is required to run a shard')
    import pytest

    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    partition = Partition(args.index, args.count, timings, args.revision)
    started = time.monotonic()
    result = int(pytest.main(['-v', '--tb=short', '-ra', '--durations=20',
                             '--junitxml=' + str(output / f'shard-{args.index}.xml')],
                            plugins=[partition]))
    if partition.report is not None:
        report = {**partition.report, 'exit_code': result,
                  'finished': sorted(partition.finished),
                  'duration_seconds': time.monotonic() - started}
        (output / f'shard-{args.index}.json').write_text(json.dumps(report, indent=2) + '\n')
    return result


if __name__ == '__main__':
    raise SystemExit(main())
