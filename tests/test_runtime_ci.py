"""Every collected runtime test belongs to one required, successful CI shard."""
from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('runtime_ci', ROOT / 'runtime/runtime_ci.py')
CI = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CI)


class RuntimeCI(unittest.TestCase):
    nodes = ['tests/test_crash.py::test_recovery[a]', 'tests/test_crash.py::test_recovery[b]',
             'tests/test_store.py::test_save', 'tests/test_new.py::test_added',
             'tests/test_other.py::test_case']
    timings = {'tests/test_crash.py': 40, 'tests/test_store.py': 30,
               'tests/test_other.py': 20}
    revision = 'a' * 40

    def reports(self):
        owners = CI.assign(self.nodes, self.timings, 3)
        return [{'schema': 1, 'revision': self.revision, 'index': index, 'count': 3,
                 'collected': sorted(self.nodes), 'exit_code': 0,
                 'finished': sorted(node for node in self.nodes if owners[node] == index),
                 'selected': sorted(node for node in self.nodes if owners[node] == index)}
                for index in range(3)]

    def test_new_modules_are_included_and_module_cases_stay_together(self):
        owners = CI.assign(self.nodes, self.timings, 3)
        self.assertEqual(set(owners), set(self.nodes))
        self.assertEqual(owners[self.nodes[0]], owners[self.nodes[1]])
        self.assertEqual(owners, CI.assign(list(reversed(self.nodes)), self.timings, 3))
        self.assertEqual({0, 1, 2}, set(owners.values()))
        result = CI.verify(self.reports(), self.timings, 3, self.revision)
        self.assertEqual(result['tests'], len(self.nodes))

    def test_pytest_hook_preserves_case_order_and_reports_deselection(self):
        items = [SimpleNamespace(nodeid=node) for node in self.nodes]
        dropped = []
        config = SimpleNamespace(hook=SimpleNamespace(
            pytest_deselected=lambda items: dropped.extend(items)))
        plugin = CI.Partition(0, 3, self.timings, self.revision)
        plugin.pytest_collection_modifyitems(config, items)
        self.assertEqual([item.nodeid for item in items], self.nodes[:2])
        self.assertEqual([item.nodeid for item in dropped], self.nodes[2:])
        self.assertEqual(plugin.report['collected'], sorted(self.nodes))

    def test_missing_duplicate_failed_changed_or_incomplete_results_are_rejected(self):
        for drift in ['missing', 'duplicate', 'failed', 'revision', 'collection',
                      'omitted', 'duplicate-test', 'wrong-shard', 'count', 'unfinished']:
            with self.subTest(drift=drift):
                reports = copy.deepcopy(self.reports())
                if drift == 'missing':
                    reports.pop()
                elif drift == 'duplicate':
                    reports[1] = copy.deepcopy(reports[0])
                elif drift == 'failed':
                    reports[0]['exit_code'] = 1
                elif drift == 'revision':
                    reports[0]['revision'] = 'b' * 40
                elif drift == 'collection':
                    reports[0]['collected'].append('tests/test_changed.py::test_case')
                elif drift == 'omitted':
                    reports[0]['selected'].pop()
                elif drift == 'duplicate-test':
                    reports[0]['selected'].append(reports[0]['selected'][0])
                elif drift == 'wrong-shard':
                    reports[0]['selected'], reports[1]['selected'] = (
                        reports[1]['selected'], reports[0]['selected'])
                elif drift == 'unfinished':
                    reports[0]['finished'].pop()
                else:
                    reports[0]['count'] = 4
                with self.assertRaises(ValueError):
                    CI.verify(reports, self.timings, 3, self.revision)

    def test_empty_duplicate_or_invalid_input_cannot_silently_pass(self):
        for nodes, timings, count in [([], {}, 2), (['same', 'same'], {}, 2),
                                      (self.nodes, self.timings, 0),
                                      (self.nodes, {'tests/test_crash.py': float('nan')}, 3)]:
            with self.subTest(nodes=nodes, count=count), self.assertRaises(ValueError):
                CI.assign(nodes, timings, count)
        with self.assertRaises(ValueError):
            CI.verify([], {}, 3, self.revision)


if __name__ == '__main__':
    unittest.main()
