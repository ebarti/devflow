"""Exercise the actual listener ownership branch with memory-only process snapshots."""
import ast
import copy
import unittest
from pathlib import Path
from types import SimpleNamespace


def listener_inspector(table, listener):
    source = Path(__file__).parents[1] / "src/devflow_temporal/delivery_native_process.py"
    tree = ast.parse(source.read_text())
    sample = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                  and node.name == "sample")
    branches = [node for node in ast.walk(tree) if isinstance(node, ast.For)
                and isinstance(node.iter, ast.Call)
                and ast.unparse(node.iter.func) == "listeners"]
    assert len(branches) == 1
    port_loop = ast.parse("for port in self.ports:\n    pass\n").body[0]
    port_loop.body = [copy.deepcopy(branches[0])]
    inspect = ast.parse(
        "def inspect(owned):\n    conflict = False\n    observed_ports = {}\n"
    ).body[0]
    inspect.body.append(port_loop)
    inspect.body.extend(ast.parse("return conflict, observed_ports").body)
    module = ast.fix_missing_locations(ast.Module(
        body=[copy.deepcopy(sample), inspect], type_ignores=[],
    ))
    scope = {"process_table": lambda: table, "listeners": listener,
             "self": SimpleNamespace(ports=[8000])}
    exec(compile(module, str(source), "exec"), scope)
    return scope["sample"], scope["inspect"]


def process(parent, identity):
    return {"ppid": parent, "pgid": 100, "stat": "S", "identity": identity}


class NativeListenerSnapshotContract(unittest.TestCase):
    def test_new_descendant_is_authenticated_after_listener_observation(self):
        for nested in (False, True):
            with self.subTest(nested=nested):
                parent = process(1, "parent-start")
                table = {100: parent}
                owned = {100: parent.copy()}

                def listener(_port, table=table, nested=nested):
                    table[101] = process(100, "child-start")
                    if nested:
                        table[102] = process(101, "grandchild-start")
                    return {102 if nested else 101}

                sample, inspect = listener_inspector(table, listener)
                sample(owned)  # The original pre-listener snapshot sees only the parent.
                conflict, observed = inspect(owned)
                self.assertFalse(conflict)
                pid = 102 if nested else 101
                self.assertEqual(observed, {"8000": {"pid": pid, **table[pid]}})

    def test_foreign_listener_remains_a_conflict(self):
        table = {100: process(1, "parent-start"), 200: process(9, "foreign-start")}
        owned = {100: table[100].copy()}
        _, inspect = listener_inspector(table, lambda _port: {200})
        self.assertEqual(inspect(owned), (True, {}))
        self.assertNotIn(200, owned)

    def test_owned_listener_exit_between_snapshots_is_not_a_conflict(self):
        parent = process(1, "parent-start")
        child = process(100, "child-start")
        table = {100: parent, 101: child}
        owned = {pid: entry.copy() for pid, entry in table.items()}

        def listener(_port):
            # lsof saw the owned child just before it exited; ps sees it absent.
            table.pop(101)
            return {101}

        _, inspect = listener_inspector(table, listener)
        self.assertEqual(inspect(owned), (False, {}))
        self.assertEqual(owned[101], child)

    def test_absent_listener_remains_a_conflict(self):
        table = {100: process(1, "parent-start")}
        _, inspect = listener_inspector(table, lambda _port: {200})
        self.assertEqual(inspect({100: table[100].copy()}), (True, {}))

    def test_changed_start_identity_remains_a_conflict(self):
        table = {100: process(1, "replacement-start")}
        owned = {100: process(1, "original-start")}
        _, inspect = listener_inspector(table, lambda _port: {100})
        self.assertEqual(inspect(owned), (True, {}))


def listener_source_fragments():
    # Historical source is fixed data; CI needs no old repository objects.
    old = ("for pid in listeners(port):\n"
           "    if pid not in owned:\n"
           "        sample(owned)\n"
           "    if pid not in owned or process_table().get(pid, {}).get('identity') "
           "!= owned[pid]['identity']:\n"
           "        conflict = True\n"
           "    else:\n"
           "        observed_ports[str(port)] = {'pid': pid, **owned[pid]}\n")
    path = Path(__file__).parents[1] / 'src/devflow_temporal/delivery_native_process.py'
    loops = [node for node in ast.walk(ast.parse(path.read_text()))
             if isinstance(node, ast.For) and isinstance(node.iter, ast.Call)
             and isinstance(node.iter.func, ast.Name) and node.iter.func.id == 'listeners']
    assert len(loops) == 1
    return old, ast.unparse(loops[0]) + '\n'


def test_verification_fingerprint_uses_authenticated_git_objects(monkeypatch):
    from tempfile import TemporaryDirectory

    from test_delivery_feature_review_context import git

    from devflow_temporal import delivery_feature_review_context as context
    from devflow_temporal import delivery_listener_verification as subject

    fragments = listener_source_fragments()
    # Only these tiny AST fragments are data; nothing from the fixture is executed.
    with TemporaryDirectory(prefix='devflow-observer-objects-') as directory:
        root = Path(directory).resolve()
        path = root / context.PREFIX / 'delivery_native_process.py'
        path.parent.mkdir(parents=True)
        git(root, 'init', '-q')
        revisions = []
        for text in fragments:
            path.write_text(text)
            git(root, 'add', '.')
            git(root, 'commit', '-qm', 'Isolated observer source data')
            revisions.append(git(root, 'rev-parse', 'HEAD'))
        payload = context._sources(root, revisions[1])[1]
        monkeypatch.setattr(context, '_protected_root', lambda: root)
        old_identity = {'dirty': False, 'local_digest': None, 'revision': revisions[0],
                        'runtime_payload_sha256': context._sources(root, revisions[0])[1]}
        assert subject._loop(old_identity) == subject._PREVIOUS_LOOP
        git(root, 'replace', revisions[1], revisions[0])
        assert git(root, 'show', revisions[1] + ':' + context.PREFIX
                   + 'delivery_native_process.py') == fragments[0].strip()
        identity = {'dirty': False, 'local_digest': None, 'revision': revisions[1],
                    'runtime_payload_sha256': payload}
        assert subject._loop(identity) == subject._CORRECTED_LOOP
