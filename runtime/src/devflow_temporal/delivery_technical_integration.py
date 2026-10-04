"""One authenticated history-preserving integration, with no feature turn."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import tempfile
import tomllib
from pathlib import Path

from .contracts import canonical_json
from .delivery_broker import _git
from .delivery_gates_admission import _reference
from .delivery_metadata_recovery import _identity, _immutable
from .delivery_resources import _ancestors, read_private


def reference(path, sha256):
    _ancestors(Path(path))
    return _reference(path, sha256)


def _object(kind, raw):
    return hashlib.sha1(kind.encode() + b" " + str(len(raw)).encode() + b"\0" + raw).hexdigest()


def tree_id(index):
    """Compute the complete Git tree in memory; preflight writes no objects."""
    root = {}
    for path, entry in index.items():
        parts = Path(path).parts
        if (
            not parts
            or Path(path).is_absolute()
            or ".." in parts
            or entry["mode"] not in {"100644", "100755", "120000"}
            or not re.fullmatch(r"[0-9a-f]{40}", entry["oid"])
        ):
            raise ValueError("integration index contains an invalid entry")
        node = root
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        if parts[-1] in node:
            raise ValueError("integration index aliases another entry")
        node[parts[-1]] = (entry["mode"], entry["oid"])

    def encode(node):
        raw = b""
        for name in sorted(
            node, key=lambda key: os.fsencode(key) + (b"/" if isinstance(node[key], dict) else b"")
        ):
            value = node[name]
            mode, oid = ("40000", encode(value)) if isinstance(value, dict) else value
            raw += mode.encode() + b" " + os.fsencode(name) + b"\0" + bytes.fromhex(oid)
        return _object("tree", raw)

    return encode(root)


def _index(repo, revision):
    raw = subprocess.check_output(
        ["git", "--no-optional-locks", "-C", str(repo), "ls-tree", "-rz", revision],
        timeout=30,
    )
    result = {}
    for item in raw.split(b"\0"):
        if not item:
            continue
        description, path = item.split(b"\t", 1)
        mode, kind, oid = description.decode().split()
        if kind != "blob":
            raise ValueError("integration cannot adopt a submodule")
        result[os.fsdecode(path)] = {"mode": mode, "oid": oid}
    if tree_id(result) != _git(repo, "rev-parse", revision + "^{tree}"):
        raise ValueError("integration input tree changed")
    return result


def _blob(repo, oid):
    raw = subprocess.check_output(["git", "-C", str(repo), "cat-file", "blob", oid], timeout=30)
    if _object("blob", raw) != oid:
        raise ValueError("integration input blob changed")
    return raw


def _merged(before, owned, main, *, conflict):
    with tempfile.TemporaryDirectory(prefix="devflow-three-tree-") as directory:
        paths = [Path(directory) / name for name in ("owned", "base", "main")]
        for path, raw in zip(paths, (owned, before, main), strict=True):
            path.write_bytes(raw)
        result = subprocess.run(
            ["git", "merge-file", "-p", "--diff3", *map(str, paths)],
            capture_output=True,
            timeout=30,
        )
    if not conflict:
        if result.returncode:
            raise ValueError("integration classified clean overlap now conflicts")
        return result.stdout
    if result.returncode != 1:
        raise ValueError("integration must retain exactly its one classified conflict")
    pattern = re.compile(
        rb"(?m)^<<<<<<<[^\n]*\n(.*?)^\|\|\|\|\|\|\|[^\n]*\n"
        rb"(.*?)^=======\n(.*?)^>>>>>>>[^\n]*\n",
        re.S,
    )
    matches = list(pattern.finditer(result.stdout))
    if len(matches) != 1:
        raise ValueError("integration conflict count changed")
    match = matches[0]
    ours, base, theirs = match.groups()
    if (
        not base
        or not ours.endswith(base)
        or theirs != base.replace(b"138-member", b"143-member")
        or base.count(b"138-member") != 1
    ):
        raise ValueError("integration conflict is not the authorized adjacent inventory title")
    # Keep every inserted coaching byte; only the existing inventory title comes from main.
    return (
        result.stdout[: match.start()] + ours[: -len(base)] + theirs + result.stdout[match.end() :]
    )


def prospective(spec, scope, packet, authority_sha256):
    repo = Path(spec["checkout"])
    if (
        packet.get("authority_sha256") != authority_sha256
        or packet.get("original_base") != scope["frozen_original_base"]
        or packet.get("owned_head") != scope["owned_predecessor_head"]
        or packet.get("authorized_main") != scope["authorized_current_main"]
    ):
        raise ValueError("integration packet does not bind the accepted three inputs")
    base, owned, main = (
        _index(repo, scope[key])
        for key in ("frozen_original_base", "owned_predecessor_head", "authorized_current_main")
    )
    overlaps = {
        path
        for path in base.keys() | owned.keys() | main.keys()
        if owned.get(path) != base.get(path) and main.get(path) != base.get(path)
    }
    classified = {item["path"]: item for item in packet["classified_merged_paths"]}
    if overlaps != set(classified) or len(overlaps) != 6:
        raise ValueError("integration six-path overlap classification changed")
    index, blobs = {}, {}
    for path in sorted(base.keys() | owned.keys() | main.keys()):
        before, ours, theirs = base.get(path), owned.get(path), main.get(path)
        if path in overlaps:
            if (
                not before
                or not ours
                or not theirs
                or {before["mode"], ours["mode"], theirs["mode"]} != {"100644"}
            ):
                raise ValueError("integration overlap type or ownership changed")
            raw = _merged(
                *(_blob(repo, value["oid"]) for value in (before, ours, theirs)),
                conflict=path == scope["conflict_path"],
            )
            item = classified[path]
            if (
                hashlib.sha256(raw).hexdigest() != item["content_sha256"]
                or _object("blob", raw) != item["expected_blob_sha1"]
                or len(raw) != item["bytes"]
            ):
                raise ValueError("integration classified output changed")
            index[path] = {"mode": "100644", "oid": _object("blob", raw)}
            blobs[path] = raw
        else:
            value = theirs if ours == before else ours
            if value:
                index[path] = value
    if (
        canonical_json(index) != canonical_json(packet["index"])
        or len(index) != packet["expected_file_count"]
        or tree_id(index) != packet["expected_complete_tree_sha1"]
    ):
        raise ValueError("integration complete prospective source tree changed")
    dependencies = []
    for path in sorted(base.keys() | owned.keys() | main.keys()):
        if Path(path).name not in {"package.json", "pyproject.toml", "uv.lock", "pnpm-lock.yaml"}:
            continue
        values = [value.get(path) for value in (base, owned, main)]
        if any(value is None for value in values):
            raise ValueError("integration added or removed a dependency input")
        raws = [_blob(repo, value["oid"]) for value in values]
        if raws[0] != raws[1]:
            raise ValueError("integration owned branch changed dependency inputs")
        if raws[1] != raws[2]:
            if path != "workers/automation/pyproject.toml":
                raise ValueError("integration changed a dependency or build input")
            before, after = (tomllib.loads(raw.decode()) for raw in (raws[1], raws[2]))
            artifacts = before["tool"]["hatch"]["build"]["artifacts"]
            expected = {**before, "tool": json.loads(json.dumps(before["tool"]))}
            expected["tool"]["hatch"]["build"]["artifacts"] = [
                *artifacts[:-1],
                "src/jobctrl/assets/interview/*.json",
                artifacts[-1],
            ]
            expected["tool"]["pytest"]["ini_options"]["tmp_path_retention_policy"] = "failed"
            if canonical_json(after) != canonical_json(expected):
                raise ValueError("integration nested preparation input exceeded its typed delta")
        dependencies.append(
            {
                "path": path,
                "before_sha256": hashlib.sha256(raws[1]).hexdigest(),
                "after_sha256": hashlib.sha256(raws[2]).hexdigest(),
            }
        )
    return {
        "index": index,
        "tree": tree_id(index),
        "blobs": blobs,
        "preparation_inputs": dependencies,
    }


def _validate_commit(repo, head, grant):
    expected = grant["integration"]
    if (
        _git(repo, "rev-parse", head + "^{tree}") != expected["tree"]
        or _git(repo, "show", "-s", "--format=%P", head).split()
        != [expected["old_head"], expected["main"]]
        or _git(repo, "show", "-s", "--format=%s", head) != expected["subject"]
        or _git(repo, "show", "-s", "--format=%an <%ae>", head) != expected["signer"]
        or "Signed-off-by: " + expected["signer"]
        not in _git(repo, "show", "-s", "--format=%B", head).splitlines()
        or _git(repo, "show", "-s", "--format=%G?", head) not in {"G", "U"}
    ):
        raise ValueError("integration commit lost exact tree, parents or human signing authority")


def integrate(broker, grant, observed, root):
    """Resume this one signed merge from its owned ref; never metadata force-push."""
    plan = grant["integration"]
    repo = broker.checkout
    ref = f"refs/devflow/technical/{broker.spec['run_id']}/integration"
    remote_main = _git(broker.source, "ls-remote", "origin", "refs/heads/main")
    if not remote_main or remote_main.split()[0] != plan["main"]:
        raise ValueError("integration admitted current main changed before effects")
    head = _git(repo, "for-each-ref", "--format=%(objectname)", ref)
    local = _git(repo, "rev-parse", "HEAD")
    remote = _git(broker.source, "ls-remote", "origin", "refs/heads/" + broker.spec["branch"])
    found = broker._existing_pr(validate_metadata=False)
    if (
        local not in {plan["old_head"], head}
        or not remote
        or remote.split()[0] not in {plan["old_head"], head}
        or _git(repo, "status", "--porcelain", "--untracked-files=all")
        or _git(repo, "rev-parse", "--show-toplevel") != str(repo)
        or _git(repo, "branch", "--show-current") != broker.spec["branch"]
        or _git(repo, "remote", "get-url", "--push", "origin") != broker.spec["origin_url"]
        or _git(broker.source, "remote", "get-url", "origin") != broker.spec["origin_url"]
        or _identity(broker)[0] != plan["signer"]
        or found is None
        or found.get("state") != "OPEN"
        or found.get("isDraft") is not False
        or found["number"] != grant["state"]["pull_request"]["number"]
        or found["headRefOid"] != remote.split()[0]
    ):
        raise ValueError("integration source, owned remote or publication changed before effects")
    if not head:
        for raw in observed["blobs"].values():
            actual = (
                subprocess.check_output(
                    ["git", "-C", str(repo), "hash-object", "-w", "--stdin"], input=raw, timeout=30
                )
                .decode()
                .strip()
            )
            if actual != _object("blob", raw):
                raise ValueError("integration written blob changed")
        with tempfile.TemporaryDirectory(prefix="devflow-integration-index-") as directory:
            env = {**os.environ, "GIT_INDEX_FILE": str(Path(directory) / "index")}
            raw = b"".join(
                (entry["mode"] + " " + entry["oid"] + "\t" + path).encode() + b"\0"
                for path, entry in sorted(observed["index"].items())
            )
            subprocess.run(
                ["git", "-C", str(repo), "update-index", "-z", "--index-info"],
                env=env,
                input=raw,
                check=True,
                capture_output=True,
                timeout=30,
            )
            tree = (
                subprocess.check_output(["git", "-C", str(repo), "write-tree"], env=env, timeout=30)
                .decode()
                .strip()
            )
        if tree != plan["tree"] or _identity(broker)[0] != plan["signer"]:
            raise ValueError("integration source tree or signer changed before commit")
        head = _git(
            repo,
            "commit-tree",
            "-S",
            tree,
            "-p",
            plan["old_head"],
            "-p",
            plan["main"],
            "-m",
            plan["subject"] + "\n\nSigned-off-by: " + plan["signer"],
        )
        _validate_commit(repo, head, grant)
        _git(repo, "update-ref", ref, head, "0" * 40)
    _validate_commit(repo, head, grant)
    _immutable(root / "integration.json", {"head": head, "ref": ref, **plan})
    local = _git(repo, "rev-parse", "HEAD")
    if local not in {plan["old_head"], head} or _git(repo, "status", "--porcelain"):
        raise ValueError("integration checkout changed before owned fast-forward")
    if local != head:
        _git(repo, "merge", "--ff-only", head)
    remote = _git(broker.source, "ls-remote", "origin", "refs/heads/" + broker.spec["branch"])
    if not remote or remote.split()[0] not in {plan["old_head"], head}:
        raise ValueError("integration remote changed before exact owned publication")
    if remote.split()[0] != head:
        _git(repo, "push", "origin", head + ":refs/heads/" + broker.spec["branch"])
    return head


def readback(spec, recovery):
    plan = recovery.get("integration")
    if not plan:
        return
    path = Path(spec["state_dir"]) / "technical-successor/integration.json"
    receipt = read_private(path)
    if any(
        canonical_json(receipt.get(key)) != canonical_json(value) for key, value in plan.items()
    ):
        raise ValueError("integration retained applicability mapping changed")
    _validate_commit(Path(spec["checkout"]), receipt["head"], {"integration": plan})
    if (
        _git(Path(spec["checkout"]), "for-each-ref", "--format=%(objectname)", receipt["ref"])
        != receipt["head"]
        or spec["base_sha"] != plan["main"]
    ):
        raise ValueError("integration retained ref or explicit base amendment changed")
