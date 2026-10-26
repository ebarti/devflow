"""Owned protocol/evidence bounds; no remote CI execution or success assertion."""

from __future__ import annotations

import json
import os
import sys
import time

import pytest

from devflow_temporal import delivery_ci_inventory_io as io


def test_exact_hash_publication_and_crash_prefix_replay(tmp_path, monkeypatch):
    root, raw = tmp_path / "observed", b"complete immutable source bytes"
    link = io.os.link
    monkeypatch.setattr(
        io.os,
        "link",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("owned crash after staging fsync")),
    )
    with pytest.raises(RuntimeError, match="owned crash"):
        io.publish(root, raw, limit=1024)
    stage = root / (".stage-" + io.sha256(raw))
    assert stage.read_bytes() == raw
    monkeypatch.setattr(io.os, "link", link)
    result = io.publish(root, raw, limit=1024)
    assert not stage.exists() and len(list(root.iterdir())) == 1
    assert io.publish(root, raw, limit=1024) == result
    with io.owned_bytes(result["path"], 1024, expected=result["sha256"]) as observed:
        assert observed == raw


@pytest.mark.parametrize("change", ["symlink", "hardlink", "fifo", "mode", "oversize"])
def test_private_raw_evidence_never_follows_or_accepts_changed_objects(tmp_path, change):
    path = tmp_path / "observed"
    path.write_bytes(b"source")
    path.chmod(0o600)
    if change == "symlink":
        real = path.with_name("real")
        path.rename(real)
        path.symlink_to(real)
    elif change == "hardlink":
        os.link(path, path.with_name("alias"))
    elif change == "fifo":
        path.unlink()
        os.mkfifo(path, 0o600)
    elif change == "mode":
        path.chmod(0o644)
    began = time.monotonic()
    with pytest.raises((ValueError, OSError)):
        with io.owned_bytes(path, 2 if change == "oversize" else 1024):
            pytest.fail("invalid evidence reached decode")
    assert time.monotonic() - began < 1


@pytest.mark.parametrize("change", ["bytes", "name", "link", "mode"])
def test_raw_generation_is_held_through_decode(tmp_path, change):
    path = tmp_path / "source"
    path.write_bytes(b"original")
    path.chmod(0o600)
    with pytest.raises(ValueError, match="generation changed"):
        with io.owned_bytes(path, 1024, expected=io.sha256(b"original")) as raw:
            assert raw == b"original"
            if change == "bytes":
                before = path.stat()
                path.write_bytes(b"modified")
                os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
            elif change == "name":
                path.rename(path.with_name("previous"))
                path.write_bytes(b"original")
                path.chmod(0o600)
            elif change == "link":
                os.link(path, path.with_name("alias"))
            else:
                path.chmod(0o644)


def test_replaced_directory_cannot_redirect_publication_writes(tmp_path, monkeypatch):
    root, displaced = tmp_path / "observed", tmp_path / "displaced"
    root.mkdir(mode=0o700)
    raw = b"new source"
    protected = tmp_path / "owned-original"
    protected.write_bytes(b"untouched")
    open_file, changed = io.os.open, []

    def opened(path, flags, *args, **kwargs):
        descriptor = open_file(path, flags, *args, **kwargs)
        if path == root and flags & os.O_DIRECTORY and not changed:
            changed.append(True)
            root.rename(displaced)
            root.mkdir(mode=0o700)
            (root / (".stage-" + io.sha256(raw))).symlink_to(protected)
        return descriptor

    monkeypatch.setattr(io.os, "open", opened)
    with pytest.raises(ValueError, match="directory was replaced"):
        io.publish(root, raw, limit=1024)
    assert protected.read_bytes() == b"untouched"
    assert (displaced / io.sha256(raw)).read_bytes() == raw
    assert not (root / io.sha256(raw)).exists()


@pytest.mark.parametrize("change", ["member", "prefix", "alias", "total", "count"])
def test_partial_namespace_and_limits_refuse_before_another_publication(
    tmp_path,
    monkeypatch,
    change,
):
    root, raw = tmp_path / "observed", b"bounded"
    root.mkdir(mode=0o700)
    if change == "member":
        (root / "unknown").mkdir()
    elif change == "prefix":
        stage = root / (".stage-" + io.sha256(raw))
        stage.write_bytes(b"foreign")
        stage.chmod(0o600)
    else:
        previous = io.publish(root, b"old", limit=1024)
        if change == "alias":
            os.link(previous["path"], root / ("a" * 64))
        elif change == "total":
            monkeypatch.setattr(io, "MAX_TOTAL", 8)
        else:
            monkeypatch.setattr(io, "MAX_FILES", 1)
    before = sorted(p.name for p in root.iterdir())
    with pytest.raises(ValueError):
        io.publish(root, raw, limit=1024)
    assert sorted(p.name for p in root.iterdir()) == before


@pytest.mark.parametrize("change", [None, "truncated", "count", "duplicate", "overbound"])
def test_complete_bounded_pages_are_joined_without_truncation(change):
    calls = []

    def run(command, _limit, _remaining):
        endpoint = next(x for x in command if x.startswith("repos/"))
        calls.append(endpoint)
        page = 1 if "page=1&" in endpoint else 2
        ids = list(range(1, 101)) if page == 1 else [101]
        total = 101
        if change == "truncated":
            ids = ids[:3]
        elif change == "count" and page == 2:
            total = 102
        elif change == "duplicate" and page == 2:
            ids = [1]
        elif change == "overbound":
            total = 1001
        body = json.dumps({"total_count": total, "jobs": [{"id": i} for i in ids]}).encode()
        return b"HTTP/2.0 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body

    reader = io.GitHubRead(repositories={"example/fixture"}, runner=run)
    if change:
        with pytest.raises(ValueError):
            reader.pages("repos/example/fixture/actions/runs/1/jobs", "jobs", head="a" * 40)
    else:
        assert (
            len(reader.pages("repos/example/fixture/actions/runs/1/jobs", "jobs", head="a" * 40))
            == 101
        )
        assert len(calls) == 2 and all("head_sha=" + "a" * 40 in p for p in calls)


@pytest.mark.parametrize("mode", ["bytes", "deadline"])
def test_real_owned_transport_child_is_bounded_and_reaped(mode):
    code = "import os;os.write(1,b'x'*100000)" if mode == "bytes" else "import time;time.sleep(2)"
    reader = io.GitHubRead(repositories={"example/fixture"}, timeout=1 if mode == "bytes" else 0.05)
    began = time.monotonic()
    with pytest.raises(ValueError, match="bound|deadline"):
        reader._bytes([sys.executable, "-I", "-c", code], 1024)
    assert time.monotonic() - began < 1.5


def test_foreign_repository_is_refused_before_transport():
    calls = []
    reader = io.GitHubRead(
        repositories={"example/fixture"}, runner=lambda *args: calls.append(args)
    )
    with pytest.raises(ValueError, match="endpoint is invalid"):
        reader.get("repos/foreign/fixture/actions/runs/1")
    assert calls == []


@pytest.mark.parametrize(
    "change",
    [
        "status",
        "length",
        "encoding",
        "duplicate_length",
        "header_missing",
        "header_control",
        "body_bound",
        "duplicate_json",
    ],
)
def test_complete_http_custody_is_required_before_json_or_raw_decode(change):
    body = b'{"id":1}'
    header = b"HTTP/2.0 200 OK\r\nContent-Length: 8"
    if change == "status":
        header = b"HTTP/2.0 302 Found"
    elif change == "length":
        header = b"HTTP/2.0 200 OK\r\nContent-Length: 9"
    elif change == "encoding":
        header += b"\r\nContent-Encoding: gzip"
    elif change == "duplicate_length":
        header += b"\r\nContent-Length: 8"
    elif change == "header_control":
        header += b"\r\nX-Untrusted: \x1b[31m"
    elif change == "duplicate_json":
        body = b'{"id":1,"id":2}'
        header = b"HTTP/2.0 200 OK\r\nContent-Length: " + str(len(body)).encode()
    content = header + b"\r\n\r\n" + body if change != "header_missing" else body
    reader = io.GitHubRead(repositories={"example/fixture"}, runner=lambda *_a: content)
    with pytest.raises(ValueError):
        reader.get("repos/example/fixture", limit=2 if change == "body_bound" else 1024)


def test_escape_sequence_flag_is_private_raw_transport_only_and_display_is_escaped():
    raw = b"\x1b[31mowned raw evidence\x1b[0m\n"
    calls = []

    def transport(command, *_):
        calls.append(command)
        return b"HTTP/2.0 200 OK\r\n\r\n" + raw

    reader = io.GitHubRead(repositories={"example/fixture"}, runner=transport)
    assert reader.get("repos/example/fixture/actions/jobs/1/logs", raw=True) == raw
    assert "--allow-escape-sequences" in calls[0]
    assert "\x1b" not in io.escaped_text(raw)
    assert "\\u001b" in io.escaped_text(raw)
    assert reader.observations[0]["raw"] == raw


@pytest.mark.parametrize("raw", [b'{"id":NaN}', b'{"id":Infinity}', b"[" * 1001 + b"]" * 1001])
def test_non_json_numbers_or_excessive_nested_metadata_never_grants_observation(raw):
    reader = io.GitHubRead(
        repositories={"example/fixture"}, runner=lambda *_a: b"HTTP/2.0 200 OK\r\n\r\n" + raw
    )
    with pytest.raises(ValueError):
        reader.get("repos/example/fixture")


def test_remote_graphql_cursor_is_an_opaque_string_never_gh_local_file_input(tmp_path):
    secret = tmp_path / "owned-canary"
    secret.write_text("must never enter a GraphQL parameter")
    cursor = "@" + str(secret)
    commands = []

    def transport(command, *_):
        commands.append(command)
        more = len(commands) == 1
        if not more:
            index = command.index("cursor=" + cursor)
            assert command[index - 1] == "-f"  # -F would read the local file.
        for field in ("owner=example", "name=fixture"):
            assert command[command.index(field) - 1] == "-f"
        value = {
            "data": {
                "repository": {
                    "pullRequest": {
                        "number": 10,
                        "state": "OPEN",
                        "isDraft": False,
                        "headRefOid": "a" * 40,
                        "headRefName": "owned",
                        "commits": {
                            "nodes": [
                                {
                                    "commit": {
                                        "oid": "a" * 40,
                                        "statusCheckRollup": {
                                            "contexts": {
                                                "nodes": [
                                                    {
                                                        "__typename": "CheckRun",
                                                        "id": "owned-" + str(len(commands)),
                                                        "name": "neutral",
                                                        "status": "COMPLETED",
                                                        "conclusion": "SUCCESS",
                                                    }
                                                ],
                                                "pageInfo": {
                                                    "hasNextPage": more,
                                                    "endCursor": cursor if more else None,
                                                },
                                            }
                                        },
                                    }
                                }
                            ]
                        },
                    }
                }
            }
        }
        raw = json.dumps(value).encode()
        return b"HTTP/2.0 200 OK\r\n\r\n" + raw

    reader = io.GitHubRead(repositories={"example/fixture"}, runner=transport)
    result = reader.rollup("example/fixture", 10, "a" * 40)
    assert result["complete"] is True and len(result["contexts"]) == 2
    assert secret.read_text() == "must never enter a GraphQL parameter"
