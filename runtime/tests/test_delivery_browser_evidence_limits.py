"""Complete PDF evidence and failed collection retain native cleanup facts."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from test_delivery_check_slots_browser import browser_checks as browser_checks
from test_delivery_store import _git

from devflow_temporal import delivery_activities as activities
from devflow_temporal import delivery_browser_qa as browser
from devflow_temporal.delivery_resources import RunResources


def test_complete_pdf_evidence_is_not_limited_to_128_small_files(tmp_path):
    report = tmp_path / "dist/playwright-report"
    report.mkdir(parents=True)
    for index in range(190):
        (report / f"page-{index}.txt").write_text(f"owned page evidence {index}")
    artifacts = browser._artifacts(tmp_path, ["dist/playwright-report"])
    assert len(artifacts) == 190
    assert all(Path(item["path"]).is_file() and item["sha256"] for item in artifacts)


def test_browser_evidence_has_a_total_byte_bound(tmp_path, monkeypatch):
    report = tmp_path / "report"
    report.mkdir()
    (report / "one").write_bytes(b"abc")
    (report / "two").write_bytes(b"def")
    monkeypatch.setattr(browser, "MAX_BROWSER_ARTIFACT_BYTES", 5, raising=False)
    with pytest.raises(ValueError, match="evidence bound"):
        browser._artifacts(tmp_path, ["report"])


def test_browser_evidence_still_rejects_symlinks(tmp_path):
    checkout = tmp_path / "checkout"
    report = checkout / "report"
    report.mkdir(parents=True)
    outside = tmp_path / "private-data"
    outside.write_bytes(b"preserve")
    (report / "link").symlink_to(outside)
    with pytest.raises(ValueError, match="symlink"):
        browser._artifacts(checkout, ["report"])
    assert outside.read_bytes() == b"preserve"


def _request_with_evidence(fixture, name, count, *, failed=False):
    request = fixture.request(name, owned_checkout=True)
    broker = fixture.brokers[name]
    checkout = Path(request["spec"]["checkout"])
    (checkout / ".gitignore").write_text("dist/\n")
    _git(checkout, "add", ".gitignore")
    _git(checkout, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
         "commit", "-qm", "ignore owned browser output")
    qa = request["spec"]["policy"]["browser_qa"]
    qa["artifact_paths"] = ["dist/playwright-report"]
    qa["argv"][-1] += (
        "\nreport=pathlib.Path('dist/playwright-report')\n"
        f"for index in range({count}): (report / f'page-{{index}}.txt').write_text('owned')\n"
        + ("print('1 failed'); sys.exit(1)\n" if failed else "")
    )
    request["candidate"] = broker.candidate()
    return request, broker


@pytest.mark.skipif(sys.platform != "darwin", reason="native process and port observation")
@pytest.mark.asyncio
@pytest.mark.parametrize("failed", [False, True])
async def test_native_browser_retains_190_files_and_original_verdict(
    browser_checks, monkeypatch, failed,
):
    request, broker = _request_with_evidence(browser_checks, "many-files", 190, failed=failed)
    try:
        result = await activities.delivery_browser_qa(request)
        assert result["state"] == ("failed" if failed else "passed"), result
        assert result["cleanup"] == "confirmed"
        assert result["native_process"]["cleanup"] == "observed-native-confirmed"
        assert result["exit_code"] == int(failed)
        assert len(result["artifacts"]) == 190
        assert all(Path(item["path"]).is_file() for item in result["artifacts"])
        monkeypatch.setattr(broker, "_run_native_check", lambda *_: pytest.fail("replayed child"))
        assert await activities.delivery_browser_qa(request) == result
    finally:
        assert RunResources(request["spec"]).finalize("blocked")["state"] == "confirmed"


@pytest.mark.skipif(sys.platform != "darwin", reason="native process and port observation")
@pytest.mark.asyncio
async def test_collection_failure_is_a_failed_receipt_with_confirmed_cleanup(
    browser_checks, monkeypatch,
):
    # This is over the old 128-file cap and the deliberately reduced new cap.
    request, broker = _request_with_evidence(browser_checks, "over-bound", 129)
    monkeypatch.setattr(browser, "MAX_BROWSER_ARTIFACTS", 2, raising=False)
    try:
        result = await activities.delivery_browser_qa(request)
        assert result["state"] == "failed", result
        assert result["cleanup"] == "confirmed"
        assert result["native_process"]["cleanup"] == "observed-native-confirmed"
        assert result["exit_code"] == 0
        assert "evidence bound" in result["evidence_failure"]
        assert "evidence bound" in result["diagnostic"]
        monkeypatch.setattr(broker, "_run_native_check", lambda *_: pytest.fail("replayed child"))
        assert await activities.delivery_browser_qa(request) == result
    finally:
        assert RunResources(request["spec"]).finalize("blocked")["state"] == "confirmed"


@pytest.mark.skipif(sys.platform != "darwin", reason="native process and port observation")
@pytest.mark.asyncio
async def test_interrupted_postprocessing_replays_completed_browser_without_launch(
    browser_checks, monkeypatch,
):
    from devflow_temporal.delivery_native_process import NativeProcess

    request, broker = _request_with_evidence(browser_checks, "historical-postprocess", 190,
                                            failed=True)
    original_write = browser._write_new

    def lose_postprocessing(*_args):
        raise ValueError("interrupted historical postprocessing")

    monkeypatch.setattr(browser, "_write_new", lose_postprocessing)
    try:
        unknown = await activities.delivery_browser_qa(request)
        assert unknown["state"] == unknown["cleanup"] == "unknown"
        journal = broker.evidence_dir / "browser-qa/0/native/native-process.json"
        from devflow_temporal.delivery_resources import read_private

        original = read_private(journal)["result"]
        monkeypatch.setattr(browser, "_write_new", original_write)
        monkeypatch.setattr(NativeProcess, "run", lambda *_: pytest.fail("new browser process"))
        result = broker.run_browser_qa(0, request["candidate"], reconcile_only=True)
        assert result["state"] == "failed" and result["cleanup"] == "confirmed"
        assert result["native_process"] == original
        assert len(result["artifacts"]) == 190
    finally:
        assert RunResources(request["spec"]).finalize("blocked")["state"] == "confirmed"


@pytest.mark.skipif(sys.platform != "darwin", reason="native process and port observation")
def test_browser_reconciliation_never_starts_an_absent_invocation(browser_checks):
    request = browser_checks.request("absent")
    broker = browser_checks.brokers["absent"]
    with pytest.raises(FileNotFoundError):
        broker.run_browser_qa(0, request["candidate"], reconcile_only=True)
    assert not (broker.evidence_dir / "browser-qa").exists()
