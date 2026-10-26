"""Parse literal runner/report formats after exact approved whole-log hashing."""

from __future__ import annotations

import re

from .contracts import digest
from .delivery_ci_inventory_io import MAX_LOG, sha256

TIMESTAMP = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+Z ?")
ANSI = re.compile(r"\x1b\[[0-9;]*m")
COUNTS = re.compile(r"(\d+) (passed|skipped|xfailed|xpassed|failed|errors?|deselected|warnings?)")
COUNT_TEXT = (
    r"\d+ (?:passed|skipped|xfailed|xpassed|failed|errors?|deselected|warnings?)"
    r"(?:\s*(?:,|\|)\s*\d+ "
    r"(?:passed|skipped|xfailed|xpassed|failed|errors?|deselected|warnings?))*"
)


def lines(raw):
    if not isinstance(raw, bytes) or len(raw) > MAX_LOG:
        raise ValueError("ordinary CI job log exceeds its complete byte bound")
    text = raw.decode("utf-8", errors="strict")
    if len(text.splitlines()) > 200000:
        raise ValueError("ordinary CI job log exceeds its complete line bound")
    # This decodes the runner's literal framing. Whole raw bytes remain pinned;
    # no transformation is used to make different log bytes match a profile.
    return [ANSI.sub("", TIMESTAMP.sub("", value)) for value in text.splitlines()]


def _pairs(value):
    result = {}
    for match in COUNTS.finditer(value):
        key = {"error": "errors", "warning": "warnings"}.get(match[2], match[2])
        if key in result:
            raise ValueError("ordinary CI report counts are ambiguous")
        result[key] = int(match[1])
    return result


def _passed(result):
    if result.get("passed", 0) < 1 or any(
        result.get(key, 0) != 0 for key in ("failed", "errors", "cancelled")
    ):
        raise ValueError("ordinary CI test report is empty or unsuccessful")
    return result


def report(kind, output):
    """Known report grammars, never meaning inferred from log prose or names."""
    if kind == "pytest":
        selected = []
        for line in output:
            value = line.strip().strip("=").strip()
            match = re.fullmatch(r"(.+?) in [0-9.]+s(?: \([^)]*\))?", value)
            if match and re.fullmatch(COUNT_TEXT, match[1]):
                selected.append(_passed(_pairs(match[1])))
        if len(selected) != 1:
            raise ValueError("ordinary CI pytest terminal report is missing or ambiguous")
        return selected[0]
    if kind == "vitest":
        selected = []
        for line in output:
            match = re.fullmatch(r"\s*Tests\s+(.+?)\s+\((\d+)\)\s*", line)
            if match:
                if not re.fullmatch(COUNT_TEXT, match[1]):
                    raise ValueError("ordinary CI Vitest report grammar is incomplete")
                counts = _passed(_pairs(match[1]))
                if sum(counts.values()) != int(match[2]):
                    raise ValueError("ordinary CI Vitest total differs from its cases")
                selected.append(counts)
        if not selected:
            raise ValueError("ordinary CI Vitest terminal report is missing")
        return selected
    if kind == "tap":
        selected, current = [], None
        for line in output:
            match = re.fullmatch(r"# (tests|pass|fail|cancelled|skipped|todo) (\d+)", line.strip())
            if not match:
                continue
            key = {"pass": "passed", "fail": "failed"}.get(match[1], match[1])
            if key == "tests":
                if current is not None:
                    selected.append(current)
                current = {}
            if current is None or key in current:
                raise ValueError("ordinary CI TAP report block is incomplete or ambiguous")
            current[key] = int(match[2])
        if current is not None:
            selected.append(current)
        for counts in selected:
            if set(counts) != {
                "tests",
                "passed",
                "failed",
                "cancelled",
                "skipped",
                "todo",
            } or counts["tests"] != sum(value for key, value in counts.items() if key != "tests"):
                raise ValueError("ordinary CI TAP case inventory is incomplete")
            _passed(counts)
        if not selected:
            raise ValueError("ordinary CI TAP terminal report is missing")
        return selected
    if kind == "jest":
        selected = []
        for line in output:
            match = re.fullmatch(r"Tests:\s+(.+?), (\d+) total\s*", line.strip())
            if match:
                if not re.fullmatch(COUNT_TEXT, match[1]):
                    raise ValueError("ordinary CI Jest report grammar is incomplete")
                counts = _passed(_pairs(match[1]))
                if sum(counts.values()) != int(match[2]):
                    raise ValueError("ordinary CI Jest total differs from its cases")
                selected.append(counts)
        if len(selected) != 1:
            raise ValueError("ordinary CI Jest terminal report is missing or ambiguous")
        return selected[0]
    if kind == "playwright":
        # The GitHub reporter emits notice blocks whose following text can be
        # on the same framed line. Record repeated identical summaries rather
        # than count an annotation as another test execution.
        summaries, counts = [], {}
        for line in output:
            value = line.strip().removeprefix("##[notice]").strip()
            match = re.fullmatch(
                r"(\d+) (passed|failed|skipped|flaky|interrupted)"
                r"(?: \([^)]*\))?",
                value,
            )
            if not match:
                continue
            key = {"flaky": "flaky", "interrupted": "cancelled"}.get(match[2], match[2])
            if key in counts:
                summaries.append(counts)
                counts = {}
            counts[key] = int(match[1])
            if key == "passed":
                summaries.append(counts)
                counts = {}
        if counts:
            summaries.append(counts)
        unique = []
        for value in summaries:
            if value not in unique:
                unique.append(value)
        if not unique or any(
            value.get("failed", 0) or value.get("cancelled", 0) for value in unique
        ):
            raise ValueError("ordinary CI Playwright terminal report is missing or unsuccessful")
        passed = [value for value in unique if value.get("passed", 0) > 0]
        if len(passed) != 1 or any(set(value) - {"passed", "skipped", "flaky"} for value in unique):
            raise ValueError("ordinary CI Playwright terminal report is ambiguous")
        return {**passed[0], "repeated_notice_blocks": len(summaries) - len(unique)}
    raise ValueError("ordinary CI report grammar is unsupported")


def execution_facts(output, plan):
    """Join literal source inputs and known runner tool formats after raw hashing."""
    enabled = [step for step in plan["steps"] if step["enabled"]]
    starts = [i for i, value in enumerate(output) if value.startswith("##[group]Run ")]
    if len(starts) != len(enabled):
        raise ValueError("ordinary CI executed step inventory differs from its frozen source")
    facts = []
    for ordinal, step in enumerate(enabled):
        source, start = step["source"], starts[ordinal]
        stop = starts[ordinal + 1] if ordinal + 1 < len(starts) else len(output)
        section = output[start:stop]
        try:
            header = section[: section.index("##[endgroup]")]
        except ValueError as exc:
            raise ValueError("ordinary CI runner step framing is incomplete") from exc
        for group, expected in (
            ("with", source.get("with") or {}),
            ("env", {**plan["environment"], **(source.get("env") or {})}),
        ):
            actual_group = []
            if expected:
                if header.count(group + ":") != 1:
                    raise ValueError("ordinary CI declared input group did not execute")
                group_start = header.index(group + ":") + 1
                for line in header[group_start:]:
                    if not line.startswith("  "):
                        break
                    actual_group.append(line)
            for key, value in expected.items():
                if type(value) not in (str, bool, int):
                    raise ValueError("ordinary CI declared runner input is not literal")
                text = str(value).lower() if type(value) is bool else str(value)
                if "\n" in text or actual_group.count("  " + key + ": " + text) != 1:
                    raise ValueError("ordinary CI declared input/environment did not execute")
        uses = source.get("uses")
        tools = []
        if uses:
            repository, revision = uses.split("@")
            downloaded = f"Download action repository '{uses}' (SHA:{revision})"
            if output.count(downloaded) != 1:
                raise ValueError("ordinary CI downloaded action does not match its pinned source")
            inputs = source.get("with") or {}
            if repository == "actions/setup-python":
                versions = [
                    m[1]
                    for line in section
                    if (
                        m := re.fullmatch(
                            r"Successfully set up CPython \(([0-9]+\.[0-9]+\.[0-9]+)\)", line
                        )
                    )
                ]
                if len(versions) != 1 or not versions[0].startswith(
                    str(inputs["python-version"]) + "."
                ):
                    raise ValueError("ordinary CI Python interpreter differs from its matrix")
                tools.append({"python": versions[0]})
            elif repository == "actions/setup-node":
                version = str(inputs["node-version"])
                if section.count("node: v" + version) != 1:
                    raise ValueError("ordinary CI Node interpreter differs from frozen source")
                tools.append({"node": version})
            elif repository == "astral-sh/setup-uv":
                version = str(inputs["version"])
                if section.count("Successfully installed uv version " + version) != 1:
                    raise ValueError("ordinary CI uv version differs from frozen source")
                tools.append({"uv": version})
        interpreters = [
            m[1]
            for line in section
            if (
                m := re.fullmatch(
                    r"Using CPython ([0-9]+\.[0-9]+\.[0-9]+) interpreter at: /[^\n]+", line
                )
            )
        ]
        facts.append(
            {
                "step_index": step["index"],
                "source_sha256": digest(source),
                "header_sha256": digest(header),
                "tools": tools,
                "locked_project_interpreters": interpreters,
            }
        )
    return facts


def executed_log(raw, job, plan, expected, *, head):
    if len(raw) != expected["log_bytes"] or sha256(raw) != expected["log_sha256"]:
        raise ValueError("ordinary CI fresh log differs from its reviewed complete text")
    output = lines(raw)
    execution = execution_facts(output, plan)
    if execution != expected["execution"]:
        raise ValueError("ordinary CI executed inputs/interpreters differ from reviewed facts")
    starts = [index for index, value in enumerate(output) if value.startswith("##[group]Run ")]
    enabled = [step for step in plan["steps"] if step["enabled"]]
    if len(starts) != len(enabled):
        raise ValueError("ordinary CI executed step inventory differs from its frozen source")
    actual_steps = job.get("steps")
    if (
        not isinstance(actual_steps, list)
        or digest(actual_steps) != expected["steps_sha256"]
        or any(
            not isinstance(s, dict)
            or type(s.get("number")) is not int
            or not isinstance(s.get("name"), str)
            for s in actual_steps
        )
        or len({s["number"] for s in actual_steps}) != len(actual_steps)
    ):
        raise ValueError("ordinary CI step metadata changed from its original completed attempt")
    source_names = {step["source"]["name"] for step in plan["steps"]}
    post_names = expected["post_steps"]
    permitted = source_names | set(post_names) | {"Set up job", "Complete job"}
    if any(s["name"] not in permitted for s in actual_steps):
        raise ValueError("ordinary CI job contains an unbound execution step")
    for name in ["Set up job", "Complete job", *post_names]:
        matched = [s for s in actual_steps if s["name"] == name]
        if (
            len(matched) != 1
            or matched[0].get("status") != "completed"
            or matched[0].get("conclusion") != "success"
        ):
            raise ValueError("ordinary CI runner or pinned post step did not finish")
    sections, observed = {}, []
    for ordinal, step in enumerate(enabled):
        source, start = step["source"], starts[ordinal]
        stop = starts[ordinal + 1] if ordinal + 1 < len(starts) else len(output)
        matches = [s for s in actual_steps if s.get("name") == source["name"]]
        if (
            len(matches) != 1
            or matches[0].get("status") != "completed"
            or matches[0].get("conclusion") != "success"
        ):
            raise ValueError("ordinary CI required source step did not complete successfully")
        script = source.get("uses", source.get("run"))
        if not isinstance(script, str) or not script.splitlines():
            raise ValueError("ordinary CI source step has no literal execution")
        first = script.splitlines()[0]
        if output[start] != "##[group]Run " + first:
            raise ValueError("ordinary CI executed action or command differs from frozen source")
        if "run" in source:
            body = script.rstrip("\n").splitlines()
            if output[start + 1 : start + 1 + len(body)] != body:
                raise ValueError("ordinary CI executed selector or matrix changed")
        sections[step["index"]] = output[start:stop]
        observed.append(
            {
                "step_index": step["index"],
                "name": source["name"],
                "source_sha256": digest(source),
                "metadata_number": matches[0]["number"],
            }
        )
    for step in plan["steps"]:
        if not step["enabled"]:
            matches = [s for s in actual_steps if s.get("name") == step["source"]["name"]]
            if matches and (len(matches) != 1 or matches[0].get("conclusion") != "skipped"):
                raise ValueError("ordinary CI excluded source step unexpectedly executed")
    checkout = [
        index
        for index, line in enumerate(output)
        if re.fullmatch(r"\[command\]/[^ ]+/git log -1 --format=%H", line)
    ]
    if (
        len(checkout) != 1
        or checkout[0] + 1 >= len(output)
        or output[checkout[0] + 1].strip() != head
    ):
        raise ValueError("ordinary CI actual checkout differs from the exact published head")
    reports = []
    for declaration in expected["reports"]:
        index, kind = declaration["step_index"], declaration["format"]
        if index not in sections:
            raise ValueError("ordinary CI required report source did not execute")
        value = {"source_exit": "success"} if kind == "exit" else report(kind, sections[index])
        if value != declaration["observed"]:
            raise ValueError("ordinary CI complete report changed from reviewed source-bound facts")
        reports.append({"step_index": index, "format": kind, "report": value})
    if not reports:
        raise ValueError("ordinary CI has no complete required execution reports")
    return {
        "checkout": head,
        "steps": observed,
        "reports": reports,
        "execution": execution,
        "log_sha256": sha256(raw),
        "log_bytes": len(raw),
        "historical_wire_sha256": None,
        "original_log_provenance": "complete retained connector UTF-8 text",
        "fresh_raw_bytes_equal_reviewed_text": True,
    }
