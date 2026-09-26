"""Deterministic managed delivery protocol; all effects are activities."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError


@workflow.defn(name="DevflowDeliveryWorkflow")
class DeliveryWorkflow:
    def __init__(self) -> None:
        self.state: dict[str, Any] = {}
        self.cancel_requested = False
        self.decision_answer: str | None = None

    async def _activity(self, name: str, request: dict[str, Any], *, hours: int = 2) -> Any:
        return await workflow.execute_activity(
            name,
            request,
            start_to_close_timeout=timedelta(hours=hours),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

    async def _project(self, spec: dict[str, Any], event: str, message: str) -> None:
        await self._activity(
            "delivery_project",
            {
                "spec": spec,
                "phase": self.state["phase"],
                "execution_state": self.state["execution_state"],
                "event_type": event,
                "message": message,
                "candidate": self.state.get("candidate"),
                "pull_request": self.state.get("pull_request"),
                "checks": self.state.get("checks"),
                "tracker": self.state.get("tracker"),
                "usage": self.state.get("usage"),
                "decision": self.state.get("decision"),
                "iteration": self.state["iteration"],
                "protocol_revision": self.state["revision"],
                "outcome": self.state.get("outcome"),
                "cleanup": self.state.get("cleanup"),
                "error": self.state.get("error"),
                "key": f"{event}:{self.state['iteration']}:{self.state['revision']}",
            },
        )

    async def _stop(self, spec: dict[str, Any], reason: str) -> dict[str, Any]:
        if any(
            role.get("cleanup") == "unknown" or role.get("finish_reason") == "recovery_unknown"
            for role in self.state["roles"]
        ):
            self.state["cleanup"] = "unknown"
        if self.cancel_requested:
            self.state["cleanup"] = "unknown"
            return await self._cancelled(spec)
        self.state["phase"] = "blocked"
        self.state["execution_state"] = "blocked"
        self.state["outcome"] = "blocked"
        self.state["error"] = reason
        self.state["revision"] += 1
        await self._project(spec, "blocked", reason)
        return self.state

    async def _cancelled(self, spec: dict[str, Any]) -> dict[str, Any]:
        uncertain = self.state.get("cleanup") == "unknown" or any(
            role.get("cleanup") == "unknown" or role.get("finish_reason") == "recovery_unknown"
            for role in self.state["roles"]
        )
        self.state["phase"] = "cancelled"
        self.state["execution_state"] = "terminal"
        self.state["outcome"] = "cancelled"
        self.state["cleanup"] = "unknown" if uncertain else "confirmed_after_role_boundary"
        self.state["revision"] += 1
        await self._project(
            spec,
            "cancelled",
            "Cancellation ended with unknown cleanup"
            if uncertain
            else "Cancellation reached a role boundary",
        )
        return self.state

    @workflow.run
    async def run(self, spec: dict[str, Any]) -> dict[str, Any]:
        self.state = {
            "run_id": spec["run_id"],
            "phase": "preparing",
            "execution_state": "running",
            "outcome": None,
            "revision": 1,
            "iteration": 0,
            "candidate": None,
            "pull_request": None,
            "roles": [],
            "checks": {},
            "tracker": {},
            "usage": {},
            "findings": [],
            "decision": None,
            "candidate_revision": 0,
            "cleanup": "none",
            "error": None,
        }
        try:
            await self._project(spec, "preparing", "Preparing owned Git checkout")
            prepared = await self._activity("delivery_prepare", {"spec": spec})
        except Exception as exc:
            return await self._stop(spec, f"preparation failed: {type(exc).__name__}")
        self.state["candidate"] = prepared["candidate"]
        self.state["candidate_revision"] += 1
        self.state["phase"] = "tracker_start"
        self.state["revision"] += 1
        await self._project(spec, "tracker_start", "Claimed issue entering In progress")
        try:
            started_tracker = await self._activity("delivery_tracker_start", {"spec": spec})
        except Exception as exc:
            return await self._stop(
                spec, f"initial tracker synchronization pending: {type(exc).__name__}"
            )
        self.state["tracker"] = started_tracker
        if started_tracker.get("state") != "consistent":
            return await self._stop(spec, "initial tracker readback remains pending")
        prompt = spec["policy"].get("initial_decision_prompt")
        if prompt:
            self.state["phase"] = "waiting_decision"
            self.state["execution_state"] = "waiting"
            self.state["revision"] += 1
            self.state["decision"] = {
                "id": f"{spec['run_id']}:initial",
                "revision": 1,
                "candidate_revision": self.state["candidate_revision"],
                "prompt": prompt,
                "options": ["proceed", "cancel"],
                "state": "pending",
            }
            await self._project(spec, "decision_pending", "Waiting for an explicit run decision")
            await workflow.wait_condition(
                lambda: self.decision_answer is not None or self.cancel_requested
            )
            if self.cancel_requested or self.decision_answer == "cancel":
                return await self._cancelled(spec)
            self.state["execution_state"] = "running"
            self.state["revision"] += 1
            await self._project(spec, "decision_accepted", "Run decision accepted")
        prior_implementer_session = None
        max_repairs = spec["policy"]["max_repairs"]
        repair_findings: list[str] = []
        for iteration in range(max_repairs + 1):
            self.state["iteration"] = iteration
            self.state["checks"] = {}
            if self.cancel_requested:
                return await self._cancelled(spec)
            self.state["phase"] = "implement" if iteration == 0 else "repair"
            self.state["revision"] += 1
            await self._project(spec, "role_started", self.state["phase"] + " role started")
            try:
                implementation = await self._activity(
                    "delivery_role",
                    {
                        "spec": spec,
                        "role": "implement",
                        "iteration": iteration,
                        "candidate": self.state["candidate"],
                        "findings": repair_findings,
                        "resume_session": prior_implementer_session,
                    },
                )
            except Exception as exc:
                return await self._stop(spec, f"implementer activity failed: {type(exc).__name__}")
            self.state["roles"].append(implementation)
            self.state["usage"][f"implement:{iteration}"] = implementation.get("usage")
            if self.cancel_requested:
                return await self._cancelled(spec)
            if implementation.get("status") != "pass":
                return await self._stop(spec, "implementer did not establish a pass")
            if iteration and implementation.get("session_id") != prior_implementer_session:
                return await self._stop(spec, "repair did not resume the original implementer")
            prior_implementer_session = implementation.get("session_id")
            if not prior_implementer_session and spec["provider"] == "codex":
                return await self._stop(spec, "implementer session identity is missing")
            self.state["candidate"] = implementation["candidate"]
            self.state["candidate_revision"] += 1
            self.state["phase"] = "prepublish_checks"
            self.state["revision"] += 1
            await self._project(spec, "prepublish_checks", "Checking candidate before the first PR")
            try:
                prechecked = await self._activity(
                    "delivery_precheck",
                    {"spec": spec, "iteration": iteration, "candidate": self.state["candidate"]},
                )
            except Exception as exc:
                return await self._stop(spec, f"prepublication checks failed: {type(exc).__name__}")
            self.state["checks"]["prepublish"] = prechecked
            if self.cancel_requested:
                return await self._cancelled(spec)
            if prechecked.get("state") != "passed":
                repair_findings = ["required prepublication checks did not pass"]
                self.state["findings"].extend(repair_findings)
                self.state["revision"] += 1
                await self._project(spec, "findings", "Prepublication candidate needs repair")
                if iteration >= max_repairs:
                    return await self._stop(spec, "prepublication repair limit exhausted")
                continue
            self.state["phase"] = "publishing"
            self.state["revision"] += 1
            await self._project(spec, "candidate_ready", "Candidate ready for publication")
            try:
                published = await self._activity(
                    "delivery_publish",
                    {"spec": spec, "iteration": iteration, "candidate": self.state["candidate"]},
                )
            except Exception as exc:
                return await self._stop(spec, f"publication unresolved: {type(exc).__name__}")
            if published["candidate"]["id"] != self.state["candidate"]["id"]:
                self.state["candidate_revision"] += 1
            self.state["candidate"] = published["candidate"]
            self.state["pull_request"] = published
            if self.cancel_requested:
                return await self._cancelled(spec)
            self.state["revision"] += 1
            await self._project(
                spec, "published", "Regular pull request read back at candidate head"
            )
            repair_findings = []
            qa_evidence = None
            for role in ("review", "verify"):
                if self.cancel_requested:
                    return await self._cancelled(spec)
                if role == "verify":
                    # The broker installs/builds the disposable gate checkout
                    # before browser QA and before the independent verifier
                    # inspects either source or receipts.
                    self.state["phase"] = "checks"
                    self.state["revision"] += 1
                    await self._project(spec, "checks_started", "Executing required local checks")
                    try:
                        checked = await self._activity(
                            "delivery_checks",
                            {
                                "spec": spec,
                                "iteration": iteration,
                                "candidate": self.state["candidate"],
                            },
                        )
                    except Exception as exc:
                        return await self._stop(
                            spec, f"checks activity failed: {type(exc).__name__}"
                        )
                    self.state["checks"]["local"] = checked
                    if self.cancel_requested:
                        return await self._cancelled(spec)
                    if checked.get("state") != "passed":
                        repair_findings.append("required local checks did not pass")
                        break
                if role == "verify" and spec["policy"].get("browser_qa"):
                    self.state["phase"] = "browser_qa"
                    self.state["revision"] += 1
                    await self._project(
                        spec, "browser_qa_started", "Running owned browser and API fixture"
                    )
                    try:
                        browser_qa = await self._activity(
                            "delivery_browser_qa",
                            {
                                "spec": spec,
                                "iteration": iteration,
                                "candidate": self.state["candidate"],
                            },
                        )
                    except Exception as exc:
                        return await self._stop(
                            spec, f"browser QA activity failed: {type(exc).__name__}"
                        )
                    self.state["checks"]["browser_qa"] = browser_qa
                    if self.cancel_requested:
                        return await self._cancelled(spec)
                    if browser_qa.get("cleanup") == "unknown":
                        self.state["cleanup"] = "unknown"
                        return await self._stop(spec, "browser QA child cleanup is unknown")
                    if browser_qa.get("state") != "passed":
                        repair_findings.append("owned browser/API QA did not pass")
                        break
                    qa_evidence = {
                        "path": browser_qa["receipt"],
                        "sha256": browser_qa["receipt_sha256"],
                        "log": browser_qa["log"],
                        "log_sha256": browser_qa["log_sha256"],
                        "candidate_id": self.state["candidate"]["id"],
                        "iteration": iteration,
                    }
                    self.state["revision"] += 1
                    await self._project(
                        spec, "browser_qa_passed", "Owned browser/API QA receipt is ready"
                    )
                self.state["phase"] = role
                self.state["revision"] += 1
                await self._project(spec, "role_started", role + " role started")
                try:
                    result = await self._activity(
                        "delivery_role",
                        {
                            "spec": spec,
                            "role": role,
                            "iteration": iteration,
                            "candidate": self.state["candidate"],
                            "findings": [],
                            "resume_session": None,
                            "qa_evidence": qa_evidence if role == "verify" else None,
                        },
                    )
                except Exception as exc:
                    return await self._stop(spec, f"{role} activity failed: {type(exc).__name__}")
                self.state["roles"].append(result)
                self.state["usage"][f"{role}:{iteration}"] = result.get("usage")
                if self.cancel_requested:
                    return await self._cancelled(spec)
                if result.get("candidate", {}).get("id") != self.state["candidate"]["id"]:
                    return await self._stop(spec, f"{role} assessed a stale candidate")
                if (
                    result.get("session_id") == prior_implementer_session
                    and spec["provider"] == "codex"
                ):
                    return await self._stop(spec, f"{role} reused the implementer session")
                self.state["checks"]["review" if role == "review" else "qa"] = {
                    "state": "passed" if result.get("status") == "pass" else "failed",
                    "detail": result.get("summary"),
                    "candidate_id": self.state["candidate"]["id"],
                }
                if result.get("status") != "pass":
                    repair_findings.extend(result.get("findings") or [f"{role} did not pass"])
                    break
            if repair_findings:
                self.state["findings"].extend(repair_findings)
                self.state["revision"] += 1
                await self._project(spec, "findings", "Candidate requires bounded repair")
                if iteration >= max_repairs:
                    return await self._stop(spec, "repair limit exhausted")
                continue
            self.state["phase"] = "waiting_ci"
            self.state["revision"] += 1
            await self._project(spec, "ci_wait", "Waiting for required CI without model calls")
            try:
                ci = await self._activity(
                    "delivery_ci", {"spec": spec, "pull_request": published}, hours=1
                )
            except Exception as exc:
                return await self._stop(spec, f"CI observation failed: {type(exc).__name__}")
            self.state["checks"]["ci"] = ci
            if self.cancel_requested:
                return await self._cancelled(spec)
            if ci.get("state") != "passed":
                return await self._stop(spec, "required CI did not confirm this PR head")
            self.state["phase"] = "tracker"
            self.state["revision"] += 1
            await self._project(spec, "tracker_started", "Reconciling issue and claim")
            try:
                tracker = await self._activity("delivery_tracker", {"spec": spec, "pr": published})
            except Exception as exc:
                return await self._stop(
                    spec, f"tracker synchronization pending: {type(exc).__name__}"
                )
            self.state["tracker"] = tracker
            if self.cancel_requested:
                return await self._cancelled(spec)
            if tracker.get("state") != "consistent":
                return await self._stop(spec, "tracker readback remains pending or conflicting")
            self.state["phase"] = "delivered"
            self.state["execution_state"] = "terminal"
            self.state["outcome"] = "delivered"
            self.state["revision"] += 1
            await self._project(spec, "delivered", "Published, independently gated run delivered")
            return self.state
        return await self._stop(spec, "repair loop ended without delivery")

    @workflow.query(name="status")
    def status(self) -> dict[str, Any]:
        return self.state

    @workflow.update(name="cancel")
    async def cancel(self, request: dict[str, Any]) -> dict[str, Any]:
        await workflow.wait_condition(lambda: bool(self.state))
        if self.state.get("outcome") is not None:
            raise ApplicationError("run is already terminal", non_retryable=True)
        if request.get("expected_revision") != self.state["revision"]:
            raise ApplicationError("stale run revision", non_retryable=True)
        if not isinstance(request.get("reason"), str) or not request["reason"].strip():
            raise ApplicationError("cancellation reason is required", non_retryable=True)
        self.cancel_requested = True
        self.state["phase"] = "cancelling"
        self.state["execution_state"] = "cancelling"
        self.state["cleanup"] = "pending_role_completion"
        self.state["revision"] += 1
        return self.state

    @workflow.update(name="decision")
    async def decision(self, request: dict[str, Any]) -> dict[str, Any]:
        await workflow.wait_condition(lambda: bool(self.state))
        pending = self.state.get("decision")
        if not pending:
            raise ApplicationError("no decision is pending", non_retryable=True)
        if request.get("expected_revision") != self.state["revision"]:
            raise ApplicationError("stale run revision", non_retryable=True)
        if (
            request.get("decision_id") != pending["id"]
            or request.get("decision_revision") != pending["revision"]
        ):
            raise ApplicationError("stale decision", non_retryable=True)
        if request.get("candidate_revision") != pending["candidate_revision"]:
            raise ApplicationError("decision candidate changed", non_retryable=True)
        if request.get("answer") not in pending["options"]:
            raise ApplicationError("answer is outside the decision options", non_retryable=True)
        self.decision_answer = request["answer"]
        self.state["decision"] = None
        self.state["revision"] += 1
        return self.state
