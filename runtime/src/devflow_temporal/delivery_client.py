"""One tokenless loopback API client for CLI and MCP callers."""

from __future__ import annotations

import http.cookiejar
import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

from .delivery_config import DeliveryConfig


class ServiceUnavailable(ValueError):
    """The local transport failed; a sent mutation must not be replayed."""


class DeliveryClient:
    def __init__(self, config: DeliveryConfig) -> None:
        self.config = config
        self.cookies = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(self.cookies)
        )
        self.csrf = ""

    def _request(
        self, method: str, path: str, body: dict[str, Any] | None = None, *, timeout: float = 30
    ) -> dict:
        payload = json.dumps(body).encode() if body is not None else None
        headers = {"Origin": self.config.dashboard_url.rstrip("/")}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        if method != "GET" and path != "/api/session":
            # Refresh CSRF state before sending, including after expiry or API reload.
            # Never replay a dispatched mutation after a transport failure.
            self.login(timeout=timeout)
            headers["X-Devflow-CSRF"] = self.csrf
        request = urllib.request.Request(
            self.config.dashboard_url.rstrip("/") + path,
            data=payload,
            headers=headers,
            method=method,
        )
        try:
            with self.opener.open(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            try:
                detail = json.load(exc).get("detail", "request failed")
            except (ValueError, OSError):
                detail = "request failed"
            if (exc.code == 409 and isinstance(detail, dict) and detail.get("version") == 1
                    and isinstance(detail.get("blockers"), list)):
                from .delivery_lifecycle import RecoveryRefused

                raise RecoveryRefused(detail) from None
            raise ValueError(f"service HTTP {exc.code}: {detail}") from None
        except (urllib.error.URLError, OSError) as exc:
            raise ServiceUnavailable(
                f"local service request failed: {exc}; inspect {self.config.state_root / 'api.log'}"
            ) from None

    def login(self, *, timeout: float = 30) -> None:
        """Bootstrap anonymous CSRF state; retained name for lifecycle callers."""
        result = self._request("GET", "/api/session", timeout=timeout)
        self.csrf = result["csrf_token"]

    def create_operation(self, request: dict[str, Any]) -> dict:
        """Return the durable operation, including pending/unknown states."""
        return self._request("POST", "/api/operations", request)

    def operation(self, operation_id: str) -> dict:
        return self._request("GET", "/api/operations/" + quote(operation_id, safe=""))

    def run_operations(self, run_id: str) -> dict:
        return self._request("GET", "/api/runs/" + quote(run_id, safe="") + "/operations")

    def reconcile_operation(self, operation_id: str) -> dict:
        return self._request(
            "POST", "/api/operations/" + quote(operation_id, safe="") + "/reconcile", {}
        )

    def pause_control(self, run_id: str) -> dict:
        """Read the persisted pause scope, state and blockers without starting a service."""
        return self._request("GET", "/api/runs/" + quote(run_id, safe="") + "/pause-control")

    def _pause_operation(self, run_id: str, action: str, payload: dict[str, Any]) -> dict:
        keys = {"operation_id", "command_id", "expected_revision"}
        if action == "resume":
            keys.add("epoch")
        if not isinstance(payload, dict) or set(payload) != keys:
            raise ValueError("pause operation requires the exact operation and command fields")
        return self.create_operation({
            "operation_id": payload["operation_id"], "run_id": run_id, "kind": action,
            "request": {key: value for key, value in payload.items() if key != "operation_id"},
        })

    def pause(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._pause_operation(run_id, "pause", payload)

    def resume(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._pause_operation(run_id, "resume", payload)

    def submit(self, payload: dict[str, Any]) -> dict:
        return self._request("POST", "/api/runs", payload)

    def service(self) -> dict:
        return self._request("GET", "/api/service")

    def runs(self, *, limit: int = 50, cursor: str | None = None, archived: bool = False) -> dict:
        parameters: dict[str, str | int] = {}
        if limit != 50:
            parameters["limit"] = limit
        if archived:
            parameters["archived"] = "true"
        if cursor is not None:
            parameters["cursor"] = cursor
        suffix = "?" + urlencode(parameters) if parameters else ""
        return self._request("GET", "/api/runs" + suffix)

    def status(self, run_id: str) -> dict:
        """Read the bounded persisted projection, without reconstructing authority."""
        return self._request("GET", "/api/runs/" + quote(run_id, safe="") + "/status")

    def detail(self, run_id: str) -> dict:
        """Explicitly reconstruct the complete run detail and indexed evidence."""
        return self._request("GET", "/api/runs/" + quote(run_id, safe=""))

    def evidence(self, run_id: str, evidence_id: str) -> dict:
        return self._request(
            "GET",
            "/api/runs/" + quote(run_id, safe="") + "/evidence/" + quote(evidence_id, safe=""),
        )

    def decision(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request("POST", "/api/runs/" + quote(run_id, safe="") + "/decision", payload)

    def refresh_issue_context(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request("POST", "/api/runs/" + quote(run_id, safe="") + "/issue-context",
                             payload, timeout=600)

    def merge(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request("POST", "/api/runs/" + quote(run_id, safe="") + "/merge", payload,
                             timeout=180)

    def merge_preflight(self, run_id: str) -> dict:
        return self._request("GET", "/api/runs/" + quote(run_id, safe="") + "/merge-preflight",
                             timeout=120)

    def revise_feature_plan(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request(
            "POST", "/api/runs/" + quote(run_id, safe="") + "/revise-feature-plan", payload,
            timeout=180,
        )

    def continue_feature(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request(
            "POST", "/api/runs/" + quote(run_id, safe="") + "/continue-feature", payload,
            timeout=180,
        )

    def cancel(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request("POST", "/api/runs/" + quote(run_id, safe="") + "/cancel", payload)

    def disable_delivery_limits(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request(
            "POST", "/api/runs/" + quote(run_id, safe="") + "/disable-limits", payload,
        )

    def reconcile_tracker(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request(
            "POST", "/api/runs/" + quote(run_id, safe="") + "/reconcile-tracker", payload,
        )

    def recover_publication(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request(
            "POST",
            "/api/runs/" + quote(run_id, safe="") + "/recover-publication",
            payload,
        )


    def gates_only_preflight(self, run_id: str) -> dict:
        return self._request(
            "GET", "/api/runs/" + quote(run_id, safe="") + "/gates-only-preflight", timeout=120,
        )

    def admit_gates_only(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request(
            "POST", "/api/runs/" + quote(run_id, safe="") + "/admit-gates-only", payload,
            timeout=120,
        )

    def assess_transition(self, run_id: str, kind: str, request: dict[str, Any]) -> dict:
        return self._request(
            "POST", "/api/runs/" + quote(run_id, safe="") + "/assess-transition",
            {"kind": kind, "request": request}, timeout=180,
        )

    def repair_admission_preflight(self, run_id, request):
        return self._request("POST", "/api/runs/" + quote(run_id, safe="")
                             + "/repair-admission-preflight", request, timeout=120)

    def ci_inventory_preflight(self, run_id, request):
        return self._request('POST', '/api/runs/' + quote(run_id, safe='')
                             + '/ci-inventory-preflight', request, timeout=180)

    def ack_ci_inventory(self, run_id, request):
        return self._request('POST', '/api/runs/' + quote(run_id, safe='')
                             + '/ack-ci-inventory', request, timeout=180)

    def ci_inventory(self, run_id):
        return self._request('GET', '/api/runs/' + quote(run_id, safe='') + '/ci-inventory')

    def native_diagnostic_preflight(self, run_id, request):
        return self._request('POST', '/api/runs/' + quote(run_id, safe='')
                             + '/native-diagnostic-preflight', request, timeout=120)

    def ack_native_diagnostic(self, run_id, request):
        return self._request('POST', '/api/runs/' + quote(run_id, safe='')
                             + '/ack-native-diagnostic', request, timeout=120)

    def native_diagnostic(self, run_id, command_id):
        return self._request('GET', '/api/runs/' + quote(run_id, safe='')
                             + '/native-diagnostics/' + quote(command_id, safe=''))

    def continue_repair(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request(
            "POST",
            "/api/runs/" + quote(run_id, safe="") + "/continue-repair",
            payload, timeout=180,
        )

    def retry_prelaunch(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request(
            "POST",
            "/api/runs/" + quote(run_id, safe="") + "/retry-prelaunch",
            payload,
        )

    def amend_scope(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request(
            "POST",
            "/api/runs/" + quote(run_id, safe="") + "/amend-scope",
            payload,
        )


def read_only_client(config_path: Path) -> DeliveryClient:
    """Connect to an existing service without starting it or creating local state."""
    return DeliveryClient(DeliveryConfig.load(config_path))


def client(config_path: Path) -> DeliveryClient:
    # The lifecycle controller also uses DeliveryClient for loopback readiness.
    from .delivery_control import ensure_service_running

    caller = read_only_client(config_path)
    ensure_service_running(caller.config)
    caller.login()
    return caller
