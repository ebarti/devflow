"""One tokenless loopback API client for CLI and MCP callers."""

from __future__ import annotations

import http.cookiejar
import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import quote

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
            raise ValueError(f"service HTTP {exc.code}: {detail}") from None
        except (urllib.error.URLError, OSError) as exc:
            raise ServiceUnavailable(
                f"local service request failed: {exc}; inspect {self.config.state_root / 'api.log'}"
            ) from None

    def login(self, *, timeout: float = 30) -> None:
        """Bootstrap anonymous CSRF state; retained name for lifecycle callers."""
        result = self._request("GET", "/api/session", timeout=timeout)
        self.csrf = result["csrf_token"]

    def submit(self, payload: dict[str, Any]) -> dict:
        return self._request("POST", "/api/runs", payload)

    def service(self) -> dict:
        return self._request("GET", "/api/service")

    def runs(self) -> dict:
        return self._request("GET", "/api/runs")

    def status(self, run_id: str) -> dict:
        return self._request("GET", "/api/runs/" + quote(run_id, safe=""))

    def evidence(self, run_id: str, evidence_id: str) -> dict:
        return self._request(
            "GET",
            "/api/runs/" + quote(run_id, safe="") + "/evidence/" + quote(evidence_id, safe=""),
        )

    def decision(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request("POST", "/api/runs/" + quote(run_id, safe="") + "/decision", payload)

    def cancel(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request("POST", "/api/runs/" + quote(run_id, safe="") + "/cancel", payload)

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

    def recovery_preflight(self, run_id: str) -> dict:
        return self._request(
            "GET", "/api/runs/" + quote(run_id, safe="") + "/recovery-preflight", timeout=120,
        )

    def recover_execution(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request(
            "POST", "/api/runs/" + quote(run_id, safe="") + "/recover-execution", payload,
            timeout=180,
        )

    def continue_repair(self, run_id: str, payload: dict[str, Any]) -> dict:
        return self._request(
            "POST",
            "/api/runs/" + quote(run_id, safe="") + "/continue-repair",
            payload,
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


def client(config_path: Path) -> DeliveryClient:
    # The lifecycle controller also uses DeliveryClient for loopback readiness.
    from .delivery_control import ensure_service_running

    caller = DeliveryClient(DeliveryConfig.load(config_path))
    ensure_service_running(caller.config)
    caller.login()
    return caller
