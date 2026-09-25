"""Bounded, explicit calendar and trusted Bitcoin Core network ports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from a_riverhog_opentimestamps_witness.proof import (
    BitcoinUnavailable,
    CalendarError,
    calendar_url,
)

_MEDIA_TYPE = "application/vnd.opentimestamps.v1"
_RPC_MAX_BYTES = 65_536


def _bounded_bytes(response: httpx.Response, maximum: int) -> bytes:
    result = bytearray()
    for chunk in response.iter_bytes():
        result.extend(chunk)
        if len(result) > maximum:
            raise ValueError("network response exceeds byte budget")
    return bytes(result)


class HttpCalendar:
    def __init__(
        self, *, timeout_seconds: float = 15.0, transport: httpx.BaseTransport | None = None
    ):
        if not 0 < timeout_seconds <= 60:
            raise ValueError("calendar timeout is outside its bound")
        self.timeout_seconds = timeout_seconds
        self.transport = transport

    def request(self, kind: str, url: str, commitment: bytes, max_bytes: int) -> bytes:
        calendar_url(url)
        if kind not in {"submit", "upgrade"} or not 1 <= len(commitment) <= 4096:
            raise ValueError("invalid calendar work")
        if max_bytes < 1 or max_bytes > 65_536:
            raise ValueError("invalid calendar response bound")
        target = f"{url}/digest" if kind == "submit" else f"{url}/timestamp/{commitment.hex()}"
        try:
            with httpx.Client(
                timeout=self.timeout_seconds,
                follow_redirects=False,
                trust_env=False,
                transport=self.transport,
            ) as client:
                with client.stream(
                    "POST" if kind == "submit" else "GET",
                    target,
                    content=commitment if kind == "submit" else None,
                    headers={"Accept": _MEDIA_TYPE, "Content-Type": "application/octet-stream"},
                ) as response:
                    if response.status_code == 404:
                        raise CalendarError("not_found", retryable=True)
                    if response.status_code in {408, 425, 429} or response.status_code >= 500:
                        raise CalendarError("unavailable", retryable=True)
                    if response.status_code != 200:
                        raise CalendarError("rejected", retryable=False)
                    try:
                        return _bounded_bytes(response, max_bytes)
                    except ValueError as exc:
                        raise CalendarError("limit", retryable=False) from exc
        except httpx.HTTPError as exc:
            raise CalendarError("unavailable", retryable=True) from exc


class BitcoinRpc:
    """Read-only JSON-RPC transport to an operator-selected validating node."""

    def __init__(
        self,
        url: str,
        cookie_file: Path,
        *,
        timeout_seconds: float = 15.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        parsed = urlsplit(url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or (
                parsed.scheme == "http" and parsed.hostname not in {"localhost", "127.0.0.1", "::1"}
            )
        ):
            raise ValueError("Bitcoin RPC requires a trusted HTTPS or local HTTP URL")
        if not 0 < timeout_seconds <= 60:
            raise ValueError("Bitcoin RPC timeout is outside its bound")
        self.url = url
        self.cookie_file = Path(cookie_file)
        self.timeout_seconds = timeout_seconds
        self.transport = transport

    def __call__(self, method: str, params: tuple[Any, ...]) -> Any:
        if method not in {"getblockchaininfo", "getblockhash", "getblockheader"}:
            raise BitcoinUnavailable("unsupported Bitcoin RPC method")
        try:
            cookie = self.cookie_file.read_text(encoding="ascii").strip()
            username, password = cookie.split(":", 1)
            if not username or not password:
                raise ValueError("empty Bitcoin RPC credential")
            body = {
                "jsonrpc": "1.0",
                "id": "riverhog-witness",
                "method": method,
                "params": list(params),
            }
            with httpx.Client(
                timeout=self.timeout_seconds,
                follow_redirects=False,
                trust_env=False,
                transport=self.transport,
                auth=httpx.BasicAuth(username, password),
            ) as client:
                with client.stream("POST", self.url, json=body) as response:
                    if response.status_code != 200:
                        raise BitcoinUnavailable("Bitcoin RPC unavailable")
                    raw = _bounded_bytes(response, _RPC_MAX_BYTES)
            parsed_body: Any = json.loads(raw)
            if (
                not isinstance(parsed_body, dict)
                or set(parsed_body) != {"result", "error", "id"}
                or parsed_body["error"] is not None
                or parsed_body["id"] != "riverhog-witness"
            ):
                raise BitcoinUnavailable("invalid Bitcoin RPC response")
            return parsed_body["result"]
        except (OSError, UnicodeError, ValueError, httpx.HTTPError) as exc:
            raise BitcoinUnavailable("Bitcoin RPC unavailable") from exc
