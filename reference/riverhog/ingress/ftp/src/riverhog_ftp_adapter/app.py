"""Service API and operator entrypoint for the maintained Riverhog FTP adapter."""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import ipaddress
import json
import logging
import secrets
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from typing import Annotated

import uvicorn
from fastapi import Depends, FastAPI, Query, Request, Security
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from http_api_contracts import (
    ErrorResponse,
    HealthResponse,
    apply_openapi_error_contract,
    error_code_for_status,
    error_payload,
    mutable_browse_operation,
)
from riverhog_client import ApiClient
from riverhog_ftp_adapter_api_client import RiverhogFtpAdapterClient
from riverhog_provenance import resolve_provenance_observer
from starlette.exceptions import HTTPException as StarletteHTTPException

from riverhog_ftp_adapter.config import FtpAdapterConfig, load_config, load_source_config
from riverhog_ftp_adapter.landing import FtpAdapter
from riverhog_ftp_adapter.listener import serve_ftp

BEARER = HTTPBearer(auto_error=False, scheme_name="RiverhogFtpAdapterBearer")
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FtpAdapterComposition:
    config: FtpAdapterConfig
    api: ApiClient
    adapter: FtpAdapter

    @classmethod
    def build(cls, config: FtpAdapterConfig) -> FtpAdapterComposition:
        observer = (
            resolve_provenance_observer(config.provenance_observer)
            if config.provenance_observer is not None
            else None
        )
        api = ApiClient(
            base_url=config.riverhog_base_url,
            token=config.riverhog_token,
            allow_insecure_http=config.allow_insecure_http,
        )
        return cls(
            config=config,
            api=api,
            adapter=FtpAdapter(
                api,
                config,
                provenance_observer_factory=observer.create if observer is not None else None,
            ),
        )


class FtpAdapterHttpError(Exception):
    def __init__(
        self,
        status: int,
        code: str,
        message: str,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.headers = dict(headers or {})


def create_app(composition: FtpAdapterComposition | None = None) -> FastAPI:
    resolved = composition or FtpAdapterComposition.build(load_config())

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        task = asyncio.create_task(_scheduler(resolved))
        try:
            yield
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            resolved.api.close()

    app = FastAPI(
        title="Riverhog FTP adapter API",
        version="1.0.0",
        description="Content-opaque FTP collection producer.",
        lifespan=lifespan,
    )
    app.state.ftp_adapter = resolved

    @app.exception_handler(FtpAdapterHttpError)
    async def ftp_adapter_http_error(_request: Request, exc: FtpAdapterHttpError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status,
            content=error_payload(code=exc.code, message=exc.message),
            headers=exc.headers,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(_request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content=error_payload(code="bad_request", message=str(exc)),
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=error_payload(
                code=error_code_for_status(exc.status_code),
                message=str(exc.detail),
            ),
            headers=exc.headers,
        )

    @app.exception_handler(Exception)
    async def unexpected_error(_request: Request, exc: Exception) -> JSONResponse:
        LOGGER.exception("unhandled Riverhog FTP adapter API error", exc_info=exc)
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content=error_payload(code="internal_error", message="internal server error"),
        )

    def management_auth(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Security(BEARER)],
    ) -> None:
        if (
            credentials is not None
            and credentials.scheme.casefold() == "bearer"
            and secrets.compare_digest(credentials.credentials, resolved.config.api_token)
        ):
            return
        raise FtpAdapterHttpError(
            401,
            "unauthorized",
            "valid FTP adapter bearer credentials are required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    @app.get(
        "/health/live",
        response_model=HealthResponse,
        operation_id="ftp_adapter_health_live",
        tags=["health"],
    )
    def health_live() -> HealthResponse:
        return HealthResponse(service="riverhog-ftp-adapter", status="ok")

    @app.get(
        "/health/ready",
        response_model=HealthResponse,
        responses={503: {"model": ErrorResponse}},
        operation_id="ftp_adapter_health_ready",
        tags=["health"],
    )
    def health_ready() -> HealthResponse:
        try:
            resolved.api.list_archive_stores(page_size=1)
        except Exception as exc:
            raise FtpAdapterHttpError(
                503,
                "service_unavailable",
                "Riverhog archive authority is not ready",
            ) from exc
        return HealthResponse(service="riverhog-ftp-adapter", status="ok")

    @app.get(
        "/v1/status",
        operation_id="get_ftp_adapter_status",
        dependencies=[Depends(management_auth)],
        tags=["service"],
        openapi_extra=mutable_browse_operation(),
    )
    def status(
        page_size: Annotated[int, Query(ge=1, le=100)] = 25,
        page_token: Annotated[str | None, Query(min_length=1, max_length=120)] = None,
    ) -> dict[str, object]:
        return resolved.adapter.status(page_size=page_size, page_token=page_token)

    @app.post(
        "/v1/run",
        operation_id="run_ftp_adapter_pass",
        dependencies=[Depends(management_auth)],
        tags=["operations"],
    )
    def run_pass() -> dict[str, object]:
        return resolved.adapter.run_once()

    @app.post(
        "/v1/sources/{source_id}/flush",
        operation_id="flush_ftp_adapter_source",
        dependencies=[Depends(management_auth)],
        tags=["operations"],
    )
    def flush(source_id: str) -> dict[str, object]:
        try:
            return resolved.adapter.flush(source_id)
        except KeyError as exc:
            raise FtpAdapterHttpError(404, "not_found", "FTP adapter source was not found") from exc

    app.openapi_schema = apply_openapi_error_contract(app.openapi())
    return app


async def _scheduler(composition: FtpAdapterComposition) -> None:
    while True:
        try:
            await asyncio.to_thread(composition.adapter.run_once)
        except Exception:
            LOGGER.exception("Riverhog FTP adapter polling pass failed")
        await asyncio.sleep(composition.config.poll_seconds)


def _print(payload: Mapping[str, object], *, json_mode: bool) -> None:
    if json_mode:
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return
    if payload.get("format") == "riverhog-ftp-adapter-status/v1":
        print("Riverhog FTP adapter")
        print(f"provenance observer: {payload.get('provenance_observer') or 'none'}")
        rows = payload.get("sources")
        for row in rows if isinstance(rows, list) else []:
            if isinstance(row, Mapping):
                print(
                    f"- {row.get('id')}: claims={row.get('claims')}  "
                    f"scratch={row.get('claim_bytes')} bytes  "
                    f"completion-failures={row.get('completion_failures')}"
                )
                oldest = row.get("oldest_completion_failure")
                if isinstance(oldest, Mapping):
                    print(f"  oldest completion failure: {oldest.get('reason')}")
        if payload.get("next_page_token") is not None:
            print(f"next page token: {payload['next_page_token']}")
        return
    print(json.dumps(payload, indent=2, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="riverhog-ftp-adapter")
    parser.add_argument(
        "--version",
        action="version",
        version=importlib.metadata.version("riverhog-ftp-adapter"),
    )
    parser.add_argument("--config", type=Path)
    parser.add_argument("--base-url")
    parser.add_argument("--token")
    parser.add_argument("--allow-insecure-http", action="store_true", default=None)
    parser.add_argument("--json", action="store_true")
    sub = parser.add_subparsers(dest="command")
    serve = sub.add_parser("serve", help="run the adapter API and background poller")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8082)
    listen = sub.add_parser("listen", help="run the success-qualified FTP listener")
    listen.add_argument("--source", required=True)
    listen.add_argument("--username", required=True)
    listen.add_argument("--password-file", type=Path, required=True)
    listen.add_argument("--host", default="127.0.0.1")
    listen.add_argument("--port", type=int, default=2121)
    listen.add_argument("--passive-port-start", type=int, default=30000)
    listen.add_argument("--passive-port-end", type=int, default=30039)
    listen.add_argument("--public-host")
    listen.add_argument("--max-connections", type=int, default=256)
    listen.add_argument("--max-connections-per-ip", type=int, default=32)
    run = sub.add_parser("run", help="run one landing-source pass")
    run.set_defaults(func=_run_command)
    status = sub.add_parser("status", help="show one bounded page of source status")
    status.add_argument("--page-size", type=int, default=25)
    status.add_argument("--page-token")
    status.set_defaults(func=_status_command)
    sub.add_parser("check-config", help="validate connected FTP adapter configuration")
    flush = sub.add_parser("flush", help="explicitly close one source batch")
    flush.add_argument("source")
    flush.set_defaults(func=_flush_command)
    return parser


def _operator_client(args: argparse.Namespace) -> RiverhogFtpAdapterClient:
    return RiverhogFtpAdapterClient(
        base_url=args.base_url,
        token=args.token,
        allow_insecure_http=args.allow_insecure_http,
    )


def _run_command(args: argparse.Namespace) -> None:
    with _operator_client(args) as client:
        payload = client.run_ftp_adapter_pass()
    _print(payload, json_mode=args.json)


def _status_command(args: argparse.Namespace) -> None:
    with _operator_client(args) as client:
        payload = client.get_ftp_adapter_status(
            page_size=args.page_size,
            page_token=args.page_token,
        )
    _print(payload, json_mode=args.json)


def _flush_command(args: argparse.Namespace) -> None:
    with _operator_client(args) as client:
        payload = client.flush_ftp_adapter_source(args.source)
    _print(payload, json_mode=args.json)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    command = args.command or "serve"
    if command == "serve":
        config = load_config(args.config)
        composition = FtpAdapterComposition.build(config)
        uvicorn.run(create_app(composition), host=args.host, port=args.port)
        return 0
    if command == "check-config":
        config = load_config(args.config)
        config.source(config.sources[0].id)
        payload = {
            "format": "riverhog-ftp-adapter-config-check/v1",
            "status": "ok",
            "sources": len(config.sources),
        }
        _print(payload, json_mode=args.json)
        return 0
    if command == "listen":
        source = load_source_config(args.config, args.source)
        if not args.username or args.username.strip() != args.username or len(args.username) > 128:
            raise ValueError("FTP listener username must be 1..128 canonical characters")
        password_size = args.password_file.stat().st_size
        if not 1 <= password_size <= 4097:
            raise ValueError("FTP listener password file must be 1..4097 bytes")
        password = args.password_file.read_text(encoding="utf-8").strip()
        if not password:
            raise ValueError("FTP listener password file is empty")
        if len(password.encode("utf-8")) > 4096:
            raise ValueError("FTP listener password exceeds 4096 UTF-8 bytes")
        if not args.host or len(args.host) > 255:
            raise ValueError("FTP listener bind host must be 1..255 characters")
        if not 1 <= args.port <= 65535:
            raise ValueError("FTP listener port must be in 1..65535")
        if not 1 <= args.passive_port_start <= args.passive_port_end <= 65535:
            raise ValueError("FTP passive port range is invalid")
        if args.max_connections < 1 or args.max_connections_per_ip < 1:
            raise ValueError("FTP connection limits must be positive")
        public_host = str(ipaddress.IPv4Address(args.public_host)) if args.public_host else None
        serve_ftp(
            source_root=source.root,
            source_id=source.id,
            username=args.username,
            password=password,
            host=args.host,
            port=args.port,
            passive_ports=range(args.passive_port_start, args.passive_port_end + 1),
            public_host=public_host,
            max_connections=args.max_connections,
            max_connections_per_ip=args.max_connections_per_ip,
        )
        return 0
    callback = getattr(args, "func", None)
    if callback is None:
        raise RuntimeError(f"FTP adapter command has no implementation: {command}")
    callback(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["FtpAdapterComposition", "create_app", "main"]
