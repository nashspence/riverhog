"""Authenticated one-role ASGI service for the FFprobe observer."""

from __future__ import annotations

import argparse
import contextlib
import importlib.metadata
import os
import secrets
from collections.abc import AsyncIterator, Sequence
from pathlib import Path

import uvicorn
from a_stove0_ffprobe_streams_contract_lib import FFPROBE_STREAMS_SEMANTIC_VALIDATOR
from a_stove0_media_sampling_contract_lib import (
    MEDIA_SAMPLING_SEMANTIC_VALIDATOR,
)
from fastapi import Depends, FastAPI, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.security import HTTPBearer
from http_api_contracts import ErrorOut, HealthOut, error_payload, operation_openapi
from http_api_contracts.metadata_binding import read_control_body
from http_api_contracts.metadata_staging import MetadataStagingError
from riverhog_canonical_json import canonical_json_bytes
from stove0_extension_support import subprocess
from stove0_observer_protocol import SemanticValidatorRegistry
from stove0_observer_support import (
    OBSERVER_HTTP_OPERATIONS,
    ObserverHttpBinding,
    PersistentObserverService,
)

from a_stove0_ffprobe_observer.observer import FfprobeObserver

SERVICE = "a-stove0-ffprobe-observer"
_PUBLIC_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE"})
_bearer = HTTPBearer(auto_error=False)


def create_app(*, token: str, observer: FfprobeObserver, state_root: Path) -> FastAPI:
    credential = token.strip()
    if not credential:
        raise ValueError("FFprobe observer token must be nonempty")
    service = PersistentObserverService(
        observer,
        state_root=state_root,
        semantic_validators=SemanticValidatorRegistry(
            (FFPROBE_STREAMS_SEMANTIC_VALIDATOR, MEDIA_SAMPLING_SEMANTIC_VALIDATOR)
        ),
    )
    binding = ObserverHttpBinding(service)

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await run_in_threadpool(service.close)

    app = FastAPI(
        title="Stove0 FFprobe observer",
        version="1",
        openapi_url="/v1/openapi.json",
        lifespan=lifespan,
    )

    app.state.observer_service = service

    @app.get("/health/live", response_model=HealthOut, tags=["health"])
    def live() -> dict[str, str]:
        return {"service": SERVICE, "status": "ok"}

    @app.get(
        "/health/ready",
        response_model=HealthOut,
        responses={503: {"model": ErrorOut}},
        tags=["health"],
    )
    def ready() -> Response:
        result = subprocess.run(
            [observer.ffprobe, "-version"], check=False, capture_output=True, timeout=15
        )
        if result.returncode:
            return _error(503, "service_unavailable", "FFprobe is not ready")
        return Response(
            content=b'{"service":"a-stove0-ffprobe-observer","status":"ok"}',
            media_type="application/json",
        )

    async def dispatch(request: Request) -> Response:
        scheme, _, supplied = request.headers.get("authorization", "").partition(" ")
        if scheme.casefold() != "bearer" or not secrets.compare_digest(supplied, credential):
            return _error(401, "unauthorized", "Bearer credential is not authorized")
        try:
            body = await read_control_body(
                request, maximum_request_bytes=binding.maximum_request_bytes
            )
        except MetadataStagingError as exc:
            return Response(
                content=canonical_json_bytes({"error": {"code": exc.code, "message": exc.message}}),
                status_code=exc.status,
                media_type="application/json",
            )
        result = await run_in_threadpool(binding.handle, request.method, request.url.path, body)
        return Response(
            content=result.body, status_code=result.status, headers=dict(result.headers)
        )

    methods_by_path: dict[str, set[str]] = {}
    for operation in OBSERVER_HTTP_OPERATIONS:
        methods_by_path.setdefault(operation.path, set()).add(operation.method)
        app.add_api_route(
            operation.path,
            dispatch,
            methods=[operation.method],
            dependencies=[Depends(_bearer)],
            tags=["observer"],
            **operation_openapi(operation),
        )
    for path, supported in methods_by_path.items():
        app.add_api_route(
            path,
            dispatch,
            methods=sorted(_PUBLIC_METHODS - supported),
            include_in_schema=False,
        )
    return app


def _error(status: int, code: str, message: str) -> Response:
    import json

    return Response(
        content=json.dumps(error_payload(code=code, message=message), separators=(",", ":")),
        status_code=status,
        media_type="application/json",
    )


def _secret() -> str:
    direct = os.getenv("A_STOVE0_FFPROBE_OBSERVER_TOKEN")
    path = os.getenv("A_STOVE0_FFPROBE_OBSERVER_TOKEN_FILE")
    if bool(direct) == bool(path):
        raise ValueError("set exactly one FFprobe observer token source")
    value = direct if direct is not None else Path(str(path)).read_text(encoding="utf-8")
    if not value.strip():
        raise ValueError("FFprobe observer token must be nonempty")
    return value.strip()


_CLI_RESULT_CONTRACT = {
    "format": "riverhog-cli-result-contract/v1",
    "identity_prefix": "a-stove0-ffprobe-observer-cli-result",
    "default_profile": "runtime",
    "profiles": {
        "runtime": {
            "id": "a-stove0-ffprobe-observer-cli-runtime/v1",
            "structured_output": "none",
            "human_json_relationship": "not-applicable",
            "success": [
                {
                    "id": "stopped",
                    "exit_status": 0,
                    "stdout": {"all": "no-command-result"},
                    "stderr": {"all": "noncontractual-runtime-log"},
                }
            ],
            "failures": [
                {
                    "id": "usage",
                    "exit_status": 2,
                    "stdout": {"all": "empty"},
                    "stderr": {"all": "noncontractual-usage-diagnostic"},
                }
            ],
        }
    },
    "command_profiles": {},
    "command_overrides": {},
    "executable_groups": [],
    "outcome_selectors": {
        "stopped": {"kind": "service-runtime-returned"},
        "usage": {"kind": "parser-rejected-invocation"},
    },
    "output_authorities": {},
    "version_distribution": "a-stove0-ffprobe-observer",
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=SERVICE)
    parser.add_argument("--version", action="version", version=importlib.metadata.version(SERVICE))
    parser.add_argument("--host", default=os.getenv("A_STOVE0_FFPROBE_OBSERVER_HOST", "127.0.0.1"))
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("A_STOVE0_FFPROBE_OBSERVER_PORT", "8080")),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    observer = FfprobeObserver(
        ffprobe=os.getenv("STOVE0_FFPROBE_BIN", "ffprobe"),
        workspace_root=Path(
            os.getenv(
                "A_STOVE0_FFPROBE_OBSERVER_WORKSPACE",
                "/run/a-stove0-ffprobe-observer",
            )
        ),
        source_revision=os.getenv("A_STOVE0_FFPROBE_OBSERVER_SOURCE_REVISION", "unknown"),
        image_id=_image_id(),
    )
    token = _secret()
    with contextlib.suppress(KeyError):
        os.environ.pop("A_STOVE0_FFPROBE_OBSERVER_TOKEN")
    application = create_app(
        token=token,
        observer=observer,
        state_root=Path(
            os.getenv("A_STOVE0_FFPROBE_OBSERVER_STATE", "/var/lib/a-stove0-ffprobe-observer")
        ),
    )
    try:
        uvicorn.run(application, host=args.host, port=args.port)
    finally:
        application.state.observer_service.close()
    return 0


def _image_id() -> str:
    value = os.getenv("A_STOVE0_FFPROBE_OBSERVER_IMAGE_ID", "").strip()
    if not (
        len(value) == 71
        and value.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(
            "A_STOVE0_FFPROBE_OBSERVER_IMAGE_ID must be an OCI ImageID (sha256:<64 lowercase hex>)"
        )
    return value


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["SERVICE", "create_app", "main"]
