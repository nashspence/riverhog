"""Authenticated read-provenance observer service."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import secrets
from collections.abc import Sequence
from pathlib import Path

import uvicorn
from a_stove0_materialization_hint_evidence_contract_lib import (
    MATERIALIZATION_HINT_SEMANTIC_VALIDATOR,
)
from a_stove0_riverhog_provenance_evidence_contract_lib import (
    CORE_PROVENANCE_SEMANTIC_VALIDATOR,
)
from fastapi import Depends, FastAPI, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.security import HTTPBearer
from http_api_contracts import HealthOut, error_payload, operation_openapi
from stove0_observer_protocol import SemanticValidatorRegistry
from stove0_observer_support import OBSERVER_HTTP_OPERATIONS, ObserverHttpBinding

from .observer import RiverhogProvenanceObserver

SERVICE = "a-stove0-riverhog-provenance-observer"
PREFIX = "A_STOVE0_RIVERHOG_PROVENANCE_OBSERVER"
_PUBLIC_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE"})
_bearer = HTTPBearer(auto_error=False)


def _error(status: int, code: str, message: str) -> Response:
    return Response(
        content=json.dumps(error_payload(code=code, message=message), separators=(",", ":")),
        status_code=status,
        media_type="application/json",
    )


def create_app(*, token: str, observer: RiverhogProvenanceObserver) -> FastAPI:
    credential = token.strip()
    if not credential:
        raise ValueError("provenance observer token must be nonempty")
    binding = ObserverHttpBinding(
        observer,
        semantic_validators=SemanticValidatorRegistry(
            (CORE_PROVENANCE_SEMANTIC_VALIDATOR, MATERIALIZATION_HINT_SEMANTIC_VALIDATOR)
        ),
    )
    app = FastAPI(
        title="Stove0 Riverhog provenance observer", version="1", openapi_url="/v1/openapi.json"
    )

    @app.get("/health/live", response_model=HealthOut, tags=["health"])
    def live() -> dict[str, str]:
        return {"service": SERVICE, "status": "ok"}

    @app.get("/health/ready", response_model=HealthOut, tags=["health"])
    def ready() -> dict[str, str]:
        return {"service": SERVICE, "status": "ok"}

    async def dispatch(request: Request) -> Response:
        scheme, _, supplied = request.headers.get("authorization", "").partition(" ")
        if scheme.casefold() != "bearer" or not secrets.compare_digest(supplied, credential):
            return _error(401, "unauthorized", "Bearer credential is not authorized")
        result = await run_in_threadpool(
            binding.handle, request.method, request.url.path, await request.body()
        )
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


def _secret() -> str:
    direct = os.getenv(f"{PREFIX}_TOKEN")
    path = os.getenv(f"{PREFIX}_TOKEN_FILE")
    if bool(direct) == bool(path):
        raise ValueError("set exactly one provenance observer token source")
    value = direct if direct is not None else Path(str(path)).read_text(encoding="utf-8")
    if not value.strip():
        raise ValueError("provenance observer token must be nonempty")
    return value.strip()


def _image_id() -> str:
    value = os.getenv(f"{PREFIX}_IMAGE_ID", "").strip()
    if not (
        len(value) == 71
        and value.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{PREFIX}_IMAGE_ID must be an OCI ImageID (sha256:<64 lowercase hex>)")
    return value


_CLI_RESULT_CONTRACT = {
    "format": "riverhog-cli-result-contract/v1",
    "identity_prefix": "a-stove0-riverhog-provenance-observer-cli-result",
    "default_profile": "runtime",
    "profiles": {
        "runtime": {
            "id": "a-stove0-riverhog-provenance-observer-cli-runtime/v1",
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
    "version_distribution": SERVICE,
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=SERVICE)
    parser.add_argument("--version", action="version", version=importlib.metadata.version(SERVICE))
    parser.add_argument("--host", default=os.getenv(f"{PREFIX}_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv(f"{PREFIX}_PORT", "8080")))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    observer = RiverhogProvenanceObserver(
        source_revision=os.getenv(f"{PREFIX}_SOURCE_REVISION", "unknown"),
        image_id=_image_id(),
    )
    token = _secret()
    os.environ.pop(f"{PREFIX}_TOKEN", None)
    uvicorn.run(create_app(token=token, observer=observer), host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["SERVICE", "create_app", "main"]
