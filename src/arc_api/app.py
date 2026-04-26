from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from datetime import timedelta

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from arc_api.auth import api_auth_dependencies
from arc_api.deps import ServiceContainer, default_container, get_container
from arc_api.routers.collections import router as collections_router
from arc_api.routers.fetches import router as fetches_router
from arc_api.routers.files import router as files_router
from arc_api.routers.images import router as images_router
from arc_api.routers.pins import router as pins_router
from arc_api.routers.plan import router as plan_router
from arc_api.routers.search import router as search_router
from arc_api.schemas.common import ErrorBody, ErrorResponse
from arc_core.domain.errors import ArcError
from arc_core.runtime_config import load_runtime_config

_LOG = logging.getLogger(__name__)


def _sweep_expired_uploads(container: ServiceContainer) -> None:
    container.collections.expire_stale_uploads()
    container.fetches.expire_stale_uploads()


async def _run_upload_expiry_reaper(
    container: ServiceContainer,
    *,
    sweep_interval: timedelta,
) -> None:
    interval_seconds = max(sweep_interval.total_seconds(), 0.1)
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            await asyncio.to_thread(_sweep_expired_uploads, container)
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive background task logging
            _LOG.exception("upload expiry reaper sweep failed")


def create_app(
    *,
    container: ServiceContainer | None = None,
    upload_expiry_reaper_interval: float | None = None,
) -> FastAPI:
    config = load_runtime_config()
    app_container = container or default_container()
    sweep_interval = (
        timedelta(seconds=upload_expiry_reaper_interval)
        if upload_expiry_reaper_interval is not None
        else config.upload_expiry_sweep_interval
    )

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        task = asyncio.create_task(
            _run_upload_expiry_reaper(app_container, sweep_interval=sweep_interval)
        )
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    app = FastAPI(title="arc API", version="0.1.0", lifespan=lifespan)
    app.dependency_overrides[get_container] = lambda: app_container

    @app.exception_handler(ArcError)
    async def handle_arc_error(_: Request, exc: ArcError) -> JSONResponse:
        status_map = {
            "bad_request": 400,
            "invalid_target": 400,
            "not_found": 404,
            "conflict": 409,
            "invalid_state": 409,
            "hash_mismatch": 409,
            "not_implemented": 501,
        }
        payload = ErrorResponse(error=ErrorBody(code=exc.code, message=exc.message))
        return JSONResponse(status_code=status_map.get(exc.code, 400), content=payload.model_dump())

    @app.exception_handler(NotImplementedError)
    async def handle_builtin_not_implemented(_: Request, exc: NotImplementedError) -> JSONResponse:
        payload = ErrorResponse(
            error=ErrorBody(code="not_implemented", message=str(exc) or "not implemented")
        )
        return JSONResponse(status_code=501, content=payload.model_dump())

    auth_deps = list(api_auth_dependencies())
    app.include_router(files_router, prefix="/v1", dependencies=auth_deps)
    app.include_router(collections_router, prefix="/v1", dependencies=auth_deps)
    app.include_router(search_router, prefix="/v1", dependencies=auth_deps)
    app.include_router(plan_router, prefix="/v1", dependencies=auth_deps)
    app.include_router(images_router, prefix="/v1", dependencies=auth_deps)
    app.include_router(pins_router, prefix="/v1", dependencies=auth_deps)
    app.include_router(fetches_router, prefix="/v1", dependencies=auth_deps)
    return app


def main() -> None:
    uvicorn.run("arc_api.app:create_app", factory=True, reload=False)


if __name__ == "__main__":
    main()
