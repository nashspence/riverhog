from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .config import ensure_directories
from .db import Base, engine
from .hooks import router as hooks_router
from .routes.discs import router as discs_router
from .routes.jobs import router as jobs_router
from .routes.progress import router as progress_router


@asynccontextmanager
async def lifespan(_app: FastAPI):
    ensure_directories()
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(title="Archive Storage MVP", version="0.4.0", lifespan=lifespan)


@app.get("/healthz")
def healthcheck():
    return {"status": "ok"}


@app.exception_handler(ValueError)
async def value_error_handler(_request, exc: ValueError):
    return JSONResponse(status_code=400, content={"detail": str(exc)})


app.include_router(jobs_router)
app.include_router(discs_router)
app.include_router(progress_router)
app.include_router(hooks_router)
