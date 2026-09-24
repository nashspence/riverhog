"""Admit exact JSON text before FastAPI loses duplicate members."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any

from fastapi import HTTPException, Request, Response
from fastapi.routing import APIRoute
from riverhog_canonical_json import CanonicalJsonError, parse_identity_json


class ExactJsonRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def admit(request: Request) -> Response:
            media_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
            if media_type in {"", "application/json"} or media_type.endswith("+json"):
                raw = await request.body()
                if raw:
                    try:
                        document = parse_identity_json(raw)
                    except CanonicalJsonError as exc:
                        raise HTTPException(
                            status_code=400, detail=f"invalid JSON request: {exc}"
                        ) from exc
                    request._json = document
            return await handler(request)

        return admit
