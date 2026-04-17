from __future__ import annotations

import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .config import API_TOKEN, HOOK_SECRET

_bearer = HTTPBearer(auto_error=False)


def require_api_auth(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> None:
    token = credentials.credentials if credentials else ""
    if not secrets.compare_digest(token, API_TOKEN):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid api token",
            headers={"WWW-Authenticate": "Bearer"},
        )


def hook_auth_ok(hook_secret: str) -> bool:
    return secrets.compare_digest(hook_secret, HOOK_SECRET)
