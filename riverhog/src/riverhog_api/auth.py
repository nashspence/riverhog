from __future__ import annotations

import os
import secrets
from collections.abc import Callable, Sequence
from typing import Annotated, Any, cast

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from riverhog_core.app_permissions import (
    ARCHIVES_MANAGE,
    ARCHIVES_READ,
    CATALOG_READ,
    COLLECTION_DESCRIPTIONS_MANAGE,
    COLLECTION_PROCESSING_CONTROL,
    COLLECTION_PROCESSING_EXECUTE,
    COLLECTION_TAGS_MANAGE,
    COLLECTIONS_CREATE,
    COLLECTIONS_DELETE,
    EVENTS_READ,
    KEYS_MANAGE,
    PROVENANCE_EXPORT,
    PROVENANCE_READ,
    QUOTAS_MANAGE,
    RETRIEVAL_MANAGE,
    ApplicationAccess,
    Principal,
)
from riverhog_protocol.errors import Forbidden, Unauthorized

from riverhog_api.deps import ContainerDep, ServiceContainer

BOOTSTRAP_TOKEN_ENV = "RIVERHOG_BOOTSTRAP_TOKEN"

_bearer = HTTPBearer(auto_error=False)
BearerCredentials = Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)]
PermissionDependency = Callable[..., Principal]


def authenticate_token(token: str, container: ServiceContainer) -> Principal | None:
    supplied = token.strip()
    if not supplied:
        return None
    bootstrap = os.getenv(BOOTSTRAP_TOKEN_ENV, "")
    if bootstrap and secrets.compare_digest(supplied, bootstrap):
        return Principal(
            id="bootstrap",
            key_id=None,
            access=frozenset(
                {
                    ApplicationAccess(KEYS_MANAGE),
                    ApplicationAccess(QUOTAS_MANAGE),
                }
            ),
            unrestricted_delegation=True,
        )
    principal = container.app_keys.authenticate(supplied)
    if principal is not None:
        return principal
    workflows = getattr(container, "collection_workflows", None)
    return workflows.authenticate_capability(supplied) if workflows is not None else None


def require_principal(
    credentials: BearerCredentials,
    container: ContainerDep,
) -> Principal:
    supplied = credentials.credentials if credentials is not None else ""
    principal = authenticate_token(supplied, container)
    if principal is not None:
        return principal
    raise Unauthorized("invalid bearer token")


def require_permission(permission: str) -> PermissionDependency:
    def dependency(
        credentials: BearerCredentials,
        container: ContainerDep,
    ) -> Principal:
        principal = require_principal(credentials, container)
        if principal.allows(permission):
            return principal
        raise Forbidden(f"permission required: {permission}")

    dependency.riverhog_permission = permission  # type: ignore[attr-defined]
    return dependency


def require_any_permission(*permissions: str) -> PermissionDependency:
    if not permissions:
        raise ValueError("at least one Riverhog permission is required")

    def dependency(
        credentials: BearerCredentials,
        container: ContainerDep,
    ) -> Principal:
        principal = require_principal(credentials, container)
        if any(principal.allows(permission) for permission in permissions):
            return principal
        raise Forbidden("one permission is required: " + ", ".join(permissions))

    dependency.riverhog_permissions = tuple(permissions)  # type: ignore[attr-defined]
    return dependency


def apply_openapi_permission_contract(
    schema: dict[str, Any],
    routes: Sequence[object],
) -> dict[str, Any]:
    """Project executable permission dependencies into each Riverhog operation."""

    route_contracts: list[tuple[str, set[str], object]] = []
    for route in routes:
        candidates = getattr(route, "_effective_candidates", ())
        if candidates:
            route_contracts.extend(
                (candidate.path_format, candidate.methods, candidate.dependant)
                for candidate in candidates
            )
            continue
        path = getattr(route, "path", "")
        methods: set[str] = getattr(route, "methods", set())
        dependant = getattr(route, "dependant", None)
        if dependant is not None:
            route_contracts.append((path, methods, dependant))

    for path, methods, dependant in route_contracts:
        if not str(path).startswith("/v1") or dependant is None:
            continue
        requirements: set[tuple[str, ...]] = set()
        pending = [dependant]
        while pending:
            current = pending.pop()
            call = getattr(current, "call", None)
            permission = getattr(call, "riverhog_permission", None)
            permissions = getattr(call, "riverhog_permissions", None)
            if isinstance(permission, str):
                requirements.add((permission,))
            if isinstance(permissions, tuple) and all(
                isinstance(item, str) for item in permissions
            ):
                requirements.add(tuple(sorted(permissions)))
            pending.extend(getattr(current, "dependencies", ()))
        if not requirements:
            raise RuntimeError(f"public Riverhog operation has no permission contract: {path}")
        path_item = schema["paths"][path]
        projection = [{"any_of": list(group)} for group in sorted(requirements)]
        for method in methods:
            operation = path_item.get(str(method).casefold())
            if isinstance(operation, dict):
                operation["x-riverhog-permission-requirements"] = projection
    return schema


CatalogReader = Annotated[
    Principal,
    Depends(cast(Callable[..., object], require_permission(CATALOG_READ))),
]
RetrievalManager = Annotated[
    Principal,
    Depends(cast(Callable[..., object], require_permission(RETRIEVAL_MANAGE))),
]
CollectionCreator = Annotated[
    Principal,
    Depends(cast(Callable[..., object], require_permission(COLLECTIONS_CREATE))),
]
CollectionDescriptionManager = Annotated[
    Principal,
    Depends(
        cast(
            Callable[..., object],
            require_permission(COLLECTION_DESCRIPTIONS_MANAGE),
        )
    ),
]
CollectionUploadReader = Annotated[
    Principal,
    Depends(
        cast(
            Callable[..., object],
            require_any_permission(COLLECTIONS_CREATE, COLLECTIONS_DELETE),
        )
    ),
]
CollectionProcessingController = Annotated[
    Principal,
    Depends(cast(Callable[..., object], require_permission(COLLECTION_PROCESSING_CONTROL))),
]
CollectionProcessingExecutor = Annotated[
    Principal,
    Depends(cast(Callable[..., object], require_permission(COLLECTION_PROCESSING_EXECUTE))),
]
CollectionProcessingLeaseManager = Annotated[
    Principal,
    Depends(
        cast(
            Callable[..., object],
            require_any_permission(
                COLLECTION_PROCESSING_CONTROL,
                COLLECTION_PROCESSING_EXECUTE,
            ),
        )
    ),
]
CollectionTagManager = Annotated[
    Principal,
    Depends(
        cast(
            Callable[..., object],
            require_permission(COLLECTION_TAGS_MANAGE),
        )
    ),
]
CollectionDeleter = Annotated[
    Principal,
    Depends(cast(Callable[..., object], require_permission(COLLECTIONS_DELETE))),
]
ArchiveReader = Annotated[
    Principal,
    Depends(cast(Callable[..., object], require_permission(ARCHIVES_READ))),
]
ArchiveManager = Annotated[
    Principal,
    Depends(cast(Callable[..., object], require_permission(ARCHIVES_MANAGE))),
]
KeyManager = Annotated[
    Principal,
    Depends(cast(Callable[..., object], require_permission(KEYS_MANAGE))),
]
QuotaManager = Annotated[
    Principal,
    Depends(cast(Callable[..., object], require_permission(QUOTAS_MANAGE))),
]
EventsReader = Annotated[
    Principal,
    Depends(cast(Callable[..., object], require_permission(EVENTS_READ))),
]
ProvenanceReader = Annotated[
    Principal,
    Depends(cast(Callable[..., object], require_permission(PROVENANCE_READ))),
]
ProvenanceExporter = Annotated[
    Principal,
    Depends(cast(Callable[..., object], require_permission(PROVENANCE_EXPORT))),
]


__all__ = [
    "ArchiveManager",
    "ArchiveReader",
    "BOOTSTRAP_TOKEN_ENV",
    "CatalogReader",
    "CollectionCreator",
    "CollectionDescriptionManager",
    "CollectionDeleter",
    "CollectionTagManager",
    "CollectionProcessingController",
    "CollectionProcessingExecutor",
    "CollectionProcessingLeaseManager",
    "CollectionUploadReader",
    "EventsReader",
    "KeyManager",
    "QuotaManager",
    "ProvenanceExporter",
    "ProvenanceReader",
    "RetrievalManager",
    "authenticate_token",
    "apply_openapi_permission_contract",
    "require_principal",
    "require_any_permission",
    "require_permission",
]
