"""Identities of registered applications and delegated processing principals."""

from __future__ import annotations

import re
from typing import Annotated

from pydantic import AfterValidator, Field

_APPLICATION_NAME_BODY = r"[a-z0-9]+(?:-[a-z0-9]+)*"
APPLICATION_NAME_PATTERN = rf"^{_APPLICATION_NAME_BODY}$"
PRINCIPAL_ID_PATTERN = (
    rf"^(?:{_APPLICATION_NAME_BODY}|claim:[0-9a-f]{{64}}|processing:[0-9a-f]{{64}})$"
)
_APPLICATION_NAME = re.compile(APPLICATION_NAME_PATTERN)
_PRINCIPAL_ID = re.compile(PRINCIPAL_ID_PATTERN)


def validate_application_name(value: str) -> str:
    if type(value) is not str or _APPLICATION_NAME.fullmatch(value) is None:
        raise ValueError("app name must use lowercase letters, digits, and single dashes")
    return value


def validate_principal_id(value: str) -> str:
    if type(value) is not str or _PRINCIPAL_ID.fullmatch(value) is None:
        raise ValueError(
            "principal ID must be an application name or a claim/processing SHA-256 ID"
        )
    return value


type ApplicationName = Annotated[
    str,
    Field(pattern=APPLICATION_NAME_PATTERN),
    AfterValidator(validate_application_name),
]
type PrincipalId = Annotated[
    str,
    Field(pattern=PRINCIPAL_ID_PATTERN),
    AfterValidator(validate_principal_id),
]


__all__ = [
    "APPLICATION_NAME_PATTERN",
    "ApplicationName",
    "PRINCIPAL_ID_PATTERN",
    "PrincipalId",
    "validate_application_name",
    "validate_principal_id",
]
