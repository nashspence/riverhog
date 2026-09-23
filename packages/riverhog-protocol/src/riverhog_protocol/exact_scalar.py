"""Lossless JSON string representations for exact protocol integers."""

from __future__ import annotations

from typing import Annotated

from pydantic import BeforeValidator, PlainSerializer, WithJsonSchema
from riverhog_canonical_json import format_scalar, parse_scalar, scalar_schema

type NonnegativeDecimal = Annotated[
    int,
    BeforeValidator(lambda value: parse_scalar("nonnegative", value)),
    PlainSerializer(lambda value: format_scalar("nonnegative", value), return_type=str),
    WithJsonSchema(scalar_schema("nonnegative")),
]
type Sequence256Hex = Annotated[
    int,
    BeforeValidator(lambda value: parse_scalar("sequence256", value)),
    PlainSerializer(lambda value: format_scalar("sequence256", value), return_type=str),
    WithJsonSchema(scalar_schema("sequence256")),
]

__all__ = ["NonnegativeDecimal", "Sequence256Hex"]
