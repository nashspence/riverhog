from __future__ import annotations

from http_api_contracts import ErrorBody, ErrorOut, HealthOut
from pydantic import BaseModel, ConfigDict


class RiverhogModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


__all__ = ["ErrorBody", "ErrorOut", "HealthOut", "RiverhogModel"]
