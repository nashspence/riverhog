from __future__ import annotations

import pytest
from jsonschema import Draft202012Validator
from pydantic import TypeAdapter, ValidationError
from riverhog_api.schemas.apps import AppKeyOut
from riverhog_api.schemas.quotas import KeyDownloadQuotaOut, SetKeyDownloadQuotaRequest
from riverhog_application_access import MonthlyDownloadQuotaBytes


def test_monthly_download_quota_schema_matches_runtime_domain() -> None:
    adapter = TypeAdapter(MonthlyDownloadQuotaBytes)
    schema = adapter.json_schema()
    Draft202012Validator.check_schema(schema)
    assert schema == {"minimum": 0, "type": "integer"}

    validator = Draft202012Validator(schema)
    for value in (0, 1, 2**63):
        assert adapter.validate_python(value) == value
        assert validator.is_valid(value)

    for value in (-1, True, 1.5, "1"):
        with pytest.raises(ValidationError):
            adapter.validate_python(value)
        assert not validator.is_valid(value)


@pytest.mark.parametrize(
    ("model", "field"),
    (
        (AppKeyOut, "monthly_download_quota_bytes"),
        (KeyDownloadQuotaOut, "monthly_bytes"),
        (SetKeyDownloadQuotaRequest, "monthly_bytes"),
    ),
)
def test_monthly_download_quota_api_models_publish_the_minimum(model: type, field: str) -> None:
    schema = model.model_json_schema()
    field_schema = schema["properties"][field]
    Draft202012Validator.check_schema(schema)
    assert schema["$defs"]["MonthlyDownloadQuotaBytes"] == {
        "minimum": 0,
        "type": "integer",
    }
    assert {"$ref": "#/$defs/MonthlyDownloadQuotaBytes"} in field_schema["anyOf"]
