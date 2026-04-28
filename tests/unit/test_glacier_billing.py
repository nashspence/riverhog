from __future__ import annotations

import gzip
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from arc_core.runtime_config import RuntimeConfig
from arc_core.services.glacier_billing import resolve_glacier_billing


def _config(tmp_path: Path, **overrides: object) -> RuntimeConfig:
    config = RuntimeConfig(
        object_store="s3",
        s3_endpoint_url="http://example.invalid:9000",
        s3_region="us-east-1",
        s3_bucket="riverhog",
        s3_access_key_id="test-access",
        s3_secret_access_key="test-secret",
        s3_force_path_style=True,
        tusd_base_url="http://example.invalid:1080/files",
        tusd_hook_secret="hook-secret",
        sqlite_path=tmp_path / "state.sqlite3",
    )
    return replace(config, **overrides)


def test_resolve_glacier_billing_returns_unavailable_for_non_aws_runtime(tmp_path: Path) -> None:
    summary = resolve_glacier_billing(_config(tmp_path), include=True)

    assert summary is not None
    assert summary.actuals is not None
    assert summary.actuals.source == "unavailable"
    assert summary.forecast is not None
    assert summary.forecast.source == "unavailable"
    assert summary.exports is not None
    assert summary.exports.source == "unavailable"
    assert summary.invoices is not None
    assert summary.invoices.source == "unavailable"


def test_resolve_glacier_billing_prefers_bucket_scoped_actuals(
    tmp_path: Path,
    monkeypatch,
) -> None:
    recorded: dict[str, object] = {}

    class _FakeCostExplorerClient:
        def get_cost_and_usage(self, **kwargs):
            calls = recorded.setdefault("actual_calls", [])
            assert isinstance(calls, list)
            calls.append(kwargs)
            current_filter = kwargs["Filter"]["And"]
            resource_values = [
                item["Dimensions"]["Values"]
                for item in current_filter
                if item["Dimensions"]["Key"] == "RESOURCE_ID"
            ]
            if resource_values:
                return {
                    "ResultsByTime": [
                        {
                            "TimePeriod": {"Start": "2026-04-14", "End": "2026-04-15"},
                            "Estimated": False,
                            "Total": {
                                "UnblendedCost": {"Amount": "0.44", "Unit": "USD"},
                                "UsageQuantity": {"Amount": "11.00", "Unit": "N/A"},
                            },
                        }
                    ]
                }
            return {
                "ResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2026-04-01", "End": "2026-05-01"},
                        "Estimated": True,
                        "Total": {
                            "UnblendedCost": {"Amount": "12.34", "Unit": "USD"},
                            "UsageQuantity": {"Amount": "56.78", "Unit": "N/A"},
                        },
                    }
                ]
            }

        def get_cost_forecast(self, **kwargs):
            recorded["forecast"] = kwargs
            return {
                "ForecastResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2026-05-01", "End": "2026-06-01"},
                        "MeanValue": "14.50",
                        "PredictionIntervalLowerBound": "11.00",
                        "PredictionIntervalUpperBound": "18.00",
                    }
                ]
            }

    monkeypatch.setattr(
        "arc_core.services.glacier_billing._create_cost_explorer_client",
        lambda config: _FakeCostExplorerClient(),
    )
    monkeypatch.setattr(
        "arc_core.services.glacier_billing._resolve_billing_exports",
        lambda config: None,
    )
    monkeypatch.setattr(
        "arc_core.services.glacier_billing._resolve_invoices",
        lambda config: None,
    )

    summary = resolve_glacier_billing(
        _config(
            tmp_path,
            glacier_endpoint_url="https://s3.us-west-2.amazonaws.com",
            glacier_backend="aws",
        ),
        include=True,
    )

    assert summary is not None
    assert summary.actuals is not None
    assert summary.actuals.source == "aws_cost_explorer_resource"
    assert summary.actuals.scope == "bucket"
    assert summary.actuals.filter_label == "riverhog"
    assert summary.actuals.granularity == "DAILY"
    assert summary.actuals.periods[0].unblended_cost_usd == 0.44
    assert summary.forecast is not None
    assert summary.forecast.scope == "service"
    assert "bucket-resource forecasting" in summary.forecast.notes[0]

    actual_calls = recorded["actual_calls"]
    assert isinstance(actual_calls, list)
    bucket_filter = actual_calls[0]["Filter"]
    assert bucket_filter == {
        "And": [
            {"Dimensions": {"Key": "SERVICE", "Values": ["Amazon Simple Storage Service"]}},
            {"Dimensions": {"Key": "REGION", "Values": ["us-west-2"]}},
            {"Dimensions": {"Key": "RESOURCE_ID", "Values": ["riverhog"]}},
        ]
    }


def test_resolve_glacier_billing_falls_back_to_tag_scope_when_bucket_actuals_unavailable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class _FakeCostExplorerClient:
        def get_cost_and_usage(self, **kwargs):
            filters = kwargs["Filter"]["And"]
            has_resource_id = any(
                item.get("Dimensions", {}).get("Key") == "RESOURCE_ID" for item in filters
            )
            if has_resource_id:
                raise RuntimeError("DataUnavailableException: resource-level data unavailable")
            return {
                "ResultsByTime": [
                    {
                        "TimePeriod": {"Start": "2026-04-01", "End": "2026-05-01"},
                        "Estimated": False,
                        "Total": {
                            "UnblendedCost": {"Amount": "12.34", "Unit": "USD"},
                            "UsageQuantity": {"Amount": "56.78", "Unit": "N/A"},
                        },
                    }
                ]
            }

        def get_cost_forecast(self, **kwargs):
            return {"ForecastResultsByTime": []}

    monkeypatch.setattr(
        "arc_core.services.glacier_billing._create_cost_explorer_client",
        lambda config: _FakeCostExplorerClient(),
    )
    monkeypatch.setattr(
        "arc_core.services.glacier_billing._resolve_billing_exports",
        lambda config: None,
    )
    monkeypatch.setattr(
        "arc_core.services.glacier_billing._resolve_invoices",
        lambda config: None,
    )

    summary = resolve_glacier_billing(
        _config(
            tmp_path,
            glacier_endpoint_url="https://s3.us-west-2.amazonaws.com",
            glacier_billing_tag_key="backup_set",
            glacier_billing_tag_value="optical_archive",
        ),
        include=True,
    )

    assert summary is not None
    assert summary.actuals is not None
    assert summary.actuals.source == "aws_cost_explorer"
    assert summary.actuals.scope == "tag"
    assert summary.actuals.filter_label == "backup_set=optical_archive"
    assert "resource-level daily data" in summary.actuals.notes[0]


def test_resolve_glacier_billing_reads_cur_or_data_exports_breakdowns(
    tmp_path: Path,
    monkeypatch,
) -> None:
    export_csv = "\n".join(
        [
            "line_item_product_code,line_item_usage_type,line_item_operation,"
            "line_item_resource_id,line_item_unblended_cost,line_item_usage_amount,"
            "line_item_usage_unit",
            "AmazonS3,TimedStorage-GlacierByteHrs,StandardStorage,riverhog,1.25,100,GB-Mo",
            "AmazonS3,Requests-Tier1,GetObject,riverhog,0.25,10,Requests",
        ]
    )

    class _Body:
        def read(self) -> bytes:
            return gzip.compress(export_csv.encode("utf-8"))

    class _Paginator:
        def paginate(self, **kwargs):
            return [
                {
                    "Contents": [
                        {
                            "Key": "billing/export-2026-04.csv.gz",
                            "LastModified": datetime(2026, 4, 28, tzinfo=UTC),
                        }
                    ]
                }
            ]

    class _FakeS3Client:
        def get_paginator(self, name: str):
            assert name == "list_objects_v2"
            return _Paginator()

        def get_object(self, **kwargs):
            return {"Body": _Body()}

    monkeypatch.setattr(
        "arc_core.services.glacier_billing._create_billing_s3_client",
        lambda config: _FakeS3Client(),
    )

    view = resolve_glacier_billing(
        _config(
            tmp_path,
            glacier_endpoint_url="https://s3.us-west-2.amazonaws.com",
            glacier_backend="aws",
            glacier_billing_export_bucket="billing-bucket",
            glacier_billing_export_prefix="billing",
        ),
        include=True,
    ).exports

    assert view is not None
    assert view.source == "aws_cur_s3"
    assert view.scope == "bucket"
    assert view.bucket == "billing-bucket"
    assert view.object_key == "billing/export-2026-04.csv.gz"
    assert view.rows_scanned == 2
    assert view.breakdowns[0].unblended_cost_usd == 1.25
    assert view.breakdowns[0].resource_id == "riverhog"


def test_resolve_glacier_billing_reads_invoice_summaries(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class _FakeStsClient:
        def get_caller_identity(self):
            return {"Account": "123456789012"}

    class _FakeInvoicingClient:
        def list_invoice_summaries(self, **kwargs):
            assert kwargs["Selector"] == {
                "ResourceType": "ACCOUNT_ID",
                "Value": "123456789012",
            }
            return {
                "InvoiceSummaries": [
                    {
                        "AccountId": "123456789012",
                        "InvoiceId": "INV-001",
                        "InvoiceType": "Invoice",
                        "BillingPeriod": {"Year": 2026, "Month": 4},
                        "IssuedDate": 1714521600,
                        "DueDate": 1715126400,
                        "Entity": {"InvoicingEntity": "Amazon Web Services, Inc."},
                        "BaseCurrencyAmount": {
                            "CurrencyCode": "USD",
                            "TotalAmount": "99.50",
                        },
                        "PaymentCurrencyAmount": {
                            "CurrencyCode": "USD",
                            "TotalAmount": "99.50",
                        },
                    }
                ]
            }

    monkeypatch.setattr(
        "arc_core.services.glacier_billing._create_cost_explorer_client",
        lambda config: (_ for _ in ()).throw(RuntimeError("ce unavailable")),
    )
    monkeypatch.setattr(
        "arc_core.services.glacier_billing._create_sts_client",
        lambda config: _FakeStsClient(),
    )
    monkeypatch.setattr(
        "arc_core.services.glacier_billing._create_invoicing_client",
        lambda config: _FakeInvoicingClient(),
    )
    monkeypatch.setattr(
        "arc_core.services.glacier_billing._resolve_billing_exports",
        lambda config: None,
    )

    summary = resolve_glacier_billing(
        _config(
            tmp_path,
            glacier_endpoint_url="https://s3.us-west-2.amazonaws.com",
            glacier_backend="aws",
        ),
        include=True,
    )

    assert summary is not None
    assert summary.invoices is not None
    assert summary.invoices.source == "aws_invoicing"
    assert summary.invoices.account_id == "123456789012"
    assert summary.invoices.invoices[0].invoice_id == "INV-001"
    assert summary.invoices.invoices[0].base_total_amount == 99.5
