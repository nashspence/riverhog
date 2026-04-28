from __future__ import annotations

import csv
import gzip
import io
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from arc_core.domain.models import (
    GlacierBillingActual,
    GlacierBillingActualsView,
    GlacierBillingExportBreakdown,
    GlacierBillingExportView,
    GlacierBillingForecast,
    GlacierBillingForecastView,
    GlacierBillingInvoiceSummary,
    GlacierBillingInvoicesView,
    GlacierBillingSummary,
)
from arc_core.runtime_config import RuntimeConfig
from arc_core.stores.s3_support import _require_boto3

_AWS_COST_EXPLORER_RESOURCE_SOURCE = "aws_cost_explorer_resource"
_AWS_COST_EXPLORER_SOURCE = "aws_cost_explorer"
_AWS_CUR_SOURCE = "aws_cur_s3"
_AWS_DATA_EXPORTS_SOURCE = "aws_data_exports_s3"
_AWS_INVOICING_SOURCE = "aws_invoicing"
_UNAVAILABLE_SOURCE = "unavailable"
_S3_SERVICE_NAME = "Amazon Simple Storage Service"
_RESOURCE_LOOKBACK_DAYS = 14
_SERVICE_COLUMN_CANDIDATES = (
    "line_item_product_code",
    "product_servicecode",
    "product_product_name",
    "product_ProductName",
    "service",
)
_USAGE_TYPE_COLUMN_CANDIDATES = (
    "line_item_usage_type",
    "lineItem/UsageType",
    "usage_type",
)
_OPERATION_COLUMN_CANDIDATES = (
    "line_item_operation",
    "lineItem/Operation",
    "operation",
)
_RESOURCE_ID_COLUMN_CANDIDATES = (
    "line_item_resource_id",
    "lineItem/ResourceId",
    "resource_id",
    "Resource",
)
_COST_COLUMN_CANDIDATES = (
    "line_item_unblended_cost",
    "lineItem/UnblendedCost",
    "unblended_cost",
)
_USAGE_QUANTITY_COLUMN_CANDIDATES = (
    "line_item_usage_amount",
    "lineItem/UsageAmount",
    "usage_quantity",
)
_USAGE_UNIT_COLUMN_CANDIDATES = (
    "line_item_usage_unit",
    "lineItem/UsageUnit",
    "pricing_unit",
    "usage_unit",
)


@dataclass(frozen=True)
class _CostExplorerScope:
    name: str
    expression: dict[str, object]
    label: str
    notes: tuple[str, ...]


@dataclass(frozen=True)
class _BucketActualsAttempt:
    view: GlacierBillingActualsView | None
    fallback_note: str | None = None


def resolve_glacier_billing(
    config: RuntimeConfig,
    *,
    include: bool,
) -> GlacierBillingSummary | None:
    if not include:
        return None
    if config.glacier_billing_mode == "disabled":
        return _unavailable_summary(
            config,
            reason="AWS Glacier billing queries are disabled for this runtime.",
        )
    if config.glacier_billing_mode == "auto" and not _should_try_aws_billing(config):
        return _unavailable_summary(
            config,
            reason="AWS Glacier billing is unavailable for this runtime.",
        )

    actuals: GlacierBillingActualsView
    forecast: GlacierBillingForecastView
    notes: list[str] = []
    try:
        client = _create_cost_explorer_client(config)
        bucket_attempt = _resolve_bucket_actuals(client, config=config)
        if bucket_attempt.view is not None:
            actuals = bucket_attempt.view
        else:
            actuals = _resolve_scope_actuals(
                client,
                config=config,
                fallback_note=bucket_attempt.fallback_note,
            )
        forecast = _resolve_forecast(
            client,
            config=config,
            actuals=actuals,
        )
    except Exception:
        if config.glacier_billing_mode == "aws":
            raise
        unavailable = _unavailable_summary(
            config,
            reason="AWS Cost Explorer billing could not be resolved for this runtime.",
        )
        actuals = unavailable.actuals or _unavailable_actuals_view(
            "AWS Cost Explorer actuals could not be resolved for this runtime."
        )
        forecast = unavailable.forecast or _unavailable_forecast_view(
            config,
            "AWS Cost Explorer forecast could not be resolved for this runtime.",
        )
        notes.extend(unavailable.notes)

    exports = _resolve_billing_exports(config)
    invoices = _resolve_invoices(config)
    return GlacierBillingSummary(
        actuals=actuals,
        forecast=forecast,
        exports=exports,
        invoices=invoices,
        notes=tuple(notes),
    )


def _unavailable_summary(config: RuntimeConfig, *, reason: str) -> GlacierBillingSummary:
    return GlacierBillingSummary(
        actuals=_unavailable_actuals_view(reason),
        forecast=_unavailable_forecast_view(config, reason),
        exports=_unavailable_exports_view(reason),
        invoices=_unavailable_invoices_view(reason),
        notes=(reason,),
    )


def _unavailable_actuals_view(reason: str) -> GlacierBillingActualsView:
    return GlacierBillingActualsView(
        source=_UNAVAILABLE_SOURCE,
        scope="unavailable",
        periods=(),
        notes=(reason,),
    )


def _unavailable_forecast_view(
    config: RuntimeConfig,
    reason: str,
) -> GlacierBillingForecastView:
    return GlacierBillingForecastView(
        source=_UNAVAILABLE_SOURCE,
        scope="unavailable",
        currency_code=config.glacier_billing_currency_code,
        periods=(),
        notes=(reason,),
    )


def _unavailable_exports_view(reason: str) -> GlacierBillingExportView:
    return GlacierBillingExportView(
        source=_UNAVAILABLE_SOURCE,
        scope="unavailable",
        breakdowns=(),
        notes=(reason,),
    )


def _unavailable_invoices_view(reason: str) -> GlacierBillingInvoicesView:
    return GlacierBillingInvoicesView(
        source=_UNAVAILABLE_SOURCE,
        scope="unavailable",
        invoices=(),
        notes=(reason,),
    )


def _should_try_aws_billing(config: RuntimeConfig) -> bool:
    if config.glacier_billing_mode == "aws":
        return True
    if config.glacier_backend.casefold() == "aws":
        return True
    endpoint = config.glacier_endpoint_url.casefold()
    return "amazonaws.com" in endpoint


def _billing_scope(config: RuntimeConfig) -> _CostExplorerScope:
    base_filters: list[dict[str, object]] = [
        _dimension_expression("SERVICE", [_S3_SERVICE_NAME]),
        _dimension_expression("REGION", [config.glacier_pricing_region_code]),
    ]
    if config.glacier_billing_tag_key and config.glacier_billing_tag_value:
        base_filters.append(
            {
                "Tags": {
                    "Key": config.glacier_billing_tag_key,
                    "Values": [config.glacier_billing_tag_value],
                }
            }
        )
        return _CostExplorerScope(
            name="tag",
            expression={"And": base_filters},
            label=f"{config.glacier_billing_tag_key}={config.glacier_billing_tag_value}",
            notes=(),
        )
    return _CostExplorerScope(
        name="service",
        expression={"And": base_filters},
        label=f"{_S3_SERVICE_NAME} in {config.glacier_pricing_region_code}",
        notes=(
            (
                "Forecast and fallback actuals are scoped to the Amazon S3 service "
                "in the configured Glacier region."
            ),
            (
                "Set ARC_GLACIER_BILLING_TAG_KEY and ARC_GLACIER_BILLING_TAG_VALUE "
                "for archive-specific forecast and fallback billing attribution."
            ),
        ),
    )


def _create_cost_explorer_client(config: RuntimeConfig) -> Any:
    boto3, Config = _require_boto3()
    return boto3.client(
        "ce",
        region_name=config.glacier_billing_api_region,
        config=Config(retries={"max_attempts": 3, "mode": "standard"}),
    )


def _create_billing_s3_client(config: RuntimeConfig) -> Any:
    boto3, Config = _require_boto3()
    return boto3.client(
        "s3",
        region_name=config.glacier_billing_export_region,
        config=Config(retries={"max_attempts": 3, "mode": "standard"}),
    )


def _create_invoicing_client(config: RuntimeConfig) -> Any:
    boto3, Config = _require_boto3()
    return boto3.client(
        "invoicing",
        region_name=config.glacier_billing_api_region,
        config=Config(retries={"max_attempts": 3, "mode": "standard"}),
    )


def _create_sts_client(config: RuntimeConfig) -> Any:
    boto3, Config = _require_boto3()
    return boto3.client(
        "sts",
        region_name=config.glacier_billing_api_region,
        config=Config(retries={"max_attempts": 3, "mode": "standard"}),
    )


def _resolve_bucket_actuals(client: Any, *, config: RuntimeConfig) -> _BucketActualsAttempt:
    notes = (
        "Bucket-scoped Cost Explorer actuals use AWS resource-level daily data and are limited "
        "to the last 14 days.",
    )
    start = date.today() - timedelta(days=_RESOURCE_LOOKBACK_DAYS - 1)
    end = date.today() + timedelta(days=1)
    resource_ids = (
        config.glacier_bucket,
        f"arn:aws:s3:::{config.glacier_bucket}",
    )
    for resource_id in resource_ids:
        try:
            response = client.get_cost_and_usage(
                TimePeriod={"Start": start.isoformat(), "End": end.isoformat()},
                Granularity="DAILY",
                Metrics=["UnblendedCost", "UsageQuantity"],
                Filter={
                    "And": [
                        _dimension_expression("SERVICE", [_S3_SERVICE_NAME]),
                        _dimension_expression("REGION", [config.glacier_pricing_region_code]),
                        _dimension_expression("RESOURCE_ID", [resource_id]),
                    ]
                },
            )
        except Exception as exc:
            if _resource_level_unavailable(exc):
                return _BucketActualsAttempt(
                    view=None,
                    fallback_note=(
                        "Bucket-scoped Cost Explorer actuals require AWS resource-level daily "
                        "data to be enabled and available for the archive bucket."
                    ),
                )
            continue
        periods = tuple(_map_actual_period(item) for item in response.get("ResultsByTime", []))
        if not periods:
            continue
        return _BucketActualsAttempt(
            view=GlacierBillingActualsView(
                source=_AWS_COST_EXPLORER_RESOURCE_SOURCE,
                scope="bucket",
                filter_label=config.glacier_bucket,
                service=_S3_SERVICE_NAME,
                granularity="DAILY",
                periods=periods,
                notes=notes,
            )
        )
    return _BucketActualsAttempt(
        view=None,
        fallback_note=(
            "Bucket-scoped Cost Explorer actuals were not available for the archive bucket, "
            "so Riverhog fell back to broader AWS billing attribution."
        ),
    )


def _resolve_scope_actuals(
    client: Any,
    *,
    config: RuntimeConfig,
    fallback_note: str | None,
) -> GlacierBillingActualsView:
    scope = _billing_scope(config)
    periods = _load_actual_costs(client, config=config, scope=scope)
    notes = list(scope.notes)
    if fallback_note:
        notes.insert(0, fallback_note)
    return GlacierBillingActualsView(
        source=_AWS_COST_EXPLORER_SOURCE,
        scope=scope.name,
        filter_label=scope.label,
        service=_S3_SERVICE_NAME,
        granularity="MONTHLY",
        periods=periods,
        notes=tuple(notes),
    )


def _resolve_forecast(
    client: Any,
    *,
    config: RuntimeConfig,
    actuals: GlacierBillingActualsView,
) -> GlacierBillingForecastView:
    scope = _billing_scope(config)
    periods = _load_cost_forecast(client, config=config, scope=scope)
    notes = list(scope.notes)
    if actuals.scope == "bucket":
        notes.insert(
            0,
            "AWS Cost Explorer forecast does not expose bucket-resource forecasting, "
            "so Riverhog falls back to tag-scoped or service-scoped forecast data.",
        )
    return GlacierBillingForecastView(
        source=_AWS_COST_EXPLORER_SOURCE,
        scope=scope.name,
        filter_label=scope.label,
        service=_S3_SERVICE_NAME,
        currency_code=config.glacier_billing_currency_code,
        granularity="MONTHLY",
        periods=periods,
        notes=tuple(notes),
    )


def _load_actual_costs(
    client: Any,
    *,
    config: RuntimeConfig,
    scope: _CostExplorerScope,
) -> tuple[GlacierBillingActual, ...]:
    start = _month_start(_add_months(date.today(), -(config.glacier_billing_lookback_months - 1)))
    end = _month_start(_add_months(date.today(), 1))
    response = client.get_cost_and_usage(
        TimePeriod={"Start": start.isoformat(), "End": end.isoformat()},
        Granularity="MONTHLY",
        Metrics=["UnblendedCost", "UsageQuantity"],
        Filter=scope.expression,
    )
    return tuple(_map_actual_period(item) for item in response.get("ResultsByTime", []))


def _load_cost_forecast(
    client: Any,
    *,
    config: RuntimeConfig,
    scope: _CostExplorerScope,
) -> tuple[GlacierBillingForecast, ...]:
    start = _month_start(_add_months(date.today(), 1))
    end = _month_start(_add_months(start, config.glacier_billing_forecast_months))
    response = client.get_cost_forecast(
        TimePeriod={"Start": start.isoformat(), "End": end.isoformat()},
        Granularity="MONTHLY",
        Metric="UNBLENDED_COST",
        PredictionIntervalLevel=80,
        Filter=scope.expression,
    )
    return tuple(
        _map_forecast_period(item, currency_code=config.glacier_billing_currency_code)
        for item in response.get("ForecastResultsByTime", [])
    )


def _resolve_billing_exports(config: RuntimeConfig) -> GlacierBillingExportView:
    if not config.glacier_billing_export_bucket or not config.glacier_billing_export_prefix:
        return _unavailable_exports_view(
            "Set ARC_GLACIER_BILLING_EXPORT_BUCKET and ARC_GLACIER_BILLING_EXPORT_PREFIX "
            "to inspect CUR or Data Exports billing detail."
        )

    try:
        client = _create_billing_s3_client(config)
        latest = _latest_export_object(client, config=config)
        if latest is None:
            return _unavailable_exports_view(
                "No CUR or Data Exports billing object was found under the configured S3 prefix."
            )
        key, exported_at = latest
        body = client.get_object(
            Bucket=config.glacier_billing_export_bucket,
            Key=key,
        )["Body"].read()
        text = _decode_export_object(key, body)
        rows_scanned, breakdowns, source = _parse_export_breakdowns(text, config=config)
    except Exception:
        return _unavailable_exports_view(
            "CUR or Data Exports billing detail could not be resolved for this runtime."
        )

    filter_label = (
        f"{config.glacier_billing_tag_key}={config.glacier_billing_tag_value}"
        if config.glacier_billing_tag_key and config.glacier_billing_tag_value
        else config.glacier_bucket
    )
    scope = (
        "tag"
        if config.glacier_billing_tag_key and config.glacier_billing_tag_value
        else "bucket"
    )
    if not breakdowns:
        return GlacierBillingExportView(
            source=_UNAVAILABLE_SOURCE,
            scope=scope,
            filter_label=filter_label,
            service=_S3_SERVICE_NAME,
            bucket=config.glacier_billing_export_bucket,
            prefix=config.glacier_billing_export_prefix,
            object_key=key,
            exported_at=exported_at,
            currency_code=config.glacier_billing_currency_code,
            rows_scanned=rows_scanned,
            breakdowns=(),
            notes=(
                (
                    "The configured CUR or Data Exports object did not contain "
                    "archive-matching rows. Use cost-allocation tags or resource IDs "
                    "in the export to make Glacier drill-down precise."
                ),
            ),
        )
    return GlacierBillingExportView(
        source=source,
        scope=scope,
        filter_label=filter_label,
        service=_S3_SERVICE_NAME,
        bucket=config.glacier_billing_export_bucket,
        prefix=config.glacier_billing_export_prefix,
        object_key=key,
        exported_at=exported_at,
        currency_code=config.glacier_billing_currency_code,
        rows_scanned=rows_scanned,
        breakdowns=breakdowns,
        notes=(),
    )


def _resolve_invoices(config: RuntimeConfig) -> GlacierBillingInvoicesView:
    try:
        account_id = config.glacier_billing_invoice_account_id or _resolve_account_id(config)
    except Exception:
        return _unavailable_invoices_view(
            "AWS account identity is unavailable for invoice summary lookup."
        )

    try:
        client = _create_invoicing_client(config)
        response = client.list_invoice_summaries(
            Selector={"ResourceType": "ACCOUNT_ID", "Value": account_id},
            MaxResults=config.glacier_billing_invoice_max_items,
        )
    except Exception:
        return _unavailable_invoices_view(
            "AWS invoice summaries could not be resolved for this runtime."
        )

    return GlacierBillingInvoicesView(
        source=_AWS_INVOICING_SOURCE,
        scope="account",
        account_id=account_id,
        invoices=tuple(
            _map_invoice_summary(item) for item in response.get("InvoiceSummaries", [])
        ),
        notes=(
            (
                "AWS invoice summaries are account-level totals and do not "
                "attribute cost to a single Glacier bucket."
            ),
        ),
    )


def _resolve_account_id(config: RuntimeConfig) -> str:
    response = _create_sts_client(config).get_caller_identity()
    account = response.get("Account")
    if account in (None, ""):
        raise RuntimeError("missing AWS account id")
    return str(account)


def _latest_export_object(client: Any, *, config: RuntimeConfig) -> tuple[str, str] | None:
    paginator = client.get_paginator("list_objects_v2")
    latest: tuple[str, datetime] | None = None
    for page in paginator.paginate(
        Bucket=config.glacier_billing_export_bucket,
        Prefix=config.glacier_billing_export_prefix,
    ):
        for entry in page.get("Contents", []):
            key = entry.get("Key")
            modified = entry.get("LastModified")
            if not isinstance(key, str) or modified is None or not _supported_export_key(key):
                continue
            if latest is None or modified > latest[1]:
                latest = (key, modified)
    if latest is None:
        return None
    return latest[0], _datetime_to_utc_iso(latest[1])


def _supported_export_key(key: str) -> bool:
    normalized = key.casefold()
    return (
        normalized.endswith(".csv")
        or normalized.endswith(".csv.gz")
        or normalized.endswith(".gz")
    )


def _decode_export_object(key: str, payload: bytes) -> str:
    if key.casefold().endswith(".gz"):
        payload = gzip.decompress(payload)
    return payload.decode("utf-8-sig")


def _parse_export_breakdowns(
    payload: str,
    *,
    config: RuntimeConfig,
) -> tuple[int, tuple[GlacierBillingExportBreakdown, ...], str]:
    reader = csv.DictReader(io.StringIO(payload))
    rows_scanned = 0
    source = _AWS_DATA_EXPORTS_SOURCE
    aggregates: dict[tuple[str | None, str | None, str | None, str | None], dict[str, object]] = {}
    for row in reader:
        rows_scanned += 1
        if not row:
            continue
        if any(name.startswith("line_item_") for name in row):
            source = _AWS_CUR_SOURCE
        if not _row_matches_archive_scope(row, config=config):
            continue
        usage_type = _row_value(row, _USAGE_TYPE_COLUMN_CANDIDATES)
        operation = _row_value(row, _OPERATION_COLUMN_CANDIDATES)
        resource_id = _row_value(row, _RESOURCE_ID_COLUMN_CANDIDATES)
        tag_value = _row_tag_value(row, config=config)
        key = (usage_type, operation, resource_id, tag_value)
        aggregate = aggregates.setdefault(
            key,
            {
                "cost": Decimal("0"),
                "quantity": Decimal("0"),
                "unit": None,
                "quantity_seen": False,
            },
        )
        aggregate["cost"] = Decimal(str(aggregate["cost"])) + _row_decimal(
            row, _COST_COLUMN_CANDIDATES
        )
        quantity = _row_decimal(row, _USAGE_QUANTITY_COLUMN_CANDIDATES)
        if quantity != Decimal("0"):
            aggregate["quantity"] = Decimal(str(aggregate["quantity"])) + quantity
            aggregate["quantity_seen"] = True
        unit = _row_value(row, _USAGE_UNIT_COLUMN_CANDIDATES)
        current_unit = aggregate["unit"]
        if current_unit in (None, ""):
            aggregate["unit"] = unit
        elif unit not in (None, "", current_unit):
            aggregate["unit"] = "mixed"

    breakdowns = [
        GlacierBillingExportBreakdown(
            usage_type=usage_type,
            operation=operation,
            resource_id=resource_id,
            tag_value=tag_value,
            unblended_cost_usd=float(values["cost"]),
            usage_quantity=float(values["quantity"]) if values["quantity_seen"] else None,
            usage_unit=str(values["unit"]) if values["unit"] not in (None, "") else None,
        )
        for (usage_type, operation, resource_id, tag_value), values in aggregates.items()
    ]
    breakdowns.sort(key=lambda current: current.unblended_cost_usd, reverse=True)
    return rows_scanned, tuple(breakdowns[: config.glacier_billing_export_max_items]), source


def _row_matches_archive_scope(row: dict[str, str], *, config: RuntimeConfig) -> bool:
    service = (_row_value(row, _SERVICE_COLUMN_CANDIDATES) or "").casefold()
    if service and "s3" not in service and "simple storage" not in service:
        return False
    if config.glacier_billing_tag_key and config.glacier_billing_tag_value:
        return _row_tag_value(row, config=config) == config.glacier_billing_tag_value
    resource_id = _row_value(row, _RESOURCE_ID_COLUMN_CANDIDATES)
    if resource_id is None:
        return False
    return resource_id in {config.glacier_bucket, f"arn:aws:s3:::{config.glacier_bucket}"}


def _row_tag_value(row: dict[str, str], *, config: RuntimeConfig) -> str | None:
    if not config.glacier_billing_tag_key:
        return None
    sanitized = config.glacier_billing_tag_key.replace("-", "_").replace(":", "_")
    candidates = (
        f"resource_tags_user_{config.glacier_billing_tag_key}",
        f"resource_tags_user_{sanitized}",
        f"resourceTags/user:{config.glacier_billing_tag_key}",
        f"resource_tags/{config.glacier_billing_tag_key}",
    )
    return _row_value(row, candidates)


def _row_value(row: dict[str, str], candidates: tuple[str, ...]) -> str | None:
    for name in candidates:
        value = row.get(name)
        if value not in (None, ""):
            return str(value)
    lowered = {key.casefold(): value for key, value in row.items()}
    for name in candidates:
        value = lowered.get(name.casefold())
        if value not in (None, ""):
            return str(value)
    return None


def _row_decimal(row: dict[str, str], candidates: tuple[str, ...]) -> Decimal:
    value = _row_value(row, candidates)
    if value in (None, ""):
        return Decimal("0")
    return Decimal(value)


def _resource_level_unavailable(exc: Exception) -> bool:
    normalized = f"{exc.__class__.__name__}: {exc}".casefold()
    return (
        "dataunavailable" in normalized
        or "resource-level" in normalized
        or "resource id" in normalized
        or "14 days" in normalized
    )


def _map_actual_period(payload: dict[str, object]) -> GlacierBillingActual:
    metrics = payload.get("Total", {})
    if not isinstance(metrics, dict):
        metrics = {}
    cost_metric = metrics.get("UnblendedCost", {})
    usage_metric = metrics.get("UsageQuantity", {})
    cost_amount = _metric_amount(cost_metric)
    usage_amount = _metric_amount(usage_metric)
    usage_unit = _metric_unit(usage_metric)
    time_period = payload.get("TimePeriod", {})
    if not isinstance(time_period, dict):
        time_period = {}
    return GlacierBillingActual(
        start=str(time_period.get("Start", "")),
        end=str(time_period.get("End", "")),
        estimated=bool(payload.get("Estimated", False)),
        unblended_cost_usd=cost_amount,
        usage_quantity=usage_amount,
        usage_unit=usage_unit,
    )


def _map_forecast_period(
    payload: dict[str, object],
    *,
    currency_code: str,
) -> GlacierBillingForecast:
    time_period = payload.get("TimePeriod", {})
    if not isinstance(time_period, dict):
        time_period = {}
    return GlacierBillingForecast(
        start=str(time_period.get("Start", "")),
        end=str(time_period.get("End", "")),
        mean_cost_usd=_decimal_to_float(payload.get("MeanValue")),
        lower_bound_cost_usd=_optional_decimal_to_float(payload.get("PredictionIntervalLowerBound")),
        upper_bound_cost_usd=_optional_decimal_to_float(payload.get("PredictionIntervalUpperBound")),
        currency_code=currency_code,
    )


def _map_invoice_summary(payload: dict[str, object]) -> GlacierBillingInvoiceSummary:
    billing_period = payload.get("BillingPeriod", {})
    if not isinstance(billing_period, dict):
        billing_period = {}
    month = int(billing_period.get("Month", 1) or 1)
    year = int(billing_period.get("Year", 1970) or 1970)
    start = date(year, month, 1)
    end = _add_months(start, 1)
    entity = payload.get("Entity", {})
    if not isinstance(entity, dict):
        entity = {}
    base_amount = payload.get("BaseCurrencyAmount", {})
    if not isinstance(base_amount, dict):
        base_amount = {}
    payment_amount = payload.get("PaymentCurrencyAmount", {})
    if not isinstance(payment_amount, dict):
        payment_amount = {}
    return GlacierBillingInvoiceSummary(
        invoice_id=_optional_str(payload.get("InvoiceId")),
        account_id=_optional_str(payload.get("AccountId")),
        billing_period_start=start.isoformat(),
        billing_period_end=end.isoformat(),
        invoice_type=_optional_str(payload.get("InvoiceType")),
        invoicing_entity=_optional_str(entity.get("InvoicingEntity")),
        issued_at=_timestamp_to_iso(payload.get("IssuedDate")),
        due_at=_timestamp_to_iso(payload.get("DueDate")),
        base_currency_code=_optional_str(base_amount.get("CurrencyCode")),
        base_total_amount=_optional_decimal_to_float(base_amount.get("TotalAmount")),
        payment_currency_code=_optional_str(payment_amount.get("CurrencyCode")),
        payment_total_amount=_optional_decimal_to_float(payment_amount.get("TotalAmount")),
        original_invoice_id=_optional_str(payload.get("OriginalInvoiceId")),
    )


def _metric_amount(metric: object) -> float:
    if not isinstance(metric, dict):
        return 0.0
    amount = metric.get("Amount")
    if amount in (None, ""):
        return 0.0
    return _decimal_to_float(amount)


def _metric_unit(metric: object) -> str | None:
    if not isinstance(metric, dict):
        return None
    unit = metric.get("Unit")
    return str(unit) if unit not in (None, "") else None


def _dimension_expression(key: str, values: list[str]) -> dict[str, object]:
    return {"Dimensions": {"Key": key, "Values": values}}


def _month_start(value: date) -> date:
    return value.replace(day=1)


def _add_months(value: date, months: int) -> date:
    month_index = (value.year * 12 + (value.month - 1)) + months
    year = month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def _timestamp_to_iso(value: object) -> str | None:
    if value in (None, ""):
        return None
    timestamp = float(Decimal(str(value)))
    if timestamp > 10_000_000_000:
        timestamp /= 1000.0
    return datetime.fromtimestamp(timestamp, tz=UTC).isoformat().replace("+00:00", "Z")


def _datetime_to_utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _optional_str(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _decimal_to_float(value: object) -> float:
    return float(Decimal(str(value)))


def _optional_decimal_to_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return _decimal_to_float(value)
