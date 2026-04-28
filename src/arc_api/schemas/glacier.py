from __future__ import annotations

from typing import Literal

from arc_api.schemas.archive import GlacierArchiveOut
from arc_api.schemas.common import ArcModel


class GlacierPricingBasisOut(ArcModel):
    label: str
    source: str
    storage_class: str
    glacier_storage_rate_usd_per_gib_month: float
    standard_storage_rate_usd_per_gib_month: float
    archived_metadata_bytes_per_object: int
    standard_metadata_bytes_per_object: int
    minimum_storage_duration_days: int
    currency_code: str | None = None
    region_code: str | None = None
    effective_at: str | None = None
    price_list_arn: str | None = None


class GlacierUsageTotalsOut(ArcModel):
    images: int
    uploaded_images: int
    measured_storage_bytes: int
    estimated_billable_bytes: int
    estimated_monthly_cost_usd: float


class GlacierUsageImageOut(ArcModel):
    id: str
    filename: str
    collection_ids: list[str]
    glacier: GlacierArchiveOut
    measured_storage_bytes: int
    estimated_billable_bytes: int
    estimated_monthly_cost_usd: float


class GlacierCollectionContributionOut(ArcModel):
    image_id: str
    filename: str
    glacier: GlacierArchiveOut
    represented_bytes: int
    represented_fraction: float | None
    derived_stored_bytes: int | None
    derived_billable_bytes: int | None
    estimated_monthly_cost_usd: float | None


class GlacierUsageCollectionOut(ArcModel):
    id: str
    bytes: int
    represented_bytes: int
    attribution_state: Literal["derived", "unavailable"]
    derived_stored_bytes: int
    derived_billable_bytes: int
    estimated_monthly_cost_usd: float
    images: list[GlacierCollectionContributionOut]


class GlacierUsageSnapshotOut(ArcModel):
    captured_at: str
    uploaded_images: int
    measured_storage_bytes: int
    estimated_billable_bytes: int
    estimated_monthly_cost_usd: float


class GlacierBillingActualOut(ArcModel):
    start: str
    end: str
    estimated: bool
    unblended_cost_usd: float
    usage_quantity: float | None = None
    usage_unit: str | None = None


class GlacierBillingActualsViewOut(ArcModel):
    source: str
    scope: str
    filter_label: str | None = None
    service: str | None = None
    billing_view_arn: str | None = None
    granularity: str | None = None
    measured_at: str | None = None
    periods: list[GlacierBillingActualOut]
    notes: list[str]


class GlacierBillingForecastOut(ArcModel):
    start: str
    end: str
    mean_cost_usd: float
    lower_bound_cost_usd: float | None = None
    upper_bound_cost_usd: float | None = None
    currency_code: str | None = None


class GlacierBillingForecastViewOut(ArcModel):
    source: str
    scope: str
    filter_label: str | None = None
    service: str | None = None
    currency_code: str | None = None
    granularity: str | None = None
    periods: list[GlacierBillingForecastOut]
    notes: list[str]


class GlacierBillingExportBreakdownOut(ArcModel):
    usage_type: str | None = None
    operation: str | None = None
    resource_id: str | None = None
    tag_value: str | None = None
    unblended_cost_usd: float
    usage_quantity: float | None = None
    usage_unit: str | None = None


class GlacierBillingExportViewOut(ArcModel):
    source: str
    scope: str
    filter_label: str | None = None
    service: str | None = None
    export_arn: str | None = None
    export_name: str | None = None
    execution_id: str | None = None
    manifest_key: str | None = None
    billing_period: str | None = None
    bucket: str | None = None
    prefix: str | None = None
    object_key: str | None = None
    exported_at: str | None = None
    currency_code: str | None = None
    files_read: int
    rows_scanned: int
    breakdowns: list[GlacierBillingExportBreakdownOut]
    notes: list[str]


class GlacierBillingInvoiceSummaryOut(ArcModel):
    invoice_id: str | None = None
    account_id: str | None = None
    billing_period_start: str | None = None
    billing_period_end: str | None = None
    invoice_type: str | None = None
    invoicing_entity: str | None = None
    issued_at: str | None = None
    due_at: str | None = None
    base_currency_code: str | None = None
    base_total_amount: float | None = None
    payment_currency_code: str | None = None
    payment_total_amount: float | None = None
    original_invoice_id: str | None = None


class GlacierBillingInvoicesViewOut(ArcModel):
    source: str
    scope: str
    account_id: str | None = None
    invoices: list[GlacierBillingInvoiceSummaryOut]
    notes: list[str]


class GlacierBillingSummaryOut(ArcModel):
    actuals: GlacierBillingActualsViewOut | None = None
    forecast: GlacierBillingForecastViewOut | None = None
    exports: GlacierBillingExportViewOut | None = None
    invoices: GlacierBillingInvoicesViewOut | None = None
    notes: list[str]


class GlacierUsageReportOut(ArcModel):
    scope: Literal["all", "image", "collection", "filtered"]
    measured_at: str
    pricing_basis: GlacierPricingBasisOut
    totals: GlacierUsageTotalsOut
    images: list[GlacierUsageImageOut]
    collections: list[GlacierUsageCollectionOut]
    history: list[GlacierUsageSnapshotOut]
    billing: GlacierBillingSummaryOut | None = None
