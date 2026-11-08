from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_cache_families import _service_family_units
from tests.unit.test_archive_payload_continuation import _two_units
from tests.unit.test_archive_payload_ingress import (
    _ingress_original_retry,
    _input_selection_interlock,
    _terminal_during_assembly,
)
from tests.unit.test_archive_payload_unit import _held_terminal, _preparation_retry
from tests.unit.test_archive_upload_invocations import _actual_begin_receiving
from tests.unit.test_upload_continuation import _retained_unit_retry

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_postgres_actual_upload_invocation_retains_original_begin_and_terminal_receiving(
    database_url,
    tmp_path,
    monkeypatch,
    kind,
    mode,
):
    _actual_begin_receiving(
        tmp_path,
        monkeypatch,
        kind,
        mode,
        database_url=database_url,
    )


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("unknown", (False, True))
def test_postgres_unit_continuation_retains_original_cache_begin_and_separate_journals(
    database_url, tmp_path, monkeypatch, mode, unknown
):
    _two_units(tmp_path, monkeypatch, mode, unknown, database_url=database_url)


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_postgres_terminal_receives_continuation_checkpoint_without_redispatch(
    database_url, tmp_path, monkeypatch, mode
):
    _two_units(tmp_path, monkeypatch, mode, False, database_url=database_url, lose_checkpoint=True)


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("refusal", ("guard", "capacity"))
def test_postgres_continuation_checks_current_guard_and_capacity_before_payload_dispatch(
    database_url, tmp_path, monkeypatch, mode, refusal
):
    _two_units(tmp_path, monkeypatch, mode, False, database_url=database_url, refuse_next=refusal)


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("committed", (False, True))
def test_postgres_retained_logical_unit_does_not_repeat_emitted_segments(
    database_url, tmp_path, monkeypatch, kind, mode, committed
):
    _retained_unit_retry(tmp_path, monkeypatch, kind, mode, committed, database_url=database_url)


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("committed", (False, True))
def test_postgres_service_payload_preserves_original_preparation_after_acknowledgement_loss(
    database_url, tmp_path, monkeypatch, kind, mode, committed
):
    _preparation_retry(tmp_path, monkeypatch, kind, mode, committed, database_url=database_url)


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("phase", ("key", "begin", "part", "complete"))
def test_postgres_service_payload_fence_retains_original_execution_and_receiving(
    database_url, tmp_path, monkeypatch, kind, mode, phase
):
    _held_terminal(tmp_path, monkeypatch, kind, mode, phase, database_url=database_url)


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_postgres_actual_payload_progresses_during_an_independent_held_key_constructor(
    database_url, tmp_path, monkeypatch, kind, mode
):
    _held_terminal(
        tmp_path,
        monkeypatch,
        kind,
        mode,
        "key",
        database_url=database_url,
        independent=True,
    )


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("unknown", (False, True))
def test_postgres_service_units_share_charged_cache_family_and_durable_receiving(
    database_url, tmp_path, monkeypatch, mode, unknown
):
    _service_family_units(tmp_path, monkeypatch, mode, unknown, database_url=database_url)


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_postgres_service_cache_closed_receipt_retries_original_acknowledgement(
    database_url, tmp_path, monkeypatch, mode
):
    _service_family_units(
        tmp_path,
        monkeypatch,
        mode,
        False,
        database_url=database_url,
        lose_transfer=True,
    )


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_postgres_terminal_child_receives_its_work_without_closing_unknown_cache_family(
    database_url, tmp_path, monkeypatch, mode
):
    _service_family_units(
        tmp_path,
        monkeypatch,
        mode,
        True,
        database_url=database_url,
        terminal_unknown=True,
    )


def test_postgres_incoming_body_is_visible_before_terminal_selection(
    database_url, tmp_path, monkeypatch
):
    _input_selection_interlock(tmp_path, monkeypatch, database_url=database_url)


def test_postgres_terminal_during_body_assembly_admits_no_fresh_provider_work(
    database_url, tmp_path, monkeypatch
):
    _terminal_during_assembly(tmp_path, monkeypatch, database_url=database_url)


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("committed", (False, True))
def test_postgres_ingress_retry_preserves_original_preparation_and_input(
    database_url, tmp_path, monkeypatch, committed, mode
):
    _ingress_original_retry(tmp_path, monkeypatch, committed, mode, database_url=database_url)


@pytest.mark.parametrize("terminal", (False, True))
@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("lease", ("2000-01-01T00:00:00Z", "2100-01-01T00:00:00Z"))
def test_postgres_original_seal_receiving_preserves_writer_lease(
    database_url, tmp_path, monkeypatch, kind, lease, terminal
):
    from tests.unit.test_upload_terminal_receiving import _receive_original_seal

    _receive_original_seal(
        tmp_path, monkeypatch, kind, "direct", lease, database_url=database_url, terminal=terminal
    )


@pytest.mark.parametrize("kind", ("pack", "segment"))
def test_postgres_original_native_seal_retains_receipt_through_closed_gate(
    database_url, tmp_path, monkeypatch, kind
):
    from tests.unit.test_original_native_archive_seal import (
        test_original_native_seal_survives_lost_reply_without_reselection_or_lease_renewal,
    )

    test_original_native_seal_survives_lost_reply_without_reselection_or_lease_renewal(
        tmp_path, monkeypatch, kind, True, True, database_url=database_url
    )
