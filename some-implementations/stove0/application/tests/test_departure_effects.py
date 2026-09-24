"""Catalog departure effects keep last-known authority without payload access."""

from __future__ import annotations

from typing import Any, cast

import pytest
from riverhog_client import ApiClient
from riverhog_protocol import (
    CatalogSyncChangePage,
    CatalogSyncCheckpoint,
    CatalogSyncCollectionPage,
    CatalogSyncDeparture,
    CatalogSyncDescriptor,
    CatalogSyncUpsert,
)
from riverhog_protocol.errors import CatalogSyncViewChanged
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from stove0_core import DepartureEffectService, SqlAlchemyStateStore
from stove0_core.persistence import _DepartureEffectRow, _DeparturePolicyRow
from stove0_operator_contracts import DepartureCatalog, DeparturePolicy
from stove0_target_protocol import (
    DepartureEffectIntent,
    DepartureEffectReceipt,
    DepartureEffectReceiptPayload,
)
from time_formats import utc_timestamp_now


def _descriptor(*, collection_id: int = 7, revision: str = "1") -> CatalogSyncDescriptor:
    return CatalogSyncDescriptor(
        collection_id=str(collection_id),
        archive_root_sha256="1" * 64,
        content_identity="2" * 64,
        description=None,
        description_revision=0,
        description_identity="3" * 64,
        tag_revision=1,
        tag_set_identity="4" * 64,
        revision=revision,
    )


class _CatalogApi:
    def __init__(self, descriptor: CatalogSyncDescriptor) -> None:
        self.descriptor = descriptor
        self.source_identity = "5" * 64
        self.view_identity = "6" * 64
        self.pages: dict[str, tuple[CatalogSyncUpsert | CatalogSyncDeparture, str]] = {}
        self.membership_calls = 0

    def create_catalog_sync_checkpoint(self) -> CatalogSyncCheckpoint:
        return CatalogSyncCheckpoint(
            source_identity=self.source_identity,
            authorization_view_identity=self.view_identity,
            catalog_cursor="baseline",
        )

    def list_catalog_sync_collections(
        self, cursor: str, *, limit: int
    ) -> CatalogSyncCollectionPage:
        assert (cursor, limit) == ("baseline", 100)
        return CatalogSyncCollectionPage(
            source_identity=self.source_identity,
            authorization_view_identity=self.view_identity,
            collections=[self.descriptor],
            changes_cursor="c0",
        )

    def list_catalog_sync_changes(self, cursor: str, *, limit: int) -> CatalogSyncChangePage:
        assert limit == 1
        item = self.pages.get(cursor)
        return CatalogSyncChangePage(
            source_identity=self.source_identity,
            authorization_view_identity=self.view_identity,
            changes=[] if item is None else [item[0]],
            next_cursor=cursor if item is None else item[1],
            caught_up=item is None,
            through_revision="1" if item is None else item[0].revision,
        )

    def collection_contains_tag(self, *args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        self.membership_calls += 1
        raise AssertionError("all-visible departure policy must not query tags")


class _Target:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.receipts: dict[str, DepartureEffectReceipt] = {}
        self.fail_next = False

    def put_effect(
        self, registration_id: str, intent: DepartureEffectIntent
    ) -> DepartureEffectReceipt:
        assert registration_id == "index"
        self.calls.append(intent.departure_id)
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("index is temporarily unavailable")
        return self.receipts.setdefault(
            intent.departure_id,
            DepartureEffectReceipt.seal(
                DepartureEffectReceiptPayload(
                    departure_id=intent.departure_id,
                    target_identity="7" * 64,
                    result={"action": "withdrawn"},
                )
            ),
        )


def _service(
    *,
    state: SqlAlchemyStateStore,
    api: _CatalogApi,
    target: _Target,
    revision: int = 1,
) -> DepartureEffectService:
    return DepartureEffectService(
        catalog=DepartureCatalog(
            policies=(
                DeparturePolicy(
                    id="withdraw-index",
                    revision=revision,
                    selector={"kind": "all"},
                    target_registration_id="index",
                    target_identity="7" * 64,
                ),
            )
        ),
        riverhog=cast(ApiClient, api),
        state=state,
        targets=cast(Any, target),
    )


def _state() -> SqlAlchemyStateStore:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    return SqlAlchemyStateStore("sqlite+pysqlite:///:memory:", engine=engine)


def _generation(state: SqlAlchemyStateStore, policy_id: str) -> str:
    with state.sessions() as session:
        row = session.get(_DeparturePolicyRow, policy_id)
        assert row is not None
        return row.generation


def test_departure_effect_is_sealed_retried_and_replayed_by_identity() -> None:
    state = _state()
    first = _descriptor()
    api = _CatalogApi(first)
    target = _Target()
    service = _service(state=state, api=api, target=target)
    assert service.advance(limit=4).failures == ()
    assert service.list_effects()["effects"] == ()
    assert api.membership_calls == 0

    departure = CatalogSyncDeparture(cause="visibility_lost", collection_id="7", revision="2")
    api.pages["c0"] = (departure, "c1")
    target.fail_next = True
    run = service.advance(limit=4)
    assert len(run.failures) == 1
    effects = cast(tuple[Any, ...], service.list_effects()["effects"])
    assert len(effects) == 1
    pending = effects[0]
    assert pending.state == "pending"
    assert pending.attempt_count == 1
    assert "temporarily unavailable" in pending.failure
    assert pending.intent.last_collection == first
    assert pending.intent.departure_revision == "2"
    assert pending.intent.departure_cause == "visibility_lost"
    assert pending.intent.source_identity == api.source_identity
    assert pending.intent.authorization_view_identity == api.view_identity
    assert "inputs" not in pending.intent.model_dump(mode="json")
    assert "artifacts" not in pending.intent.model_dump(mode="json")

    with state.sessions() as session, session.begin():
        row = session.get(_DepartureEffectRow, pending.intent.departure_id)
        assert row is not None
        row.next_attempt_at = utc_timestamp_now()
    restarted = _service(state=state, api=api, target=target)
    assert restarted.advance(limit=4).failures == ()
    completed = restarted.get_effect(pending.intent.departure_id)
    assert completed.state == "complete"
    assert completed.receipt == target.receipts[pending.intent.departure_id]
    assert target.calls == [pending.intent.departure_id, pending.intent.departure_id]
    restarted.advance(limit=4)
    assert target.calls == [pending.intent.departure_id, pending.intent.departure_id]


def test_reappearance_and_later_departure_produce_distinct_effects() -> None:
    state = _state()
    first = _descriptor()
    api = _CatalogApi(first)
    target = _Target()
    service = _service(state=state, api=api, target=target)
    service.advance(limit=4)
    api.pages["c0"] = (
        CatalogSyncDeparture(cause="collection_deleted", collection_id="7", revision="2"),
        "c1",
    )
    second = _descriptor(revision="3")
    api.pages["c1"] = (CatalogSyncUpsert(**second.model_dump()), "c2")
    api.pages["c2"] = (
        CatalogSyncDeparture(cause="visibility_lost", collection_id="7", revision="4"),
        "c3",
    )
    assert service.advance(limit=12).failures == ()
    effects = cast(tuple[Any, ...], service.list_effects()["effects"])
    assert len(effects) == 2
    assert {item.intent.departure_revision for item in effects} == {"2", "4"}
    assert {item.intent.departure_cause for item in effects} == {
        "collection_deleted",
        "visibility_lost",
    }
    assert all(item.state == "complete" for item in effects)
    assert len(set(target.calls)) == 2


def test_duplicate_departure_is_exact_and_conflicting_replay_fails_closed() -> None:
    state = _state()
    api = _CatalogApi(_descriptor())
    service = _service(state=state, api=api, target=_Target())
    service.advance(limit=4)
    policy = service.catalog.policies[0]
    departure = CatalogSyncDeparture(cause="visibility_lost", collection_id="7", revision="2")
    page = CatalogSyncChangePage(
        source_identity=api.source_identity,
        authorization_view_identity=api.view_identity,
        changes=[departure],
        next_cursor="c1",
        caught_up=True,
        through_revision="2",
    )
    assert service._commit_change_page(  # noqa: SLF001 - replay authority regression
        policy,
        cursor="c0",
        page=page,
        evaluated=None,
        expected_generation=_generation(state, policy.id),
    )
    assert not service._commit_change_page(  # noqa: SLF001 - replay authority regression
        policy,
        cursor="c0",
        page=page,
        evaluated=None,
        expected_generation=_generation(state, policy.id),
    )
    conflict = CatalogSyncUpsert(**_descriptor(revision="2").model_dump())
    conflicting_page = page.model_copy(update={"changes": [conflict]})
    with pytest.raises(RuntimeError, match="replay differs"):
        service._commit_change_page(  # noqa: SLF001 - replay authority regression
            policy,
            cursor="c0",
            page=conflicting_page,
            evaluated=True,
            expected_generation=_generation(state, policy.id),
        )
    changed_cause = CatalogSyncDeparture(
        cause="collection_deleted", collection_id="7", revision="2"
    )
    with pytest.raises(RuntimeError, match="replay differs"):
        service._commit_change_page(  # noqa: SLF001 - replay authority regression
            policy,
            cursor="c0",
            page=page.model_copy(update={"changes": [changed_cause]}),
            evaluated=None,
            expected_generation=_generation(state, policy.id),
        )
    assert len(cast(tuple[Any, ...], service.list_effects()["effects"])) == 1


def test_view_change_requires_explicit_rebaseline_without_fabricating_departure() -> None:
    state = _state()
    api = _CatalogApi(_descriptor())
    target = _Target()
    service = _service(state=state, api=api, target=target)
    service.advance(limit=4)
    api.view_identity = "8" * 64
    run = service.advance(limit=4)
    assert len(run.failures) == 1
    assert service.policies().policies[0].phase == "reset_required"
    assert service.list_effects()["effects"] == ()
    status = service.rebaseline("withdraw-index")
    assert status.phase == "baseline"
    service.advance(limit=4)
    assert service.policies().policies[0].phase == "following"
    assert service.list_effects()["effects"] == ()
    with state.sessions() as session:
        row = session.get(_DeparturePolicyRow, "withdraw-index")
        assert row is not None
        assert row.authorization_view_identity == "8" * 64


@pytest.mark.parametrize("transition", ["reset", "rebaseline"])
def test_delayed_departure_baseline_page_cannot_cross_a_fence(
    monkeypatch: pytest.MonkeyPatch, transition: str
) -> None:
    state = _state()
    api = _CatalogApi(_descriptor())
    service = _service(state=state, api=api, target=_Target())
    policy = service.catalog.policies[0]
    assert service.advance(limit=1).failures == ()
    generation = _generation(state, policy.id)
    page = api.list_catalog_sync_collections("baseline", limit=100)

    def delayed(_cursor: str, *, limit: int) -> CatalogSyncCollectionPage:
        assert limit == 100
        if transition == "reset":
            service._require_rebaseline(  # noqa: SLF001 - delayed response race
                policy.id, generation=generation, phase="baseline", cursor="baseline"
            )
        else:
            service.rebaseline(policy.id)
        return page

    monkeypatch.setattr(api, "list_catalog_sync_collections", delayed)
    with pytest.raises(RuntimeError, match="rebaseline is required"):
        service._advance_policy(policy)  # noqa: SLF001 - delayed response race
    assert service.policies().policies[0].phase == (
        "reset_required" if transition == "reset" else "baseline"
    )
    assert service.list_effects()["effects"] == ()
    if transition == "rebaseline":
        assert _generation(state, policy.id) != generation


@pytest.mark.parametrize("transition", ["reset", "rebaseline"])
def test_delayed_departure_change_cannot_emit_effect_after_a_fence(
    monkeypatch: pytest.MonkeyPatch, transition: str
) -> None:
    state = _state()
    api = _CatalogApi(_descriptor())
    service = _service(state=state, api=api, target=_Target())
    policy = service.catalog.policies[0]
    assert service.advance(limit=4).failures == ()
    generation = _generation(state, policy.id)
    api.pages["c0"] = (
        CatalogSyncDeparture(cause="visibility_lost", collection_id="7", revision="2"),
        "c1",
    )
    page = api.list_catalog_sync_changes("c0", limit=1)

    def delayed(_cursor: str, *, limit: int) -> CatalogSyncChangePage:
        assert limit == 1
        if transition == "reset":
            service._require_rebaseline(  # noqa: SLF001 - delayed response race
                policy.id, generation=generation, phase="following", cursor="c0"
            )
        else:
            service.rebaseline(policy.id)
        return page

    monkeypatch.setattr(api, "list_catalog_sync_changes", delayed)
    with pytest.raises(RuntimeError, match="rebaseline is required"):
        service._advance_policy(policy)  # noqa: SLF001 - delayed response race
    assert service.policies().policies[0].phase == (
        "reset_required" if transition == "reset" else "baseline"
    )
    assert service.list_effects()["effects"] == ()


def test_delayed_departure_error_cannot_reset_a_new_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _state()
    api = _CatalogApi(_descriptor())
    service = _service(state=state, api=api, target=_Target())
    policy = service.catalog.policies[0]
    assert service.advance(limit=1).failures == ()
    generation = _generation(state, policy.id)

    def stale_error(_cursor: str, *, limit: int) -> CatalogSyncCollectionPage:
        assert limit == 100
        service.rebaseline(policy.id)
        raise CatalogSyncViewChanged("old view changed")

    monkeypatch.setattr(api, "list_catalog_sync_collections", stale_error)
    with pytest.raises(CatalogSyncViewChanged):
        service._advance_policy(policy)  # noqa: SLF001 - delayed error race
    assert service.policies().policies[0].phase == "baseline"
    assert _generation(state, policy.id) != generation


def test_departure_intent_and_receipt_reject_tampering() -> None:
    state = _state()
    api = _CatalogApi(_descriptor())
    service = _service(state=state, api=api, target=_Target())
    service.advance(limit=4)
    api.pages["c0"] = (
        CatalogSyncDeparture(cause="collection_deleted", collection_id="7", revision="2"),
        "c1",
    )
    service.advance(limit=1)
    effect = cast(tuple[Any, ...], service.list_effects()["effects"])[0]
    payload = effect.intent.model_dump(mode="json")
    with pytest.raises(ValueError):
        DepartureEffectIntent.model_validate({**payload, "departure_revision": "3"})
    with pytest.raises(ValueError):
        DepartureEffectIntent.model_validate({**payload, "departure_cause": "visibility_lost"})
    receipt = DepartureEffectReceipt.seal(
        DepartureEffectReceiptPayload(
            departure_id=effect.intent.departure_id,
            target_identity="7" * 64,
            result={"action": "withdrawn"},
        )
    )
    with pytest.raises(ValueError):
        DepartureEffectReceipt.model_validate(
            {**receipt.model_dump(mode="json"), "result": {"action": "deleted"}}
        )
