"""Durable, artifact-free effects for collections leaving a Riverhog catalog view."""

from __future__ import annotations

import json
import secrets
from datetime import timedelta
from typing import Any, Literal, Protocol, cast

from riverhog_canonical_json import canonical_json_sha256
from riverhog_client import ApiClient
from riverhog_protocol import (
    CatalogSyncChangePage,
    CatalogSyncCheckpoint,
    CatalogSyncCollectionPage,
    CatalogSyncDeparture,
    CatalogSyncDescriptor,
    CatalogSyncUpsert,
)
from riverhog_protocol.errors import RiverhogError
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from stove0_operator_contracts import (
    DepartureCatalog,
    DepartureEffectState,
    DepartureEffectView,
    DeparturePhase,
    DeparturePolicy,
    DeparturePolicyCatalogView,
    DeparturePolicyStatus,
    DepartureRun,
    SchedulerFailure,
)
from stove0_target_client import DepartureEffectClient
from stove0_target_protocol import (
    DepartureEffectIntent,
    DepartureEffectIntentPayload,
    DepartureEffectReceipt,
)
from time_formats import format_utc_timestamp, utc_now, utc_timestamp_now

from stove0_core.catalog_predicate import catalog_selector_matches
from stove0_core.persistence import (
    SqlAlchemyStateStore,
    _DepartureEffectRow,
    _DeparturePolicyRow,
    _DepartureSeenRow,
)

_POLICY_CURSOR = "stove0:departure-policy-scan/v1"
_LANE_CURSOR = "stove0:departure-lane/v1"
_RESET_ERRORS = frozenset(
    {
        "catalog_sync_cursor_expired",
        "catalog_sync_history_expired",
        "catalog_sync_source_changed",
        "catalog_sync_view_changed",
        "precondition_failed",
        "forbidden",
        "unauthorized",
    }
)


class DepartureTargetPort(Protocol):
    def put_effect(
        self, registration_id: str, intent: DepartureEffectIntent
    ) -> DepartureEffectReceipt: ...


class HttpDepartureTargetPort:
    def __init__(self, registrations: dict[str, DepartureEffectClient]) -> None:
        self._registrations = dict(registrations)

    def put_effect(
        self, registration_id: str, intent: DepartureEffectIntent
    ) -> DepartureEffectReceipt:
        try:
            target = self._registrations[registration_id]
        except KeyError as exc:
            raise KeyError(f"unknown departure target registration: {registration_id}") from exc
        return target.put_effect(intent)

    def has_registration(self, registration_id: str) -> bool:
        return registration_id in self._registrations


class _AuthorityChanged(RuntimeError):
    pass


class DepartureEffectService:
    def __init__(
        self,
        *,
        catalog: DepartureCatalog,
        riverhog: ApiClient,
        state: SqlAlchemyStateStore,
        targets: DepartureTargetPort,
    ) -> None:
        self.catalog = catalog
        self.riverhog = riverhog
        self.state = state
        self.targets = targets
        self._policies = {policy.id: policy for policy in catalog.policies}
        for policy in catalog.policies:
            if isinstance(targets, HttpDepartureTargetPort) and not targets.has_registration(
                policy.target_registration_id
            ):
                raise ValueError(f"departure policy {policy.id} names an unconfigured target")
        self._synchronize_policy_rows()

    def advance(self, *, limit: int = 25) -> DepartureRun:
        if isinstance(limit, bool) or not 1 <= limit <= 100:
            raise ValueError("departure advancement limit is outside the v1 bound")
        progressed: list[str] = []
        failures: list[SchedulerFailure] = []
        saved = self.state.load_cursor(_LANE_CURSOR)
        lane, lane_revision = saved if saved is not None else ("effect", None)
        if lane not in {"effect", "policy"}:
            raise RuntimeError("departure lane cursor is invalid")
        for _ in range(limit):
            performed = False
            for selected in (lane, "policy" if lane == "effect" else "effect"):
                if selected == "effect":
                    pending = self._pending_effect_ids(limit=1)
                    if not pending:
                        continue
                    departure_id = pending[0]
                    try:
                        self._advance_effect(departure_id)
                        progressed.append(f"effect:{departure_id}")
                    except Exception as exc:
                        self._record_effect_failure(departure_id, exc)
                        failures.append(
                            SchedulerFailure(
                                event_id=f"departure-effect:{departure_id}",
                                error=f"{type(exc).__name__}: {exc}"[:1000],
                            )
                        )
                    lane = "policy"
                    performed = True
                    break
                policy, revision = self._next_policy()
                if policy is None:
                    continue
                try:
                    advanced = self._advance_policy(policy)
                    if advanced:
                        progressed.append(f"policy:{policy.id}")
                except Exception as exc:
                    failures.append(
                        SchedulerFailure(
                            event_id=f"departure-policy:{policy.id}",
                            error=f"{type(exc).__name__}: {exc}"[:1000],
                        )
                    )
                    advanced = True
                self._store_cursor(_POLICY_CURSOR, revision, policy.id)
                if not advanced:
                    continue
                lane = "effect"
                performed = True
                break
            if not performed:
                break
        self._store_cursor(_LANE_CURSOR, lane_revision, lane)
        return DepartureRun(progressed=tuple(progressed), failures=tuple(failures))

    def policies(self) -> DeparturePolicyCatalogView:
        with self.state.sessions() as session:
            statuses = []
            for policy in self.catalog.policies:
                row = session.get(_DeparturePolicyRow, policy.id)
                if row is None:
                    raise RuntimeError("configured departure policy state is unavailable")
                statuses.append(_policy_status(policy, row))
        return DeparturePolicyCatalogView(
            catalog_sha256=self.catalog.catalog_sha256, policies=tuple(statuses)
        )

    def rebaseline(self, policy_id: str) -> DeparturePolicyStatus:
        policy = self._policy(policy_id)
        checkpoint = self.riverhog.create_catalog_sync_checkpoint()
        with self.state.sessions() as session, session.begin():
            row = session.get(_DeparturePolicyRow, policy.id, with_for_update=True)
            if row is None or row.policy_sha256 != policy.policy_sha256:
                raise RuntimeError("departure policy state differs from configuration")
            session.execute(
                delete(_DepartureSeenRow).where(_DepartureSeenRow.policy_id == policy.id)
            )
            _bind_checkpoint(row, checkpoint)
            return _policy_status(policy, row)

    def list_effects(
        self, *, page_size: int = 100, after_id: str | None = None
    ) -> dict[str, object]:
        if isinstance(page_size, bool) or not 1 <= page_size <= 100:
            raise ValueError("departure effect page size is outside the v1 bound")
        with self.state.sessions() as session:
            statement = select(_DepartureEffectRow).order_by(_DepartureEffectRow.departure_id)
            if after_id is not None:
                statement = statement.where(_DepartureEffectRow.departure_id > after_id)
            rows = list(session.scalars(statement.limit(page_size + 1)))
            visible = rows[:page_size]
            next_after = visible[-1].departure_id if len(rows) > page_size else None
            return {
                "page_size": page_size,
                "effects": tuple(_effect_view(row) for row in visible),
                "_next_position": None if next_after is None else (next_after,),
            }

    def get_effect(self, departure_id: str) -> DepartureEffectView:
        with self.state.sessions() as session:
            row = session.get(_DepartureEffectRow, departure_id)
            if row is None:
                raise KeyError(departure_id)
            return _effect_view(row)

    def _synchronize_policy_rows(self) -> None:
        now = utc_timestamp_now()
        with self.state.sessions() as session, session.begin():
            dialect = session.get_bind().dialect.name
            insert_policy: Any
            if dialect == "postgresql":
                insert_policy = postgresql_insert
            elif dialect == "sqlite":
                insert_policy = sqlite_insert
            else:
                raise RuntimeError(f"unsupported Stove0 state-store dialect: {dialect}")
            for policy in self.catalog.policies:
                session.execute(
                    insert_policy(_DeparturePolicyRow)
                    .values(
                        policy_id=policy.id,
                        policy_revision=policy.revision,
                        policy_sha256=policy.policy_sha256,
                        phase="new",
                        generation=secrets.token_hex(32),
                        source_identity=None,
                        authorization_view_identity=None,
                        cursor=None,
                        through_revision="0",
                        updated_at=now,
                    )
                    .on_conflict_do_nothing(index_elements=[_DeparturePolicyRow.policy_id])
                )
                row = session.get(_DeparturePolicyRow, policy.id, with_for_update=True)
                if row is None:
                    raise RuntimeError("departure policy state is unavailable")
                if row.policy_sha256 == policy.policy_sha256:
                    continue
                if policy.revision <= row.policy_revision:
                    raise ValueError(
                        f"departure policy {policy.id} changed without a higher revision"
                    )
                row.policy_revision = policy.revision
                row.policy_sha256 = policy.policy_sha256
                session.execute(
                    delete(_DepartureSeenRow).where(_DepartureSeenRow.policy_id == policy.id)
                )
                row.phase = "reset_required"
                row.generation = secrets.token_hex(32)
                row.source_identity = None
                row.authorization_view_identity = None
                row.cursor = None
                row.through_revision = "0"
                row.updated_at = now

    def _next_policy(self) -> tuple[DeparturePolicy | None, int | None]:
        saved = self.state.load_cursor(_POLICY_CURSOR)
        cursor, revision = saved if saved is not None else ("", None)
        for policy in self.catalog.policies:
            if policy.id > cursor:
                return policy, revision
        return (self.catalog.policies[0] if self.catalog.policies else None), revision

    def _advance_policy(self, policy: DeparturePolicy) -> bool:
        with self.state.sessions() as session:
            row = session.get(_DeparturePolicyRow, policy.id)
            if row is None:
                raise RuntimeError("departure policy state is unavailable")
            phase, cursor, generation = row.phase, row.cursor, row.generation
        if phase == "reset_required":
            return False
        if phase == "new":
            checkpoint = self.riverhog.create_catalog_sync_checkpoint()
            with self.state.sessions() as session, session.begin():
                row = session.get(_DeparturePolicyRow, policy.id, with_for_update=True)
                if row is None or row.phase != "new":
                    return False
                _bind_checkpoint(row, checkpoint)
            return True
        if not isinstance(cursor, str):
            raise RuntimeError("active departure policy has no catalog cursor")
        try:
            if phase == "baseline":
                catalog_page = self.riverhog.list_catalog_sync_collections(cursor, limit=100)
                matches = [
                    (item, catalog_selector_matches(self.riverhog, policy.selector, item))
                    for item in catalog_page.collections
                ]
                return self._commit_baseline_page(
                    policy,
                    cursor=cursor,
                    page=catalog_page,
                    matches=matches,
                    expected_generation=generation,
                )
            if phase == "following":
                change_page = self.riverhog.list_catalog_sync_changes(cursor, limit=1)
                evaluated = None
                if change_page.changes and isinstance(change_page.changes[0], CatalogSyncUpsert):
                    evaluated = catalog_selector_matches(
                        self.riverhog, policy.selector, change_page.changes[0]
                    )
                return self._commit_change_page(
                    policy,
                    cursor=cursor,
                    page=change_page,
                    evaluated=evaluated,
                    expected_generation=generation,
                )
        except RiverhogError as exc:
            if exc.code in _RESET_ERRORS:
                self._require_rebaseline(
                    policy.id, generation=generation, phase=phase, cursor=cursor
                )
            raise
        except _AuthorityChanged:
            self._require_rebaseline(policy.id, generation=generation, phase=phase, cursor=cursor)
            raise
        raise RuntimeError("departure policy phase is invalid")

    def _commit_baseline_page(
        self,
        policy: DeparturePolicy,
        *,
        cursor: str,
        page: CatalogSyncCollectionPage,
        matches: list[tuple[CatalogSyncDescriptor, bool]],
        expected_generation: str,
    ) -> bool:
        with self.state.sessions() as session, session.begin():
            row = session.get(_DeparturePolicyRow, policy.id, with_for_update=True)
            next_cursor = page.next_cursor or page.changes_cursor
            next_phase = "baseline" if page.next_cursor is not None else "following"
            if (
                row is not None
                and row.policy_sha256 == policy.policy_sha256
                and row.generation == expected_generation
                and row.source_identity == page.source_identity
                and row.authorization_view_identity == page.authorization_view_identity
                and row.cursor == next_cursor
                and row.phase == next_phase
            ):
                for descriptor, _matched in matches:
                    self._require_replayed_change(session, row, descriptor)
                return False
            self._require_page_authority(
                row,
                policy,
                cursor,
                page,
                expected_generation=expected_generation,
                expected_phase="baseline",
            )
            assert row is not None
            for descriptor, matched in matches:
                self._apply_upsert(session, row, descriptor, matched)
            row.cursor = next_cursor
            row.phase = next_phase
            row.updated_at = utc_timestamp_now()
            return True

    def _commit_change_page(
        self,
        policy: DeparturePolicy,
        *,
        cursor: str,
        page: CatalogSyncChangePage,
        evaluated: bool | None,
        expected_generation: str,
    ) -> bool:
        with self.state.sessions() as session, session.begin():
            row = session.get(_DeparturePolicyRow, policy.id, with_for_update=True)
            if (
                row is not None
                and row.policy_sha256 == policy.policy_sha256
                and row.generation == expected_generation
                and row.source_identity == page.source_identity
                and row.authorization_view_identity == page.authorization_view_identity
                and row.cursor == page.next_cursor
                and row.phase == "following"
                and row.through_revision == page.through_revision
            ):
                if len(page.changes) > 1:
                    raise RuntimeError("departure change step exceeded its one-change transaction")
                if page.changes:
                    replay = page.changes[0]
                    self._require_replayed_change(session, row, replay)
                return False
            self._require_page_authority(
                row,
                policy,
                cursor,
                page,
                expected_generation=expected_generation,
                expected_phase="following",
            )
            assert row is not None
            if len(page.changes) > 1:
                raise RuntimeError("departure change step exceeded its one-change transaction")
            if page.changes:
                change = page.changes[0]
                if isinstance(change, CatalogSyncDeparture):
                    self._apply_departure(session, policy, row, change)
                else:
                    if evaluated is None:
                        raise RuntimeError("departure upsert was not evaluated exactly")
                    self._apply_upsert(session, row, change, evaluated)
            row.cursor = page.next_cursor
            row.through_revision = page.through_revision
            row.updated_at = utc_timestamp_now()
            return bool(page.changes or page.next_cursor != cursor)

    @staticmethod
    def _require_replayed_change(
        session: Any,
        policy_row: _DeparturePolicyRow,
        change: CatalogSyncDescriptor | CatalogSyncDeparture,
    ) -> None:
        operation = "departure" if isinstance(change, CatalogSyncDeparture) else "upsert"
        identity = canonical_json_sha256({"operation": operation, **change.model_dump(mode="json")})
        seen = session.get(
            _DepartureSeenRow,
            (policy_row.policy_id, policy_row.generation, change.collection_id),
        )
        if (
            seen is None
            or seen.revision != change.revision
            or seen.operation != operation
            or seen.authority_sha256 != identity
        ):
            raise RuntimeError("departure catalog replay differs from committed authority")

    @staticmethod
    def _apply_upsert(
        session: Any,
        policy_row: _DeparturePolicyRow,
        descriptor: CatalogSyncDescriptor,
        matched: bool,
    ) -> None:
        identity = canonical_json_sha256(
            {"operation": "upsert", **descriptor.model_dump(mode="json")}
        )
        key = (policy_row.policy_id, policy_row.generation, descriptor.collection_id)
        seen = session.get(_DepartureSeenRow, key)
        if seen is not None:
            if int(descriptor.revision) < int(seen.revision):
                return
            if int(descriptor.revision) == int(seen.revision):
                if seen.operation != "upsert" or seen.authority_sha256 != identity:
                    raise RuntimeError("Riverhog catalog revision changed its exact authority")
                return
        encoded = json.dumps(
            descriptor.model_dump(mode="json", include=set(CatalogSyncDescriptor.model_fields)),
            sort_keys=True,
            separators=(",", ":"),
        )
        if seen is None:
            session.add(
                _DepartureSeenRow(
                    policy_id=policy_row.policy_id,
                    generation=policy_row.generation,
                    collection_id=descriptor.collection_id,
                    revision=descriptor.revision,
                    operation="upsert",
                    authority_sha256=identity,
                    matched=matched,
                    document_bytes=len(encoded.encode("utf-8")),
                    document_json=encoded,
                )
            )
            return
        seen.revision = descriptor.revision
        seen.operation = "upsert"
        seen.authority_sha256 = identity
        seen.matched = matched
        seen.document_bytes = len(encoded.encode("utf-8"))
        seen.document_json = encoded

    @staticmethod
    def _apply_departure(
        session: Any,
        policy: DeparturePolicy,
        policy_row: _DeparturePolicyRow,
        departure: CatalogSyncDeparture,
    ) -> None:
        identity = canonical_json_sha256(departure.model_dump(mode="json"))
        key = (policy.id, policy_row.generation, departure.collection_id)
        seen = session.get(_DepartureSeenRow, key)
        if seen is not None:
            if int(departure.revision) < int(seen.revision):
                return
            if int(departure.revision) == int(seen.revision):
                if seen.operation != "departure" or seen.authority_sha256 != identity:
                    raise RuntimeError("Riverhog catalog revision changed its exact authority")
                return
        if seen is not None and seen.operation == "upsert" and seen.matched:
            if seen.document_json is None:
                raise RuntimeError("matched departure has no last visible descriptor")
            if policy_row.source_identity is None or policy_row.authorization_view_identity is None:
                raise RuntimeError("departure policy is not bound to a catalog authority")
            intent = DepartureEffectIntent.seal(
                DepartureEffectIntentPayload(
                    policy_id=policy.id,
                    policy_revision=policy.revision,
                    policy_sha256=policy.policy_sha256,
                    target_registration_id=policy.target_registration_id,
                    target_identity=policy.target_identity,
                    source_identity=policy_row.source_identity,
                    authorization_view_identity=policy_row.authorization_view_identity,
                    last_collection=CatalogSyncDescriptor.model_validate_json(seen.document_json),
                    departure_cause=departure.cause,
                    departure_revision=departure.revision,
                )
            )
            if session.get(_DepartureEffectRow, intent.departure_id) is None:
                encoded = _encode(intent)
                now = utc_timestamp_now()
                session.add(
                    _DepartureEffectRow(
                        departure_id=intent.departure_id,
                        policy_id=policy.id,
                        state="pending",
                        document_bytes=len(encoded.encode("utf-8")),
                        document_json=encoded,
                        receipt_sha256=None,
                        receipt_bytes=None,
                        receipt_json=None,
                        attempt_count=0,
                        next_attempt_at=now,
                        failure=None,
                        created_at=now,
                        updated_at=now,
                    )
                )
        if seen is None:
            session.add(
                _DepartureSeenRow(
                    policy_id=policy.id,
                    generation=policy_row.generation,
                    collection_id=departure.collection_id,
                    revision=departure.revision,
                    operation="departure",
                    authority_sha256=identity,
                    matched=False,
                    document_bytes=None,
                    document_json=None,
                )
            )
            return
        seen.revision = departure.revision
        seen.operation = "departure"
        seen.authority_sha256 = identity
        seen.matched = False

    def _pending_effect_ids(self, *, limit: int) -> tuple[str, ...]:
        with self.state.sessions() as session:
            return tuple(
                session.scalars(
                    select(_DepartureEffectRow.departure_id)
                    .where(
                        _DepartureEffectRow.state == "pending",
                        _DepartureEffectRow.next_attempt_at <= utc_timestamp_now(),
                    )
                    .order_by(
                        _DepartureEffectRow.next_attempt_at,
                        _DepartureEffectRow.departure_id,
                    )
                    .limit(limit)
                )
            )

    def _advance_effect(self, departure_id: str) -> None:
        with self.state.sessions() as session:
            row = session.get(_DepartureEffectRow, departure_id)
            if row is None or row.state == "complete":
                return
            intent = DepartureEffectIntent.model_validate_json(row.document_json)
        receipt = self.targets.put_effect(intent.target_registration_id, intent)
        if (
            receipt.departure_id != intent.departure_id
            or receipt.target_identity != intent.target_identity
        ):
            raise ValueError("departure target receipt differs from sealed intent")
        encoded = _encode(receipt)
        with self.state.sessions() as session, session.begin():
            row = session.get(_DepartureEffectRow, departure_id, with_for_update=True)
            if row is None:
                raise RuntimeError("departure effect disappeared")
            if row.state == "complete":
                if row.receipt_sha256 != receipt.receipt_sha256:
                    raise RuntimeError("departure target changed a completed effect receipt")
                return
            row.state = "complete"
            row.receipt_sha256 = receipt.receipt_sha256
            row.receipt_bytes = len(encoded.encode("utf-8"))
            row.receipt_json = encoded
            row.next_attempt_at = None
            row.failure = None
            row.updated_at = utc_timestamp_now()

    def _record_effect_failure(self, departure_id: str, exc: Exception) -> None:
        with self.state.sessions() as session, session.begin():
            row = session.get(_DepartureEffectRow, departure_id, with_for_update=True)
            if row is None or row.state == "complete":
                return
            row.attempt_count += 1
            delay = min(3600, 2 ** min(row.attempt_count, 10))
            row.next_attempt_at = format_utc_timestamp(utc_now() + timedelta(seconds=delay))
            row.failure = f"{type(exc).__name__}: {exc}"[:1000]
            row.updated_at = utc_timestamp_now()

    @staticmethod
    def _require_page_authority(
        row: _DeparturePolicyRow | None,
        policy: DeparturePolicy,
        cursor: str,
        page: CatalogSyncCollectionPage | CatalogSyncChangePage,
        *,
        expected_generation: str,
        expected_phase: Literal["baseline", "following"],
    ) -> None:
        if (
            row is None
            or row.policy_sha256 != policy.policy_sha256
            or row.generation != expected_generation
            or row.phase != expected_phase
            or row.cursor != cursor
            or row.source_identity != page.source_identity
            or row.authorization_view_identity != page.authorization_view_identity
        ):
            raise _AuthorityChanged("departure catalog authority changed; rebaseline is required")

    def _require_rebaseline(
        self, policy_id: str, *, generation: str, phase: str, cursor: str
    ) -> None:
        with self.state.sessions() as session, session.begin():
            row = session.get(_DeparturePolicyRow, policy_id, with_for_update=True)
            if (
                row is not None
                and row.generation == generation
                and row.phase == phase
                and row.cursor == cursor
            ):
                row.phase = "reset_required"
                row.updated_at = utc_timestamp_now()

    def _policy(self, policy_id: str) -> DeparturePolicy:
        try:
            return self._policies[policy_id]
        except KeyError as exc:
            raise KeyError(policy_id) from exc

    def _store_cursor(self, stream: str, revision: int | None, value: str) -> None:
        try:
            self.state.compare_and_swap_cursor(stream, expected_revision=revision, cursor=value)
        except Exception:
            current = self.state.load_cursor(stream)
            if current is None or current[0] != value:
                raise


def _bind_checkpoint(row: _DeparturePolicyRow, checkpoint: CatalogSyncCheckpoint) -> None:
    row.phase = "baseline"
    row.generation = secrets.token_hex(32)
    row.source_identity = checkpoint.source_identity
    row.authorization_view_identity = checkpoint.authorization_view_identity
    row.cursor = checkpoint.catalog_cursor
    row.through_revision = "0"
    row.updated_at = utc_timestamp_now()


def _policy_status(policy: DeparturePolicy, row: _DeparturePolicyRow) -> DeparturePolicyStatus:
    return DeparturePolicyStatus(
        policy=policy,
        policy_sha256=row.policy_sha256,
        phase=cast(DeparturePhase, row.phase),
        source_identity=row.source_identity,
        authorization_view_identity=row.authorization_view_identity,
        through_revision=row.through_revision,
        updated_at=row.updated_at,
    )


def _effect_view(row: _DepartureEffectRow) -> DepartureEffectView:
    return DepartureEffectView(
        intent=DepartureEffectIntent.model_validate_json(row.document_json),
        state=cast(DepartureEffectState, row.state),
        receipt=(
            None
            if row.receipt_json is None
            else DepartureEffectReceipt.model_validate_json(row.receipt_json)
        ),
        attempt_count=row.attempt_count,
        next_attempt_at=row.next_attempt_at,
        failure=row.failure,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _encode(value: Any) -> str:
    return json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


__all__ = ["DepartureEffectService", "DepartureTargetPort", "HttpDepartureTargetPort"]
