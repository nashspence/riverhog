"""Bounded member queries over sealed controller views for compiled planning."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from typing import Any, Literal, Protocol, cast

from pydantic import JsonValue
from sqlalchemy import Table, and_, or_, select
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnElement
from stove0_protocol.observation_evidence import AcceptedView, AcceptedViewRecord
from stove0_protocol.observation_interfaces import (
    GlobalFactsView,
    ObservationInterface,
    RelationView,
    SubjectFactsView,
)
from stove0_protocol.observation_views import (
    CoverageStatus,
    GlobalView,
    ProjectedView,
    RelationViewResult,
    SubjectView,
)


class AcceptedViewQueryPort(Protocol):
    @property
    def engine(self) -> Engine: ...

    @property
    def tables(self) -> Mapping[str, Table]: ...

    @property
    def members(self) -> Table: ...

    @property
    def key(self) -> Callable[[str], str]: ...


type SubjectRecordValue = CoverageStatus | tuple[dict[str, JsonValue], ...]


class _SubjectRecords(Mapping[str, SubjectRecordValue]):
    def __init__(
        self,
        engine: Engine,
        table: Table,
        members: Table,
        question_sha256: str,
        view_id: str,
        scope_sha256: str,
        *,
        status: bool,
    ) -> None:
        self.engine, self.table, self.members = engine, table, members
        self.scope_sha256 = scope_sha256
        self.question_sha256, self.view_id, self.status = question_sha256, view_id, status

    def _base(self, kind: Literal["coverage", "subject"]) -> Select[Any]:
        r = self.table
        # Indexed fields are projections of the exact canonical record.
        return select(r.c.record_json).where(
            r.c.question_sha256 == self.question_sha256,
            r.c.view_id == self.view_id,
            r.c.subject_id.is_not(None),
            r.c.kind == kind,
            self._scoped(),
        )

    def _scoped(self) -> ColumnElement[bool]:
        m, r = self.members, self.table
        return (
            select(m.c.artifact_id)
            .where(m.c.selection_sha256 == self.scope_sha256, m.c.artifact_id == r.c.subject_id)
            .exists()
        )

    def __getitem__(self, subject_id: str) -> SubjectRecordValue:
        r = self.table
        with self.engine.connect() as connection:
            documents = connection.scalars(
                self._base("coverage" if self.status else "subject")
                .where(r.c.subject_id == subject_id)
                .order_by(r.c.request_id, r.c.record_ordinal)
            )
            status = None
            rows = []
            for document in documents:
                record = AcceptedViewRecord.model_validate_json(document)
                if record.kind != ("coverage" if self.status else "subject"):
                    raise ValueError("accepted record kind differs from its indexed projection")
                if record.kind == "coverage":
                    if status is not None:
                        raise ValueError("accepted view repeats exact subject coverage")
                    if record.value not in {"complete", "unsupported", "ambiguous", "insufficient"}:
                        raise ValueError("accepted coverage has an undeclared status")
                    status = cast(CoverageStatus, record.value)
                elif not self.status and record.kind == "subject":
                    if not isinstance(record.value, dict):
                        raise ValueError("accepted subject fact is not an object")
                    rows.append(record.value)
            if self.status:
                if status is None:
                    raise KeyError(subject_id)
                return status
            if not rows and subject_id not in self:
                raise KeyError(subject_id)
            return tuple(rows)

    def __iter__(self) -> Iterator[str]:
        r = self.table
        after = ""
        while True:
            with self.engine.connect() as connection:
                members = connection.scalars(
                    select(r.c.subject_id)
                    .where(
                        r.c.question_sha256 == self.question_sha256,
                        r.c.view_id == self.view_id,
                        r.c.subject_id.is_not(None),
                        r.c.subject_id > after,
                        self._scoped(),
                    )
                    .distinct()
                    .order_by(r.c.subject_id)
                    .limit(100)
                ).all()
            if not members:
                return
            yield from members
            after = members[-1]

    def __len__(self) -> int:
        from sqlalchemy import func

        r = self.table
        with self.engine.connect() as connection:
            count = connection.scalar(
                select(func.count(func.distinct(r.c.subject_id))).where(
                    r.c.question_sha256 == self.question_sha256,
                    r.c.view_id == self.view_id,
                    r.c.subject_id.is_not(None),
                    self._scoped(),
                )
            )

            if count is None:
                raise ValueError("accepted subject index lost its count")
            return int(count)

    def __contains__(self, subject_id: object) -> bool:
        r = self.table
        with self.engine.connect() as connection:
            return (
                connection.scalar(
                    select(r.c.subject_id)
                    .where(
                        r.c.question_sha256 == self.question_sha256,
                        r.c.view_id == self.view_id,
                        r.c.subject_id == subject_id,
                        self._scoped(),
                    )
                    .limit(1)
                )
                is not None
            )


def _check_authority(
    store: AcceptedViewQueryPort, authority: AcceptedView, interface: ObservationInterface
) -> None:
    if interface.ref != authority.interface or authority.view_id not in interface.views:
        raise ValueError("planner query requires an exact declared view")
    v = store.tables["view"]
    with store.engine.connect() as connection:
        document = connection.scalar(
            select(v.c.view_json).where(
                v.c.view_sha256 == store.key(authority.view_sha256),
                v.c.state == "complete",
            )
        )
    if document is None or AcceptedView.model_validate_json(document) != authority:
        raise ValueError("planner query has no exact sealed accepted view")


class _ViewRecords:
    def __init__(
        self,
        store: AcceptedViewQueryPort,
        authority: AcceptedView,
        *,
        kind: Literal["global", "relation"],
    ) -> None:
        self.store, self.authority, self.kind = store, authority, kind

    def __iter__(self) -> Iterator[JsonValue | tuple[str, str]]:
        r, m = self.store.tables["records"], self.store.members
        after_request, after_ordinal = "", -1
        while True:
            statement = select(r).where(
                r.c.question_sha256 == self.store.key(self.authority.question_sha256),
                r.c.view_id == self.authority.view_id,
                r.c.kind == self.kind,
                or_(
                    r.c.request_id > after_request,
                    and_(r.c.request_id == after_request, r.c.record_ordinal > after_ordinal),
                ),
            )
            if self.kind == "relation":
                for endpoint in (r.c.primary_id, r.c.associated_id):
                    statement = statement.where(
                        select(m.c.artifact_id)
                        .where(
                            m.c.selection_sha256 == self.authority.selected_scope.selection_sha256,
                            m.c.artifact_id == endpoint,
                        )
                        .exists()
                    )
            with self.store.engine.connect() as connection:
                page = (
                    connection.execute(
                        statement.order_by(
                            r.c.request_id,
                            r.c.record_ordinal,
                        ).limit(100)
                    )
                    .mappings()
                    .all()
                )
            if not page:
                return
            for row in page:
                record = AcceptedViewRecord.model_validate_json(row["record_json"])
                if record.kind != self.kind:
                    raise ValueError("accepted record kind differs from its indexed projection")
                if self.kind == "relation":
                    if record.value != {
                        "primary_id": row["primary_id"],
                        "associated_id": row["associated_id"],
                    }:
                        raise ValueError("accepted relation differs from its indexed endpoints")
                    yield row["primary_id"], row["associated_id"]
                else:
                    yield record.value
            after_request, after_ordinal = page[-1]["request_id"], page[-1]["record_ordinal"]


def _global_records(store: AcceptedViewQueryPort, authority: AcceptedView) -> Iterator[JsonValue]:
    for record in _ViewRecords(store, authority, kind="global"):
        if isinstance(record, tuple):
            raise ValueError("accepted global record contains a relation edge")
        yield record


def _relation_edges(
    store: AcceptedViewQueryPort, authority: AcceptedView
) -> Iterator[tuple[str, str]]:
    for record in _ViewRecords(store, authority, kind="relation"):
        if not isinstance(record, tuple):
            raise ValueError("accepted relation record has no exact endpoints")
        yield record


def accepted_view_queries(
    store: AcceptedViewQueryPort, authority: AcceptedView, *, interface: ObservationInterface
) -> ProjectedView:
    """Lazy internal projections of one sealed view; original evidence stays intact."""
    _check_authority(store, authority, interface)
    definition = interface.views[authority.view_id]
    if isinstance(definition, SubjectFactsView):
        return subject_view_queries(store, authority, interface=interface)
    if isinstance(definition, GlobalFactsView):
        return GlobalView(records=_global_records(store, authority))
    if isinstance(definition, RelationView):
        return RelationViewResult(
            edges=_relation_edges(store, authority),
            statuses=cast(
                Mapping[str, CoverageStatus],
                _SubjectRecords(
                    store.engine,
                    store.tables["records"],
                    store.members,
                    store.key(authority.question_sha256),
                    authority.view_id,
                    authority.selected_scope.selection_sha256,
                    status=True,
                ),
            ),
            # Planning consumes exact edges and coverage. Raw producer rows are
            # available through the retained original result and its support refs.
            records=(),
        )
    raise ValueError("planner query selected an unknown view type")


def subject_view_queries(
    store: AcceptedViewQueryPort, authority: AcceptedView, *, interface: ObservationInterface
) -> SubjectView:
    """Internal planner reads of one previously sealed complete subject view.

    External consumers use the distinct authorized page boundary. These lazy
    mappings do not serialize or copy a larger observer envelope.
    """
    if interface.ref != authority.interface or not isinstance(
        interface.views.get(authority.view_id), SubjectFactsView
    ):
        raise ValueError("subject query requires the exact declared subject-facts view")
    _check_authority(store, authority, interface)
    records = store.tables["records"]
    return SubjectView(
        rows=cast(
            Mapping[str, tuple[dict[str, JsonValue], ...]],
            _SubjectRecords(
                store.engine,
                records,
                store.members,
                store.key(authority.question_sha256),
                authority.view_id,
                authority.selected_scope.selection_sha256,
                status=False,
            ),
        ),
        statuses=cast(
            Mapping[str, CoverageStatus],
            _SubjectRecords(
                store.engine,
                records,
                store.members,
                store.key(authority.question_sha256),
                authority.view_id,
                authority.selected_scope.selection_sha256,
                status=True,
            ),
        ),
    )
