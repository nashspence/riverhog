"""Deterministic structural views of already accepted observer testimony."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Literal

from jsonschema import Draft202012Validator
from pydantic import JsonValue

from stove0_protocol.interface_schemas import SchemaSlice, schema_slice
from stove0_protocol.jcs import canonical_json_bytes
from stove0_protocol.models import (
    JsonSchemaValidationProfile,
    ObserverContract,
    Stove0ProtocolModel,
)
from stove0_protocol.observation_interfaces import (
    ExactDocumentRef,
    ExactEndpoint,
    GlobalFactsView,
    ObservationInterface,
    RecordSource,
    RecordStatus,
    SemanticStatus,
    StatusSource,
    SubjectEndpoint,
    SubjectFactsView,
    SubjectPort,
)
from stove0_protocol.predicates import (
    MISSING,
    FactsQuantification,
    Truth,
    evaluate_row,
    read_pointer,
)

CoverageStatus = Literal["complete", "unsupported", "ambiguous", "insufficient"]
StatusResolver = Callable[
    [ExactDocumentRef, tuple[str, ...], dict[str, JsonValue]], Mapping[str, CoverageStatus]
]


@dataclass(frozen=True, slots=True)
class SubjectView:
    rows: Mapping[str, tuple[dict[str, JsonValue], ...]]
    statuses: Mapping[str, CoverageStatus]


class _CoverageRecord(Stove0ProtocolModel):
    status: CoverageStatus


def coverage_record_schema() -> SchemaSlice:
    return schema_slice(
        JsonSchemaValidationProfile.from_schema(
            "stove0.accepted-view-coverage-record/v1", _CoverageRecord.model_json_schema()
        ),
        "",
    )


def evaluate_subject_facts(
    predicate: FactsQuantification, view: SubjectView, subject: str
) -> Iterator[Truth]:
    status = view.statuses.get(subject)
    if status not in {"complete", "unsupported", "ambiguous", "insufficient"}:
        yield Truth.INDETERMINATE
        return
    if predicate.inspect == "status":
        # This is the exact accepted coverage statement, never a claim that
        # unsupported/ambiguous/insufficient records constitute complete facts.
        yield evaluate_row(predicate.where, {"status": status})
        return
    if subject not in view.rows or status != "complete":
        yield Truth.INDETERMINATE
        return
    records = view.rows[subject]
    if not records and predicate.quantifier == "every":
        yield Truth.FALSE
    for record in records:
        yield evaluate_row(predicate.where, record)


@dataclass(frozen=True, slots=True)
class GlobalView:
    records: Iterable[JsonValue]


@dataclass(frozen=True, slots=True)
class RelationViewResult:
    edges: Iterable[tuple[str, str]]
    statuses: Mapping[str, CoverageStatus]
    records: tuple[dict[str, JsonValue], ...]


type ProjectedView = SubjectView | GlobalView | RelationViewResult


def _records(facts: dict[str, JsonValue], source: RecordSource) -> Iterator[dict[str, JsonValue]]:
    rows = read_pointer(facts, source.records_at)
    if not isinstance(rows, list):
        raise ValueError("interface record source must contain its declared array")
    for row in rows:
        nested: Iterable[JsonValue]
        if source.nested_records_at is None:
            nested = (row,)
        else:
            selected = read_pointer(row, source.nested_records_at)
            if not isinstance(selected, list):
                raise ValueError("interface nested record source must contain an array")
            nested = selected
        for record in nested:
            if not isinstance(record, dict):
                raise ValueError("interface subject/relation record must be an object")
            yield record


def _statuses(
    status: StatusSource,
    subjects: Collection[str],
    facts: dict[str, JsonValue],
    semantic_status: StatusResolver | None,
) -> dict[str, CoverageStatus]:
    result: dict[str, CoverageStatus]
    if isinstance(status, SemanticStatus):
        if semantic_status is None:
            raise ValueError("exact semantic completeness extraction is unavailable")
        result = dict(semantic_status(status.profile, tuple(sorted(subjects)), facts))
    elif isinstance(status, RecordStatus):
        rows = read_pointer(facts, status.records_at)
        if not isinstance(rows, list):
            raise ValueError("interface status source must contain an array")
        result = {}
        for row in rows:
            subject, value = (
                read_pointer(row, status.subject_at),
                read_pointer(row, status.value_at),
            )
            if not isinstance(subject, str) or subject in result or not isinstance(value, str):
                raise ValueError("interface statuses require distinct exact subject identities")
            if value not in status.values:
                raise ValueError("interface status has an undeclared wire value")
            result[subject] = status.values[value]
    else:
        raise ValueError("interface requires explicit completeness status")
    if set(result) != set(subjects) or any(
        value
        not in {
            "complete",
            "unsupported",
            "ambiguous",
            "insufficient",
        }
        for value in result.values()
    ):
        raise ValueError("interface completeness status differs from the exact subject scope")
    return result


def _endpoint_map(
    endpoint: ExactEndpoint,
    subjects: Collection[str],
    projected: Mapping[str, ProjectedView],
    evidence: Mapping[str, Mapping[str, ProjectedView]],
) -> dict[bytes, str]:
    source = endpoint.lookup.source
    views = projected if source == "self" else evidence.get(source.input, {})
    view = views.get(endpoint.lookup.view)
    if not isinstance(view, SubjectView) or not set(subjects) <= set(view.rows):
        raise ValueError("exact endpoint lookup lacks its complete subject view")
    result: dict[bytes, str] = {}
    for subject in subjects:
        if view.statuses.get(subject) != "complete":
            raise ValueError("exact endpoint lookup has incomplete subject coverage")
        for row in view.rows[subject]:
            for key in endpoint.lookup.keys:
                value = read_pointer(row, key)
                if value is MISSING:
                    raise ValueError("exact endpoint lookup omits a declared endpoint key")
                identity = canonical_json_bytes(value)
                previous = result.setdefault(identity, subject)
                if previous != subject:
                    raise ValueError("one exact endpoint resolves to multiple member instances")
    return result


def project_interface(
    *,
    interface: ObservationInterface,
    contract: ObserverContract,
    subjects: tuple[str, ...],
    ports: Mapping[str, tuple[str, ...]],
    facts: dict[str, JsonValue] | None,
    evidence_views: Mapping[str, Mapping[str, ProjectedView]] | None = None,
    semantic_status: StatusResolver | None = None,
) -> dict[str, ProjectedView]:
    """Validate a whole accepted question before extracting any scoped view.

    Observer schema/semantic acceptance remains a prerequisite at the caller.
    This algebra supplies coverage and structure, never media/provenance meaning.
    """
    interface = ObservationInterface.model_validate(
        interface.model_dump(mode="json", by_alias=True)
    )
    contract = ObserverContract.model_validate(
        contract.model_dump(mode="json", by_alias=True, exclude_none=True)
    )
    interface.validate_contract(contract)
    if subjects != tuple(sorted(set(subjects))):
        raise ValueError("interface question subjects must be a canonical exact set")
    subject_ports = {
        name for name, port in interface.inputs.items() if isinstance(port, SubjectPort)
    }
    if set(ports) != subject_ports or set().union(*(set(value) for value in ports.values())) != set(
        subjects
    ):
        raise ValueError("interface subject ports differ from the exact question union")
    seen: set[str] = set()
    for members in ports.values():
        if members != tuple(sorted(set(members))) or seen & set(members):
            raise ValueError("interface subject partitions must be exact and disjoint")
        seen.update(members)
    if not subjects:
        if interface.empty_scope != "complete-empty" or facts is not None:
            raise ValueError("empty task is controller completion, never observer testimony")
        return {
            name: SubjectView(rows={}, statuses={})
            if isinstance(view, SubjectFactsView)
            else RelationViewResult(edges=(), statuses={}, records=())
            for name, view in interface.views.items()
        }
    if facts is None:
        raise ValueError("nonempty task requires accepted observer facts")
    error = next(Draft202012Validator(contract.facts_schema.document).iter_errors(facts), None)
    if error is not None:
        raise ValueError(f"interface facts violate the exact observer schema: {error.message}")
    projected: dict[str, ProjectedView] = {}
    # Subject lookups are constructed first; relations cannot recursively invent
    # another relation as an endpoint authority.
    for name, view in interface.views.items():
        if isinstance(view, SubjectFactsView):
            rows: dict[str, list[dict[str, JsonValue]]] = {subject: [] for subject in subjects}
            record_schema = schema_slice(contract.facts_schema, view.record_schema_at)
            for row in _records(facts, view.records):
                record_schema.validate(row)
                subject = read_pointer(row, view.subject_at)
                if not isinstance(subject, str) or subject not in rows:
                    raise ValueError("interface record names a subject outside the exact question")
                rows[subject].append(row)
            if view.cardinality == "one-per-subject" and any(
                len(values) != 1 for values in rows.values()
            ):
                raise ValueError("subject view has missing or duplicate subject records")
            statuses: dict[str, CoverageStatus] = (
                _statuses(view.status, subjects, facts, semantic_status)
                if view.status is not None
                else {subject: "complete" for subject in subjects}
            )
            projected[name] = SubjectView(
                rows={key: tuple(value) for key, value in rows.items()}, statuses=statuses
            )
        elif isinstance(view, GlobalFactsView):
            record = read_pointer(facts, view.record_at)
            if record is MISSING:
                raise ValueError("global interface record is missing")
            schema_slice(contract.facts_schema, view.record_schema_at).validate(record)
            projected[name] = GlobalView(records=(record,))
    for name, view in interface.views.items():
        if isinstance(view, (SubjectFactsView, GlobalFactsView)):
            continue
        covered = set().union(*(set(ports[port]) for port in view.coverage))
        statuses = _statuses(view.status, covered, facts, semantic_status)
        lookups = {
            label: _endpoint_map(endpoint, covered, projected, evidence_views or {})
            for label, endpoint in (("primary", view.primary), ("associated", view.associated))
            if isinstance(endpoint, ExactEndpoint)
        }
        edges, records = set(), []
        for row in _records(facts, view.records):
            selected = evaluate_row(view.where, row)
            if selected == Truth.FALSE:
                continue
            resolved: dict[str, str | None] = {}
            for label, endpoint in (("primary", view.primary), ("associated", view.associated)):
                value = read_pointer(row, endpoint.at)
                if value is MISSING:
                    raise ValueError("relevant relation omits its declared endpoint")
                if isinstance(endpoint, SubjectEndpoint):
                    if not isinstance(value, str) or value not in covered:
                        raise ValueError("relation names an unknown exact question subject")
                    resolved[label] = value
                else:
                    resolved[label] = lookups[label].get(canonical_json_bytes(value))
            primary, associated = resolved["primary"], resolved["associated"]
            if (
                selected != Truth.TRUE
                or evaluate_row(view.require, row) != Truth.TRUE
                or (primary is None or associated is None)
            ):
                # A relevant but unusable claim is never a complete negative.
                affected = (associated,) if associated is not None else covered
                for subject in affected:
                    if statuses[subject] == "complete":
                        statuses[subject] = "unsupported"
                continue
            if primary == associated:
                raise ValueError("relation cannot associate a member with itself")
            edges.add((primary, associated))
            records.append(row)
        projected[name] = RelationViewResult(
            edges=tuple(sorted(edges)), statuses=statuses, records=tuple(records)
        )
    return projected
