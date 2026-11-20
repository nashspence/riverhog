"""Native corpus authority from the current owned Core inputs."""

from __future__ import annotations

from dataclasses import replace

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
    growth_binding,
)
from riverhog_provenance import CorpusReplayBinding

from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_corpus_inputs import corpus_source_admitted
from riverhog_core.canonical_discovery_metadata import DiscoveryMetadata
from riverhog_core.canonical_discovery_selection import (
    CorpusSelectionWork,
    corpus_selection_admitted,
    frozen_corpus_selection,
)


class DiscoveryCorpusSource:
    """Bind Native to actual frozen inputs beneath one serialized Core owner.

    Metadata keeps the original kernel custody and live catalog fence. Exact
    membership reads the owned indexed records; this port has no database or
    directory supplied by a client and grants no publication authority.
    """

    def __init__(self, metadata: DiscoveryMetadata, selection: CorpusSelectionWork) -> None:
        self.metadata, self.selection = metadata, selection
        self._admission = metadata.admission
        with metadata.read() as (db, key):
            self._manifest = frozen_corpus_selection(db, key, selection)
            self._binding = metadata.job.binding
            if (
                selection.history.admission is not self._admission
                or self._manifest.sources.admission_sha256 != self._admission.identity
                or self._binding.source_manifest_sha256 != self._manifest.sources.manifest_sha256
                or self._binding.source_count != self._manifest.sources.source_count
                or self._binding.selection_manifest_sha256 != self._manifest.selection_sha256
                or self._binding.selection_count != self._manifest.selection_count
            ):
                raise StaleArchiveTask("Native corpus has another exact original input binding")

    @property
    def binding(self) -> CorpusReplayBinding:
        return self._binding

    def _require_binding(self) -> None:
        if (
            self.metadata.admission is not self._admission
            or self.selection.history.admission is not self._admission
            or self.selection.sources != self._manifest.sources
            or self.metadata.job.binding != self._binding
        ):
            raise StaleArchiveTask("Native corpus original owner binding changed")

    def verify_current(self) -> bool:
        try:
            self._require_binding()
            with self.metadata.read() as (db, key):
                return frozen_corpus_selection(db, key, self.selection) == self._manifest
        except StaleArchiveTask:
            return False

    def admit_source(self, anchor: HistoryJournalAnchor) -> bool:
        self._require_binding()
        with self.metadata.read() as (db, key):
            if frozen_corpus_selection(db, key, self.selection) != self._manifest:
                raise StaleArchiveTask("Native corpus original frozen inputs changed")
            return corpus_source_admitted(db, key, self._admission, self._manifest.sources, anchor)

    def admit_selection(self, anchor: HistoryJournalAnchor) -> bool:
        self._require_binding()
        with self.metadata.read() as (db, key):
            return corpus_selection_admitted(db, key, self.selection, self._manifest, anchor)


def _source_declaration(operation: str) -> Declaration:
    storage = growth_binding(DiscoveryMetadata.read).declaration
    inputs = (
        (corpus_selection_admitted,)
        if operation == "admit_selection"
        else (frozen_corpus_selection, corpus_source_admitted)
        if operation == "admit_source"
        else (frozen_corpus_selection,)
    )
    children = (storage, *(growth_binding(function).declaration for function in inputs))
    names = {
        "registered_inputs": "retained_core_rows",
        "control_octets": "metadata_octets",
    }

    def renamed(expression: Expression) -> Expression:
        return replace(
            expression,
            name=names.get(expression.name, expression.name),
            terms=tuple(renamed(term) for term in expression.terms),
        )

    def total(metric: str, unit: tuple[tuple[str, int], ...]) -> Expression:
        body_metric = {
            "core_sql_statements": "sql_statements",
            "core_returned_rows": "returned_rows",
            "core_returned_octets": "returned_octets",
            "core_parameter_octets": "parameter_octets",
            "core_vm_instructions": "sqlite_vm_instructions",
            "source_payload_octets": "source_read_octets",
        }.get(metric, metric)
        terms = tuple(
            renamed(bound.expression)
            for index, child in enumerate(children)
            for bound in child.bounds
            if bound.metric == (metric if index == 0 else body_metric)
        )
        return (
            Expression("constant", value=0, unit=unit)
            if not terms
            else terms[0]
            if len(terms) == 1
            else Expression("sum", terms=terms)
        )

    metrics = {
        **{bound.metric: bound.unit for bound in storage.bounds},
        **{
            metric: OCTETS
            for metric in (
                "encoded_octets",
                "decoded_octets",
                "sha256_block_octets",
                "written_octets",
            )
        },
        "source_reads": COUNT,
        "native_anchor_calls": COUNT,
    }
    return Declaration(
        id="riverhog-server.discovery-corpus-source-"
        + operation.strip("_").replace("_", "-")
        + ".growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.canonical_discovery_corpus_source.DiscoveryCorpusSource." + operation,
        ),
        scope="actual Core storage fences and exact frozen Native corpus input membership",
        starts_at="receive the owned Core metadata, original selection and one exact anchor",
        ends_at="close all Core/catalog cursors and return the original binding or membership",
        dimensions=(
            Dimension("retained_core_rows", COUNT, "All independently retained Core work rows."),
            Dimension("retained_catalog_rows", COUNT, "All retained catalog and worker rows."),
            Dimension("metadata_octets", OCTETS, "Largest complete original control or row width."),
            Dimension("path_octets", OCTETS, "Complete selected filesystem path width."),
        ),
        bounds=tuple(
            Bound(
                metric,
                unit,
                "upper",
                total(metric, unit),
                "One owned read boundary plus its fixed original freeze/membership checks.",
            )
            for metric, unit in metrics.items()
        ),
        conditions=(
            "The application serializes calls and retains the original exclusive kernel custody.",
            "Storage read fences and input operations retain their owning declarations.",
            "The bound expands those owners' expressions over the same independent inputs.",
            "The maintained schema retains its original indexes and has no added triggers.",
        ),
        invariants=(
            "Current catalog/root/worker and actual Core file custody fence every result.",
            "Original complete source anchors and selected prefixes retain distinct exact sets.",
            "No source bytes or whole input registry are replayed for membership.",
            "Input membership grants no member-State relevance, access or publication.",
        ),
        limitations=(
            "This boundary excludes Native body algorithms and archive input preparation.",
            "Logical VM and metadata bytes exclude physical I/O and internal byte comparisons.",
        ),
    )


for _operation in ("__init__", "verify_current", "admit_source", "admit_selection"):
    declare_growth(_source_declaration(_operation))(getattr(DiscoveryCorpusSource, _operation))
