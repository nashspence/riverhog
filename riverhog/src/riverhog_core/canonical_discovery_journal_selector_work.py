"""Exact source fencing and atomic source replay for journal selectors."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import cast

from riverhog_canonical_json import canonical_json_bytes

from riverhog_core.canonical_discovery_archive_work import (
    ArchiveDiscoveryAdmission,
    ArchiveTaskKey,
    StaleArchiveTask,
    _atomic,
    _one,
)
from riverhog_core.canonical_discovery_corpus_inputs import (
    CorpusSourceInputWork,
    FrozenCorpusSourceInputs,
    corpus_source_admitted,
)
from riverhog_core.canonical_discovery_endpoint_index import (
    EndpointIndexWork,
    endpoint_index_progress,
)
from riverhog_core.canonical_discovery_journal_lookup import _source
from riverhog_core.canonical_discovery_journal_selector_growth import (
    declare_growth_for_journal_selector_apply,
)
from riverhog_core.canonical_discovery_journal_work import JournalTaskKey
from riverhog_core.canonical_discovery_selector_sources import (
    SelectorSourceReads,
    SelectorSourceReceipt,
    source_control_json,
)
from riverhog_core.canonical_discovery_selector_work import (
    PreparedSelectorPage,
    SelectorProgress,
    SelectorWork,
    apply_selector_page,
)


@dataclass(frozen=True, slots=True)
class PreparedJournalSelector:
    page: PreparedSelectorPage
    sources: tuple[SelectorSourceReceipt, ...]


class JournalSelectorWork(SelectorWork):
    """One selected complete Native source and one bounded pending preparation."""

    def __init__(
        self,
        key: ArchiveTaskKey,
        admission: ArchiveDiscoveryAdmission,
        inputs: FrozenCorpusSourceInputs,
        endpoints: EndpointIndexWork,
        *,
        octet_budget: int,
    ) -> None:
        super().__init__(key, octet_budget=octet_budget)
        self.original = CorpusSourceInputWork(admission, endpoints.source)
        self.inputs, self.endpoints = inputs, endpoints
        self.journal_key = JournalTaskKey(key.build_id, endpoints.source.source_key, key.worker_id)
        if (
            key.admission_sha256 != admission.identity
            or inputs.admission_sha256 != admission.identity
        ):
            raise StaleArchiveTask("journal selector has another exact archive admission")
        self._binding_json = self._binding()
        self._prepared_journal: PreparedJournalSelector | None = None
        self.check(key)

    def _binding(self) -> str:
        return canonical_json_bytes(
            {
                "admission": self.original.admission.binding_json,
                "root": self.original.admission.root_json,
                "source": self.original.source_binding_json,
                "manifest": self.inputs.manifest_sha256,
                "sources": str(self.inputs.source_count),
            }
        ).decode()

    def check(self, key: ArchiveTaskKey) -> None:
        if key != self.key or self._binding() != self._binding_json:
            raise StaleArchiveTask("journal selector lost its original archive/source binding")
        self.original.check_source()
        self.endpoints.check(self.journal_key)


def journal_selector_reads(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: JournalSelectorWork,
    expected: SelectorProgress,
) -> SelectorSourceReads:
    if db.in_transaction:
        raise ValueError("journal selector preparation requires no active transaction")
    work.check(key)
    selected = expected.selector
    if selected.source_key != work.original.source_key or selected.anchor is None:
        raise StaleArchiveTask("journal selector has another exact original source")
    reads = SelectorSourceReads(db, octet_budget=work.octet_budget)
    source_db = cast(sqlite3.Connection, reads)
    if not corpus_source_admitted(
        source_db, key, work.original.admission, work.inputs, work.original.anchor
    ):
        raise StaleArchiveTask("journal selector source is outside its exact frozen manifest")
    _source(source_db, work.journal_key, work.original.source, selected.anchor)
    if not endpoint_index_progress(source_db, work.journal_key, work.endpoints).complete:
        raise StaleArchiveTask("journal selector requires its completed original endpoint index")
    return reads


def _same_source(left: str, right: str) -> bool:
    return left == right


def apply_journal_selector(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: JournalSelectorWork,
    prepared: PreparedJournalSelector,
) -> SelectorProgress:
    """Recheck complete original controls before committing incidence and cursor."""
    work.check(key)
    if work._prepared_journal is not prepared:
        raise StaleArchiveTask("journal selector is not its current exact preparation")
    with _atomic(db):
        for receipt in prepared.sources:
            row = _one(db, receipt.sql, receipt.parameters)
            actual = source_control_json(receipt.sql, receipt.parameters, row, work.octet_budget)
            if not _same_source(actual, receipt.exact_json):
                raise StaleArchiveTask("journal selector original source changed after preparation")
        return apply_selector_page(db, key, work, prepared.page)


declare_growth_for_journal_selector_apply(globals())
