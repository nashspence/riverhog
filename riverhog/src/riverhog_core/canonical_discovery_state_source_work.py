"""Resolve a derivation's exact input State through a fixed admitted source lease."""

from __future__ import annotations

import sqlite3

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance_contracts import ExternalReference, require_canonical_uuid_urn

from riverhog_core.canonical_discovery_archive_work import (
    ArchiveDiscoveryAdmission,
    ArchiveTaskKey,
    StaleArchiveTask,
)
from riverhog_core.canonical_discovery_corpus_inputs import (
    CorpusSourceInputWork,
    FrozenCorpusSourceInputs,
    corpus_source_admitted,
)
from riverhog_core.canonical_discovery_endpoint_index import EndpointIndexWork
from riverhog_core.canonical_discovery_journal_lookup import effective_journal_assertion
from riverhog_core.canonical_discovery_journal_selector_work import JournalSelectorWork
from riverhog_core.canonical_discovery_journal_work import JournalTaskKey
from riverhog_core.canonical_discovery_selectors import Selector
from riverhog_core.canonical_discovery_state_source_growth import declare_growth_for_state_input


class StateSourceRequired(RuntimeError):
    """The owner must supply the named original source before this atom can advance."""

    def __init__(self, journal_id: str) -> None:
        require_canonical_uuid_urn(journal_id)
        self.journal_id = journal_id
        super().__init__("State input requires another current admitted source lease")


def _equal_binding(left: str, right: str) -> bool:
    return left == right


class StateSelectorWork(JournalSelectorWork):
    """Retain the primary source and at most one exact foreign source for an atom."""

    def __init__(
        self,
        key: ArchiveTaskKey,
        admission: ArchiveDiscoveryAdmission,
        inputs: FrozenCorpusSourceInputs,
        endpoints: EndpointIndexWork,
        *,
        octet_budget: int,
        foreign: CorpusSourceInputWork | None = None,
    ) -> None:
        self.foreign = foreign
        self._foreign_binding_json = self._foreign_binding()
        super().__init__(key, admission, inputs, endpoints, octet_budget=octet_budget)

    def _foreign_binding(self) -> str:
        owner = self.foreign
        return canonical_json_bytes(
            None
            if owner is None
            else {
                "admission": owner.admission.identity,
                "source_key": owner.source_key,
                "source": owner.source_binding_json,
                "anchor": owner.anchor.to_mapping(),
            }
        ).decode()

    def check(self, key: ArchiveTaskKey) -> None:
        super().check(key)
        if not _equal_binding(self._foreign_binding(), self._foreign_binding_json):
            raise StaleArchiveTask("State input changed its exact foreign source binding")
        if self.foreign is not None:
            if self.foreign.admission.identity != key.admission_sha256:
                raise StaleArchiveTask("State input has another source admission")
            self.foreign.check_source()
            self.foreign.source.check_open()


def state_input_selector(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: StateSelectorWork,
    reference: ExternalReference,
) -> Selector:
    """Detach the exact historical input State without changing reference identity.

    The State preparer supplies its source-control capture connection. Its parent
    receipts and the selected child's source/point reads replay together through
    the same JournalSelectorWork apply path. Current metadata custody and complete
    independent corpus acceptance remain the caller's prerequisites.
    """
    if db.in_transaction:
        raise ValueError("State input preparation requires no active transaction")
    work.check(key)
    if not isinstance(reference, ExternalReference) or reference.object_type != "state":
        raise TypeError("State input requires a typed exact external State reference")
    owner = work.original
    if reference.journal_id != owner.anchor.journal_id:
        if work.foreign is None or work.foreign.anchor.journal_id != reference.journal_id:
            raise StateSourceRequired(reference.journal_id)
        owner = work.foreign
    if not corpus_source_admitted(db, key, work.original.admission, work.inputs, owner.anchor):
        raise StaleArchiveTask("State input source is outside the exact frozen source manifest")
    # Native checks entry ID, sequence, entry JSON hash, assertion ID and referent
    # together, and supplies the actual prefix hash/byte count at that entry.
    anchor = HistoryJournalAnchor.from_mapping(
        owner.source.reference_anchor(reference.model_dump(mode="json"))
    )
    journal_key = JournalTaskKey(key.build_id, owner.source_key, key.worker_id)
    original = effective_journal_assertion(
        db, journal_key, owner.source, anchor, reference.object_id
    )
    if (
        original is None
        or original.kind != "state"
        or original.assertion_id != reference.assertion_id
    ):
        raise StaleArchiveTask("State input differs from its exact effective original assertion")
    return Selector(
        "state",
        source_key=owner.source_key,
        anchor=anchor,
        object_id=reference.object_id,
        scope="input-history",
    )


declare_growth_for_state_input(globals())
