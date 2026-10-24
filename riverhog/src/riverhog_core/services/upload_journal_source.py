"""Bounded owner-authenticated source reads for retained native journal validation."""

from __future__ import annotations

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    SCALAR,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
    growth_binding,
)
from riverhog_protocol import COLLECTION_UPLOAD_PROVENANCE_APPEND_BYTES_MAX
from riverhog_provenance import JournalValidationCapacity
from riverhog_provenance.constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from riverhog_provenance.replay_binding import JournalReplayBinding
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy import select
from state_schema import read_snapshot

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal

from . import collection_uploads as source_owner
from . import upload_journal_replay as replay


def _current_declaration() -> Declaration:
    probe = growth_binding(replay._probe).declaration

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    one = Expression("constant", value=1, unit=COUNT)
    return Declaration(
        id="riverhog-server.retained-source-current.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.services.upload_journal_source.UploadJournalValidationSource.verify_current",
        ),
        scope="sqlite retained source and exact replay binding verification",
        starts_at="accept the selected immutable source and cached catalog identity",
        ends_at="return currentness or reject changed binding after closing the catalog reader",
        dimensions=(
            Dimension("retained_journals", COUNT, "Catalog journal and private scan rows."),
            Dimension(
                "binding_present", SCALAR, "Zero or one: the selected probe has binding text."
            ),
            Dimension("binding_octets", OCTETS, "UTF-8 octets in that pre-existing binding text."),
            Dimension(
                "source_metadata_octets",
                OCTETS,
                "Sum of UTF-8 text and decimal scalar widths in the selected source fence "
                "and its cursor, excluding source payload and SQL/schema labels.",
            ),
        ),
        bounds=(
            Bound(
                "changed_rows",
                COUNT,
                "equal",
                Expression("constant", value=0, unit=COUNT),
                "Source verification writes no catalog state.",
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                Expression(
                    "product",
                    terms=(Expression("constant", value=8), dimension("source_metadata_octets")),
                ),
                "The point query repeats only fixed source/attempt fence fields.",
            ),
            *(
                Bound(
                    bound.metric,
                    bound.unit,
                    bound.relation,
                    Expression(
                        "delegate",
                        name="probe",
                        metric=bound.metric,
                        terms=(one,),
                        arguments=(("retained_journals", dimension("retained_journals")),),
                    ),
                    "The actual closed source/attempt point probe executes once.",
                )
                for bound in probe.bounds
            ),
            *(
                Bound(
                    metric,
                    COUNT,
                    "upper",
                    Expression("product", terms=(one, dimension("binding_present"))),
                    "A present binding is parsed once and canonically reserialized once.",
                )
                for metric in ("json_decodes", "json_encodes")
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    dimension("binding_octets"),
                    "Only the selected fixed-shape binding text enters the codec.",
                )
                for metric in ("json_decoded_octets", "json_encoded_octets")
            ),
        ),
        conditions=(
            "The maintained catalog primary keys and source/attempt fences are present.",
            "The owner constructed this source with its cached sealed catalog identity.",
            "Stored bindings use the owning UUID/digest/canonical scalar domains; malformed "
            "protocol values are rejected without acquiring semantic or archive authority.",
        ),
        invariants=(
            "A missing preparation binding differs from a replaced source.",
            "Every source/catalog field is compared exactly; matching prefixes never suffice.",
            "No original or captured source payload is read and no catalog state is written.",
        ),
        limitations=(
            "Source construction, captured or deferred reads, native validation, certificate "
            "checks, publication and retirement remain their owners' separately composed costs.",
            "SQLite instructions exclude internal key comparisons, page I/O and physical CPU; "
            "other engines require separate qualification.",
        ),
    )


def _read_declaration(mode: str) -> Declaration:
    current = growth_binding(UploadJournalValidationSource.verify_current).declaration
    capture = growth_binding(source_owner._read_upload_journal_capture_segment).declaration
    children = {"current": current, "capture": capture}

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def deferred(term: Expression) -> Expression:
        return Expression("product", terms=(dimension("deferred_window"), term))

    def delegate(tag: str, metric: str) -> Expression:
        calls = "current_calls" if tag == "current" else "captured_loads"
        return Expression(
            "delegate",
            name=tag,
            metric=metric,
            terms=(dimension(calls),),
            arguments=tuple((item.name, dimension(item.name)) for item in children[tag].dimensions),
        )

    def depth(name: str) -> Expression:
        return Expression("log2_ceil", terms=(add(count(2), dimension(name)),))

    controls = add(
        scale(8, dimension("capture_control_octets")),
        deferred(scale(8, dimension("source_metadata_octets"))),
    )
    return Declaration(
        id="riverhog-server.retained-source-" + mode.replace("_", "-") + ".growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.services.upload_journal_source.UploadJournalValidationSource." + mode,
        ),
        scope="sqlite owner-authenticated captured and deferred source reads",
        starts_at="validate the exact requested range and current source before any cached read",
        ends_at="return one short source window or an exact admitted range, or reject the call",
        dimensions=(
            *current.dimensions,
            Dimension("captured_segments", COUNT, "Pages retained in the exact source directory."),
            Dimension("staged_chunks", COUNT, "Retained original transport chunks in this source."),
            Dimension("range_octets", OCTETS, "Nonnegative requested byte extent."),
            Dimension("current_calls", COUNT, "Zero or one, from pre-call range validity."),
            Dimension(
                "captured_loads",
                COUNT,
                "Captured pages selected by range/admission/currentness and initial cache state; "
                "only an already cached first selected page can avoid a load.",
            ),
            Dimension(
                "capture_control_octets",
                OCTETS,
                "Sum of source-fence scalar widths and each selected page ordinal's decimal "
                "width over those pre-call page-load inputs, excluding source payload.",
            ),
            Dimension(
                "deferred_window",
                SCALAR,
                "Zero or one: a nonempty window begins beyond the captured prefix and "
                "reaches its deferred transport branch.",
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                add(
                    delegate("current", "sql_statements"),
                    delegate("capture", "sql_statements"),
                    deferred(count(2)),
                ),
                "Actual currentness/page delegates and two additional deferred point queries.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                add(
                    delegate("current", "returned_rows"),
                    delegate("capture", "source_rows"),
                    dimension("captured_loads"),
                    deferred(count(2)),
                ),
                "Each page also returns one fixed source-fence scalar; deferred reads return "
                "one scalar and one transport row.",
            ),
            Bound(
                "changed_rows",
                COUNT,
                "equal",
                count(0),
                "Every source read and currentness check is read-only.",
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                add(delegate("current", "bound_parameter_octets"), controls),
                "Exact source fences, selected ordinals and one indexed deferred offset.",
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                add(delegate("current", "binding_octets"), controls),
                "Only the exact binding, fixed source keys/page digests and transport positions.",
            ),
            Bound(
                "loaded_payload_octets",
                OCTETS,
                "upper",
                add(
                    delegate("capture", "loaded_segment_octets"),
                    deferred(
                        Expression(
                            "constant",
                            value=COLLECTION_UPLOAD_PROVENANCE_APPEND_BYTES_MAX,
                            unit=OCTETS,
                        )
                    ),
                ),
                "Selected captured pages or one existing bounded transport chunk.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                delegate("capture", "hashed_octets"),
                "Actual page integrity hashing; a cached page introduces no second page hash.",
            ),
            Bound(
                "returned_octets",
                OCTETS,
                "upper",
                dimension("range_octets"),
                "Short windows retain their real extent; successful exact reads are checked "
                "against the complete requested bytes.",
            ),
            Bound(
                "assembled_piece_octets",
                OCTETS,
                "upper",
                dimension("range_octets")
                if mode == "read_exact"
                else Expression("constant", value=0, unit=OCTETS),
                "Actual local assembly pieces cover only the requested admitted range.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                add(
                    delegate("current", "sqlite_vm_instructions"),
                    delegate("capture", "sqlite_vm_instructions"),
                    deferred(
                        add(
                            count(1024),
                            Expression("product", terms=(count(256), depth("staged_chunks"))),
                            Expression("product", terms=(count(128), depth("retained_journals"))),
                        )
                    ),
                ),
                "Actual delegate instructions and an indexed native deferred-window envelope.",
            ),
            *(
                Bound(
                    metric,
                    unit,
                    "upper",
                    delegate("current", metric),
                    "All binding codec work belongs to the actual currentness delegate.",
                )
                for metric, unit in (
                    ("json_decodes", COUNT),
                    ("json_encodes", COUNT),
                    ("json_decoded_octets", OCTETS),
                    ("json_encoded_octets", OCTETS),
                )
            ),
            *(
                Bound(
                    tag + "_" + bound.metric,
                    bound.unit,
                    bound.relation,
                    delegate(tag, bound.metric),
                    "Compose every actual successful child call with its own input domain.",
                )
                for tag, child in children.items()
                for bound in child.bounds
            ),
        ),
        conditions=(
            "The maintained source-directory, journal and byte-offset indexes are present.",
            "Pre-call request, source/cursor, admission and initial one-page cache state "
            "determine the exact child call inputs; empty or rejected calls load no payload.",
            "Selected captured pages satisfy their owning successful-read contract; lost or "
            "changed pages retain separate fail-closed correctness evidence.",
        ),
        invariants=(
            "A cached page is never returned before exact currentness succeeds.",
            "Deferred windows read one actual historical transport chunk without range assembly.",
            "Exact reads preserve the existing frame admission extent and complete-byte checks.",
            "No accepted semantic history is replayed and all catalog readers close on exit.",
        ),
        limitations=(
            "These logical byte, query, row, codec and hash observations do not assert physical "
            "allocations, page I/O, CPU timing or lock latency.",
            "Source capture/preparation, native semantics, certificates, publication, retries "
            "and retirement retain their owning composed costs.",
            "SQLite VM instructions exclude internal B-tree comparisons and page I/O; other "
            "engines require separate qualification.",
        ),
    )


class UploadJournalValidationSource:
    def __init__(
        self,
        factory: SessionFactory,
        source: source_owner._UploadJournalValidationWork,
        *,
        catalog: ContractCatalog,
    ) -> None:
        self.factory, self.source = factory, source
        self._binding = replay._binding(source, catalog)
        self._page: tuple[int, bytes] | None = None

    @property
    def binding(self) -> JournalReplayBinding:
        return self._binding

    @declare_growth(_current_declaration(), delegates=(("probe", replay._probe),))
    def verify_current(self) -> bool:
        current, raw = replay._probe(self.factory, self.source)
        if raw is not None and JournalReplayBinding.from_json(raw) != self._binding:
            raise RuntimeError("retained replay inputs differ from the selected attempt")
        return current

    def _range(self, offset: int, octets: int) -> None:
        if (
            type(offset) is not int
            or type(octets) is not int
            or offset < 0
            or octets < 0
            or offset + octets > self.source.source_bytes
        ):
            raise ValueError("invalid retained validation source range")
        if not self.verify_current():
            raise source_owner._StaleUploadJournalValidation

    def _captured_page(self, ordinal: int) -> bytes:
        if self._page is not None and self._page[0] == ordinal:
            return self._page[1]
        raw = source_owner._read_upload_journal_capture_segment(
            self.factory, self.source, ordinal=ordinal
        )
        if raw is None:
            raise source_owner._StaleUploadJournalValidation
        self._page = ordinal, raw
        return raw

    def read_window(self, offset: int, maximum_octets: int) -> bytes:
        """Return part of one fixed page or one deferred transport chunk.

        Framing may stop before a requested range ends. A short window lets the
        caller publish that exact progress without fetching many tiny historical
        transport chunks. Completed frame atoms use normalized captured pages.
        """
        self._range(offset, maximum_octets)
        if not maximum_octets:
            return b""
        extent = source_owner._JOURNAL_CAPTURE_SEGMENT_OCTETS
        if offset < self.source.cursor.byte_offset:
            raw = self._captured_page(offset // extent)
            return raw[offset % extent : offset % extent + maximum_octets]
        if self.source.phase != "deferred":
            raise RuntimeError("validation source has no captured page at its cursor")
        with read_snapshot(self.factory) as session:
            if (
                session.scalar(
                    select(Journal.collection_id).where(
                        *source_owner._upload_journal_validation_fence(self.source)
                    )
                )
                is None
            ):
                raise source_owner._StaleUploadJournalValidation
            _, start, raw = source_owner._upload_journal_chunk_at(
                session, self.source.collection_id, self.source.journal_id, offset=offset
            )
        begin = offset - start
        return raw[begin : begin + min(maximum_octets, extent)]

    def read_exact(self, offset: int, octets: int) -> bytes:
        """Read one admitted frame atom from its fixed captured pages."""
        self._range(offset, octets)
        if octets > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
            raise JournalValidationCapacity("validation source cannot admit the existing frame")
        if offset + octets > self.source.cursor.byte_offset:
            raise RuntimeError("validation frame exceeds the captured source prefix")
        if not octets:
            return b""
        extent = source_owner._JOURNAL_CAPTURE_SEGMENT_OCTETS
        end = offset + octets
        pieces = []
        for ordinal in range(offset // extent, (end - 1) // extent + 1):
            raw = self._captured_page(ordinal)
            start = ordinal * extent
            pieces.append(raw[max(0, offset - start) : min(extent, end - start)])
        raw = b"".join(pieces)
        if len(raw) != octets:
            raise RuntimeError("validation source has an incomplete captured frame")
        return raw


for _source_read_operation in (
    UploadJournalValidationSource.read_window,
    UploadJournalValidationSource.read_exact,
):
    declare_growth(
        _read_declaration(_source_read_operation.__name__),
        delegates=(
            ("current", UploadJournalValidationSource.verify_current),
            ("capture", source_owner._read_upload_journal_capture_segment),
        ),
    )(_source_read_operation)
del _source_read_operation


declare_growth(
    Declaration(
        id="riverhog-server.retained-source-construction.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.services.upload_journal_source.UploadJournalValidationSource.__init__",
        ),
        scope="successful construction over an existing immutable source and constructed catalog",
        starts_at="accept the exact selected finite source metadata and constructed catalog",
        ends_at="retain its validated replay binding and empty one-page cache",
        dimensions=(
            Dimension(
                "source_control_octets",
                OCTETS,
                "Sum of logical scalar widths of the nine exact binding fields.",
            ),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    COUNT,
                    "equal",
                    Expression("constant", value=value, unit=COUNT),
                    "Observe the actual fixed-shape binding and cached catalog operations.",
                )
                for metric, value in (
                    ("binding_constructions", 1),
                    ("binding_validations", 1),
                    ("cached_catalog_calls", 1),
                    ("uuid_validations", 1),
                    ("regex_calls", 5),
                    ("binding_fields", 9),
                    ("catalog_calls", 0),
                    ("driver_statements", 0),
                    ("filesystem_calls", 0),
                    ("native_session_calls", 0),
                    ("catalog_schema_visits", 0),
                    ("json_decodes", 0),
                    ("json_encodes", 0),
                )
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    Expression("dimension", name="source_control_octets"),
                    (
                        "Logical validated and converted scalar text stays within the "
                        "selected binding metadata."
                    ),
                )
                for metric in ("regex_input_octets", "uuid_input_octets", "scalar_text_octets")
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "equal",
                    Expression("constant", value=0, unit=OCTETS),
                    "Construction performs no payload, codec or hash work.",
                )
                for metric in (
                    "source_payload_octets",
                    "hashed_octets",
                    "json_decoded_octets",
                    "json_encoded_octets",
                )
            ),
        ),
        conditions=(
            "The selected source metadata satisfies the owning replay-binding scalar contract.",
            (
                "The caller supplies an already constructed sealed catalog with "
                "its cached exact identity."
            ),
        ),
        invariants=(
            "The binding retains the exact source, attempt, directory and catalog identity.",
            (
                "Construction starts with an empty page cache and grants no "
                "current worker or archive authority."
            ),
            (
                "No source payload, database, filesystem, schema traversal or "
                "native Session work occurs."
            ),
        ),
        limitations=(
            (
                "These logical field and text counts exclude interpreter "
                "allocation, regex internals and CPU latency."
            ),
            (
                "Catalog construction, currentness, source reads and "
                "native/service work retain their owning costs."
            ),
            "Rejected malformed constructor inputs retain separate correctness obligations.",
        ),
    )
)(UploadJournalValidationSource.__init__)
