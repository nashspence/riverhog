"""Opaque-artifact provenance, independent of paths, hosts and storage models."""

from .common import (
    artifact,
    assertion,
    byte_string,
    content_description,
    evidence,
    new_id,
    reference,
    software_agent_id,
    utc_now,
)
from .constants import PACKAGE_VERSION as __version__
from .constants import PROVENANCE_ENTRY_SCHEMA, PROVENANCE_PROFILE
from .delivery import verify_delivery
from .errors import (
    ConcurrentJournalChangeError,
    IncompleteSourceError,
    ObservationError,
    ProvenanceValidationError,
    UnresolvedContractError,
)
from .graph import GraphValidation, validate_graph
from .interface import ObservationSource, StateObserver
from .journal import (
    JournalFrame,
    JournalSetValidation,
    JournalSummary,
    RecoveryResult,
    append_assertion_batches,
    append_assertions,
    append_checkpoint,
    append_correction,
    append_observation,
    assertion_reference,
    create_journal,
    encode_entry,
    external_reference,
    iter_journal_frames,
    parse_journal,
    recover_complete_prefix,
    validate_journal,
    validate_journal_chunks,
    validate_journal_set,
    validate_journal_set_chunks,
)
from .model import (
    BinaryReadable,
    ObservationPolicy,
    ObservationRequest,
    ObservationResult,
    ObservationSession,
    SourceCapabilities,
    SourceEvidence,
)
from .observer import BoundedSourceObserver
from .schema import validate_entry_document, validate_graph_fragment
from .segmented_archive import (
    JournalSegment,
    ordered_segment_commitment,
    reassemble_journal,
    segment_journal,
)
from .sources import BytesSource, SegmentSource, StreamSource

__all__ = [
    "__version__",
    "PROVENANCE_ENTRY_SCHEMA",
    "PROVENANCE_PROFILE",
    "artifact",
    "assertion",
    "byte_string",
    "content_description",
    "evidence",
    "new_id",
    "reference",
    "software_agent_id",
    "utc_now",
    "verify_delivery",
    "ConcurrentJournalChangeError",
    "IncompleteSourceError",
    "ObservationError",
    "ProvenanceValidationError",
    "UnresolvedContractError",
    "GraphValidation",
    "validate_graph",
    "ObservationSource",
    "StateObserver",
    "JournalFrame",
    "JournalSetValidation",
    "JournalSummary",
    "RecoveryResult",
    "append_assertion_batches",
    "append_assertions",
    "append_checkpoint",
    "append_correction",
    "append_observation",
    "assertion_reference",
    "create_journal",
    "encode_entry",
    "external_reference",
    "iter_journal_frames",
    "parse_journal",
    "recover_complete_prefix",
    "validate_journal",
    "validate_journal_chunks",
    "validate_journal_set",
    "validate_journal_set_chunks",
    "BinaryReadable",
    "ObservationPolicy",
    "ObservationRequest",
    "ObservationResult",
    "ObservationSession",
    "SourceCapabilities",
    "SourceEvidence",
    "BoundedSourceObserver",
    "validate_entry_document",
    "validate_graph_fragment",
    "JournalSegment",
    "ordered_segment_commitment",
    "reassemble_journal",
    "segment_journal",
    "BytesSource",
    "SegmentSource",
    "StreamSource",
]

from .providers import (
    PROVENANCE_OBSERVER_ENTRY_POINT_GROUP,
    ProvenanceObserverBinding,
    ProvenanceProviderMetadata,
    ResolvedProvenanceObserver,
    StateObserverFactory,
    list_provenance_observers,
    resolve_provenance_observer,
)

__all__ += [
    "PROVENANCE_OBSERVER_ENTRY_POINT_GROUP",
    "ProvenanceObserverBinding",
    "ProvenanceProviderMetadata",
    "ResolvedProvenanceObserver",
    "StateObserverFactory",
    "list_provenance_observers",
    "resolve_provenance_observer",
]
