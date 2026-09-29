class ProvenanceValidationError(ValueError):
    """A document violates structural, reference, graph or journal invariants."""


class ObservationError(RuntimeError):
    """A source could not be observed under the requested boundary and policy."""


class IncompleteSourceError(ObservationError):
    """The declared source boundary was not completely measured."""


class ConcurrentJournalChangeError(ProvenanceValidationError):
    """An expected predecessor does not match the supplied immutable journal."""


class UnresolvedContractError(ProvenanceValidationError):
    """The exact referenced metadata contract is unavailable."""
