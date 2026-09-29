"""Schema vocabulary, generated alongside the normative schemas."""

PROFILE = "https://nashspence.github.io/riverhog/v1/provenance"
SOURCE_NAMING_VIEW_SCHEME = PROFILE + "/identifiers/source-naming-view"
ENTRY_SCHEMA = "https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json"
DIALECT = "https://json-schema.org/draft/2020-12/schema"
CATEGORY_TYPES = {
    "artifacts": ("artifact",),
    "occurrences": ("occurrence",),
    "states": ("state",),
    "agents": ("agent",),
    "contexts": ("context",),
    "activities": ("activity",),
    "descriptions": ("observation", "reported_description"),
    "relations": (
        "usage",
        "generation",
        "invalidation",
        "derivation",
        "specialization",
        "continuity",
        "content_comparison",
    ),
    "locator_bindings": ("locator_binding", "locator_binding_end"),
    "delivery_associations": ("delivery_association",),
    "custody_assertions": ("custody_assertion",),
    "availability_assertions": ("availability_assertion",),
    "journal_subjects": ("journal_subject",),
    "extensions": ("extension",),
}
