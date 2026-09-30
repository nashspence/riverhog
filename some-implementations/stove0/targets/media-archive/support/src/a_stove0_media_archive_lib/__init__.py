"""Pure projection support bridging observation evidence into media archive target plans."""

from a_stove0_media_archive_lib.projection import (
    MEDIA_FACT_PROJECTION_FIELDS,
    MEDIA_PROJECTION_FORMAT,
    MediaArchiveProjection,
    MediaArchiveProjectionPayload,
    MediaProjectedValue,
    MediaProjectionItem,
    RetainedXmpSidecar,
    ffmpeg_container_metadata_args,
    render_projection_xmp,
    resolve_media_archive_preflight_projection,
    resolve_media_archive_projection,
)
from a_stove0_media_archive_lib.publication import (
    MaterializationDecisionRequired,
    MediaOutputPublicationDecision,
    MediaPublicationPlan,
    accepted_source_hints,
    append_leaf_suffix,
    replace_final_suffix,
    seal_publication_plan,
    sibling_hint,
)

__all__ = [
    "MEDIA_FACT_PROJECTION_FIELDS",
    "MEDIA_PROJECTION_FORMAT",
    "MediaArchiveProjection",
    "MediaArchiveProjectionPayload",
    "MediaProjectedValue",
    "MediaProjectionItem",
    "RetainedXmpSidecar",
    "ffmpeg_container_metadata_args",
    "render_projection_xmp",
    "resolve_media_archive_preflight_projection",
    "resolve_media_archive_projection",
    "MediaOutputPublicationDecision",
    "MediaPublicationPlan",
    "MaterializationDecisionRequired",
    "accepted_source_hints",
    "append_leaf_suffix",
    "replace_final_suffix",
    "seal_publication_plan",
    "sibling_hint",
]
