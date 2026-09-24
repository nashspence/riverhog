"""Exact admission and departure selection within one Riverhog catalog view."""

from __future__ import annotations

from riverhog_client import ApiClient
from riverhog_protocol import CatalogSyncDescriptor
from stove0_operator_contracts import AdmissionSelector, TaggedAdmissionSelector


def catalog_selector_matches(
    riverhog: ApiClient,
    selector: AdmissionSelector,
    descriptor: CatalogSyncDescriptor,
) -> bool:
    if not isinstance(selector, TaggedAdmissionSelector):
        return True
    for tag in selector.required:
        response = riverhog.collection_contains_tag(
            descriptor.collection_id,
            tag=tag,
            revision=descriptor.tag_revision,
            tag_set_identity=descriptor.tag_set_identity,
        )
        if (
            response.get("collection_id") != str(descriptor.collection_id)
            or response.get("revision") != descriptor.tag_revision
            or response.get("tag_set_identity") != descriptor.tag_set_identity
            or response.get("tag") != tag
            or not isinstance(response.get("present"), bool)
        ):
            raise RuntimeError("Riverhog tag membership response changed its authority")
        if not response["present"]:
            return False
    return True


__all__ = ["catalog_selector_matches"]
