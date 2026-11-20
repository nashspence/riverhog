"""Ordered pending selection for initial collection tag publication."""

from __future__ import annotations

from typing import Literal

from sqlalchemy import Select, select

from riverhog_core.catalog_models import CollectionUploadTagPublicationFrontierRecord as Frontier


def pending_initial_tag_query(
    collection_id: int, *, work: Literal["expand", "publish"]
) -> Select[tuple[Frontier]]:
    """Select one pending node through its matching ordered partial index."""
    if work == "expand":
        pending = Frontier.expanded.is_(False)
    elif work == "publish":
        pending = Frontier.published.is_(False)
    else:
        raise ValueError("unsupported initial tag frontier work")
    return (
        select(Frontier)
        .where(Frontier.collection_id == collection_id, pending)
        .order_by(Frontier.node_digest)
        .limit(1)
    )
