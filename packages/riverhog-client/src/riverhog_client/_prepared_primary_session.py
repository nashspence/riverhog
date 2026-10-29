"""Durable creation binding before a producer performs remote construction work."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from typing import Any

from riverhog_canonical_json import canonical_json_bytes
from riverhog_protocol import CollectionDescription, CollectionTag
from riverhog_protocol.storage_names import ArchiveStoreName

from riverhog_client._prepared_primary_documents import PrimarySession, primary_control_bytes
from riverhog_client._prepared_primary_storage import PrimaryStorage
from riverhog_client._prepared_primary_store import PrimaryStore
from riverhog_client._prepared_primary_types import PrimaryStateError
from riverhog_client.canonical_production import ProducerAttribution
from riverhog_client.client import ApiClient
from riverhog_client.initial_tags import prepare_initial_collection_tags


def open_primary_session(
    api: ApiClient,
    store: PrimaryStore,
    *,
    attribution: ProducerAttribution,
    idempotency_key: str | None,
    archive_store: ArchiveStoreName | None,
    use_cache: bool | None,
    copy_to: Sequence[ArchiveStoreName] | None,
    description: CollectionDescription | None,
    tags: Sequence[CollectionTag],
    event_context: Mapping[str, object] | None,
    storage_preparation: Callable[[PrimaryStorage], bool] | None = None,
) -> tuple[dict[str, Any], ProducerAttribution]:
    """Compare the complete creation intent before create, tag or completion writes."""
    initial_identity = hashlib.sha256(
        (idempotency_key or attribution.source_event_id).encode("utf-8")
    ).hexdigest()
    if attribution.construction_identity != initial_identity:
        raise PrimaryStateError("producer attribution differs from its original retry identity")
    requirement = attribution.completion_requirement
    if requirement is not None:
        requirement = type(requirement).model_validate_json(primary_control_bytes(requirement))
    attribution = replace(
        attribution,
        source_context=json.loads(canonical_json_bytes(dict(attribution.source_context))),
        completion_requirement=requirement,
    )
    event_context = (
        None if event_context is None else json.loads(canonical_json_bytes(dict(event_context)))
    )
    copy_to = None if copy_to is None else tuple(copy_to)
    requirement_identity = None if requirement is None else requirement.identity
    with prepare_initial_collection_tags(tags) as initial_tags:
        request = canonical_json_bytes(
            {
                "format": "riverhog-prepared-primary-creation/v1",
                "idempotency_key": idempotency_key or initial_identity,
                "custody_mode": "custody-transfer",
                "ingest_source": attribution.ingest_source,
                "description": description,
                "initial_tag_set_identity": initial_tags.tag_set_identity,
                "archive_store": archive_store,
                "use_cache": use_cache,
                "copy_to": None if copy_to is None else sorted(copy_to),
                "event_context": None if event_context is None else dict(event_context),
                "producer": {
                    "producer_app": attribution.producer_app,
                    "adapter_id": attribution.adapter_id,
                    "adapter_version": attribution.adapter_version,
                    "source_event_id": attribution.source_event_id,
                    "source_context": dict(attribution.source_context),
                    "initial_construction_identity": initial_identity,
                    "completion_requirement": None
                    if requirement is None
                    else requirement.model_dump(mode="json"),
                },
            }
        )
        retained = store.session_for(request, storage_preparation=storage_preparation)
        fresh = retained is None
        batches = initial_tags.iter_batches()
        first_batch = next(batches, ())
        if retained is None:
            session = api.create_or_resume_collection_upload_session(
                idempotency_key or initial_identity,
                ingest_source=attribution.ingest_source,
                description=description,
                tags=first_batch,
                initial_tag_set_identity=initial_tags.tag_set_identity,
                archive_store=archive_store,
                use_cache=use_cache,
                copy_to=copy_to,
                event_context=event_context,
                custody_mode="custody-transfer",
            )
            if session.get("resumed") is not False or session.get("state") != "open":
                raise PrimaryStateError(
                    "initial creation has no accepted local header; retain state for reconciliation"
                )
            actual_identity = (
                initial_identity
                if requirement is None
                else session.get("construction_identity_sha256")
            )
            retained = PrimarySession.model_validate(
                {
                    "allocation_sha256": hashlib.sha256(
                        primary_control_bytes(store.allocation)
                    ).hexdigest(),
                    "create_request": {
                        "bytes": str(len(request)),
                        "sha256": hashlib.sha256(request).hexdigest(),
                    },
                    "collection_id": session.get("collection_id"),
                    "delivery_context_id": session.get("delivery_context_id"),
                    "server_construction_identity_sha256": session.get(
                        "construction_identity_sha256"
                    ),
                    "attribution_construction_identity_sha256": actual_identity,
                    "completion_requirement_sha256": requirement_identity,
                }
            )
            store.bind_session(retained)
        else:
            # A bound allocation never performs create-or-resume under a changed principal.
            session = api.get_collection_upload_session(retained.collection_id)
            actual_identity = (
                initial_identity
                if requirement is None
                else retained.server_construction_identity_sha256
            )
            if (
                session.get("collection_id") != str(retained.collection_id)
                or session.get("delivery_context_id") != retained.delivery_context_id
                or session.get("construction_identity_sha256")
                != retained.server_construction_identity_sha256
                or actual_identity != retained.attribution_construction_identity_sha256
                or requirement_identity != retained.completion_requirement_sha256
            ):
                raise PrimaryStateError("current session differs from its accepted local header")
        attribution = replace(
            attribution, construction_identity=retained.attribution_construction_identity_sha256
        )
        if session.get("state") == "open":
            if requirement is not None:
                api.set_collection_upload_session_completion_requirement(
                    retained.collection_id, requirement
                )
            # Fresh creation consumed the first batch; a bound-header retry replays every batch.
            if not fresh and first_batch:
                api.add_collection_upload_session_tags(retained.collection_id, first_batch)
            for batch in batches:
                api.add_collection_upload_session_tags(retained.collection_id, batch)
        elif session.get("state") != "finalized":
            raise PrimaryStateError("accepted session cannot publish or expose terminal cleanup")
        session["resumed"] = not fresh
        return session, attribution
