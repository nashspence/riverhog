"""Receive actual archived histories and ciphertext commitments after publication."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable

from riverhog_age import iter_decrypt_age_scrypt
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    RETAINED_HISTORY_EXTENT,
    MemberHistoryBinding,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectRecord,
    CollectionRecord,
    CollectionUploadRecord,
)
from riverhog_core.domain.archive import StoredArchivePart
from riverhog_core.raw_verification import (
    _consume_verified_plaintext_parts,
    _iter_verified_stored_parts,
)
from riverhog_protocol.collection_production_provenance import (
    COLLECTION_MEMBER_ROLE,
    collection_production_contract,
)
from riverhog_provenance import MemberHistoryClosure, validate_journal_chunks
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy import select

from tests.support.qualification.canonical_history_scale import SharedHistoryFixture
from tests.support.uploaded_archive import UploadedArchiveStore


def verify_stored_publication(fixture: SharedHistoryFixture) -> dict[str, int]:
    """Read real ciphertext and check whole-object or ordered-part commitments."""
    container = fixture.container
    store = container.collection_uploads._archive_stores.require("primary").store
    assert isinstance(store, UploadedArchiveStore)
    whole = parts_checked = plaintext_bytes = stored_bytes = 0
    with session_scope(container.session_factory) as session:
        collection = session.get(CollectionRecord, fixture.collection_id)
        assert collection is not None
        passphrase = store.passphrases[collection.passphrase_id]
        objects = list(
            session.scalars(
                select(CollectionArchiveObjectRecord).where(
                    CollectionArchiveObjectRecord.collection_id == fixture.collection_id,
                    CollectionArchiveObjectRecord.store == "primary",
                )
            )
        )
        assert objects
        for row in objects:
            raw = store.read_stored(row.object_path)
            assert len(raw) == row.stored_bytes
            if row.archive_parts_json is not None:
                receipts = tuple(
                    StoredArchivePart(**part) for part in json.loads(row.archive_parts_json)
                )
                assert receipts
                digest = hashlib.sha256()
                decrypted = iter_decrypt_age_scrypt(
                    _iter_verified_stored_parts((raw,), receipts),
                    passphrase,
                )
                verified = _consume_verified_plaintext_parts(
                    decrypted,
                    receipts,
                    artifact_digest=digest,
                )
                assert verified == row.plaintext_bytes
                expected_start = 0
                for part in receipts:
                    assert part.plaintext_start == expected_start
                    expected_start += part.plaintext_bytes
                assert expected_start == verified
                parts_checked += len(receipts)
            else:
                assert hashlib.sha256(raw).hexdigest() == row.stored_sha256
                assert (
                    sum(
                        len(chunk)
                        for chunk in store.iter_archive_object(
                            collection_id=fixture.collection_id,
                            object=row,
                            passphrase_id=collection.passphrase_id,
                        )
                    )
                    == row.plaintext_bytes
                )
                whole += 1
            plaintext_bytes += row.plaintext_bytes
            stored_bytes += row.stored_bytes
    return {
        "archive_objects": len(objects),
        "whole_object_checks": whole,
        "part_checks": parts_checked,
        "plaintext_archive_bytes": plaintext_bytes,
        "stored_archive_bytes": stored_bytes,
    }


def measure_published_history(
    fixture: SharedHistoryFixture,
    *,
    record: Callable[[dict[str, object]], None] | None = None,
) -> dict[str, object]:
    """Measure Native closure separately from requested fixture geometry.

    This exhaustive observer retains identity sets and uses the in-memory test
    store. It establishes exact work/output, not a process-memory bound or
    production throughput. Verification timing is separate from publication.
    """
    started = time.perf_counter()
    assert all(not path.exists() for path in fixture.source_paths)
    with session_scope(fixture.container.session_factory) as session:
        assert session.get(CollectionUploadRecord, fixture.collection_id) is None
    archives = fixture.container.provenance._archives
    root = archives.archive_root_preimage(fixture.collection_id, attribution=None)
    assert hashlib.sha256(root).hexdigest() == fixture.archive_root_sha256
    result: dict[str, object] = {
        "members": len(fixture.payload_identities),
        "payload_bytes": sum(value[0] for value in fixture.payload_identities.values()),
        "source_and_upload_scratch_removed": True,
        "archive_root_sha256": fixture.archive_root_sha256,
        "stored_publication": verify_stored_publication(fixture),
    }
    reader = archives.reader(fixture.collection_id, attribution=None)
    catalog = ContractCatalog((collection_production_contract(),))
    with reader.cached() as cached, cached.prepared() as prepared:
        bindings = tuple(MemberHistoryBinding.from_mapping(row) for row in prepared.iter_bindings())
        assert {b.artifact_id: (b.bytes, b.sha256) for b in bindings} == fixture.payload_identities
        assert {b.artifact_id: b for b in bindings} == {
            b.artifact_id: b for b in fixture.history_bindings
        }
        physical_entries = set()
        physical_bytes = 0
        journal_ids = set(prepared.iter_journal_ids())
        for journal_id in sorted(journal_ids):
            size, _digest = prepared.journal_metadata(journal_id)
            physical_bytes += size
            with validate_journal_chunks(
                prepared.iter_journal_range(journal_id),
                catalog=catalog,
            ) as summary:
                physical_entries.update(
                    (journal_id, frame.reference["entry_id"]) for frame in summary.frames
                )
        assert physical_bytes == fixture.journal_bytes
        result.update(
            {
                "physical_journals": len(journal_ids),
                "physical_entries": len(physical_entries),
                "physical_journal_bytes": physical_bytes,
            }
        )
        shared_attachments = 0
        extents = {}
        for extent in (BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT):
            required_journals: dict[str, int] = {}
            required_entries = set()
            occurrences = logical_bytes = 0
            per_member = []
            for binding in bindings:
                descriptor = prepared.member_history(binding)
                assert descriptor.identity == binding.history_sha256
                if extent == BOUND_HISTORY_EXTENT:
                    shared_attachments += sum(
                        selected.journal.journal_id == fixture.shared_journal_id
                        for selected in prepared.iter_selected_history_roots(binding, extent=extent)
                    )
                with MemberHistoryClosure(
                    prepared.history_store(),
                    lambda journal, size: prepared.iter_journal_range(journal, size=size),
                    member_role=COLLECTION_MEMBER_ROLE,
                    catalog=catalog,
                ) as closure:
                    closure.resolve(binding, extent=extent)
                    anchors = tuple(closure.journal_anchors())
                    occurrences += len(anchors)
                    logical_bytes += sum(anchor.prefix_bytes for anchor in anchors)
                    for anchor in anchors:
                        required_journals[anchor.journal_id] = max(
                            required_journals.get(anchor.journal_id, 0),
                            anchor.prefix_bytes,
                        )
                        with closure.summary_at(anchor) as summary:
                            required_entries.update(
                                (anchor.journal_id, frame.reference["entry_id"])
                                for frame in summary.frames
                            )
                    per_member.append(
                        {
                            "artifact_id": binding.artifact_id,
                            "journals": len(anchors),
                            "journal_bytes": sum(anchor.prefix_bytes for anchor in anchors),
                        }
                    )
            assert set(required_journals) <= journal_ids
            assert required_entries <= physical_entries
            extents[extent] = {
                "unique_journals": len(required_journals),
                "unique_entries": len(required_entries),
                "unique_journal_bytes": sum(required_journals.values()),
                "journal_occurrences": occurrences,
                "logical_journal_bytes": logical_bytes,
                "per_member": per_member,
            }
        assert (
            extents[BOUND_HISTORY_EXTENT]["logical_journal_bytes"] == fixture.logical_journal_bytes
        )
        result.update({"shared_root_attachments": shared_attachments, "extents": extents})
    result["verification_seconds"] = time.perf_counter() - started
    if record is not None:
        record(result)
    return result
