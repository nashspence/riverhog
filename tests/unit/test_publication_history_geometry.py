from __future__ import annotations

from pathlib import Path

import pytest
from riverhog_archive_contracts import BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectRecord
from sqlalchemy import select

from tests.support.qualification.canonical_history_scale import publish_shared_history
from tests.support.qualification.publication_evidence import (
    measure_published_history,
    verify_stored_publication,
)
from tests.support.qualification.publication_geometry import PublicationGeometry


@pytest.mark.parametrize(
    ("fanout", "retained", "required", "documentary"),
    [(1, 0, 0, None), (2, 0, 0, None), (2, 2, 0, None), (2, 2, 2, None), (2, 0, 0, 1)],
)
def test_real_publication_measures_independent_history_dimensions(
    tmp_path: Path,
    record_property,
    monkeypatch: pytest.MonkeyPatch,
    fanout: int,
    retained: int,
    required: int,
    documentary: int | None,
) -> None:
    fixture = publish_shared_history(
        tmp_path,
        members=2,
        geometry=PublicationGeometry(
            (0, 17), fanout, retained, required, shared_claim_members=documentary
        ),
    )
    try:
        observed = measure_published_history(fixture)
        assert observed["members"] == 2
        assert observed["payload_bytes"] == 17
        assert observed["physical_journals"] == 3 + retained
        assert observed["shared_root_attachments"] == fanout
        bound = observed["extents"][BOUND_HISTORY_EXTENT]
        complete = observed["extents"][RETAINED_HISTORY_EXTENT]
        assert bound["unique_journals"] == 3 + required
        assert complete["unique_journals"] == 3 + retained
        claimed = 2 if documentary is None else documentary
        assert observed["shared_documentary_primaries"] == claimed
        assert observed["shared_required_journals"] == 1 + claimed + required
        assert bound["journal_occurrences"] == (
            2 - min(fanout, claimed) + fanout * (1 + claimed + required)
        )
        assert complete["journal_occurrences"] == (
            bound["journal_occurrences"] + retained - required
        )
        stored = observed["stored_publication"]
        assert stored["part_checks"] >= stored["archive_objects"] - stored["whole_object_checks"]
        assert stored["part_checks"] > 0
        record_property("publication_geometry", observed)
        if fanout == 1:
            store = fixture.container.collection_uploads._archive_stores.require("primary").store
            with session_scope(fixture.container.session_factory) as session:
                path = session.scalar(
                    select(CollectionArchiveObjectRecord.object_path)
                    .where(
                        CollectionArchiveObjectRecord.collection_id == fixture.collection_id,
                        CollectionArchiveObjectRecord.store == "primary",
                        CollectionArchiveObjectRecord.archive_parts_json.is_not(None),
                    )
                    .limit(1)
                )
                assert path is not None
            read = store.read_stored

            def corrupted(selected: str) -> bytes:
                raw = read(selected)
                return raw[:-1] + bytes([raw[-1] ^ 1]) if selected == path else raw

            with monkeypatch.context() as scoped:
                scoped.setattr(store, "read_stored", corrupted)
                with pytest.raises((ValueError, AssertionError)):
                    verify_stored_publication(fixture)
            verify_stored_publication(fixture)
    finally:
        fixture.api.close()
        fixture.container.close()


def test_publication_rejects_geometry_for_a_different_collection_size(tmp_path: Path) -> None:
    original = set(tmp_path.iterdir())
    with pytest.raises(ValueError, match="exact collection size"):
        publish_shared_history(
            tmp_path,
            members=3,
            geometry=PublicationGeometry((1, 1), 2),
        )
    assert set(tmp_path.iterdir()) == original
