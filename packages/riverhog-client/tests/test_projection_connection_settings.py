from __future__ import annotations

import pytest
from riverhog_client.initial_tags import PreparedInitialCollectionTags
from sqlite_admission import SqliteConnectionAdmission, SqliteConnectionSettings
from test_initial_tags_admission import Factory, values


@pytest.mark.parametrize("temporary", ["FILE", "MEMORY"])
def test_initial_tags_profile_precedes_input_and_preserves_complete_identity(temporary):
    admission = SqliteConnectionAdmission(
        connection_limit=1,
        cursor_limit=4,
        operation_limit=1,
        verify_process=lambda: None,
        connection_settings=SqliteConnectionSettings(cache_kib=23, temporary_storage=temporary),
    )
    retained, seen = [], []
    with PreparedInitialCollectionTags(values(205)) as expected:
        identity = expected.tag_set_identity

    def supplied():
        owner = retained[0]
        assert owner._one("PRAGMA cache_size") == (-23,)
        assert owner._one("PRAGMA temp_store") == (1 if temporary == "FILE" else 2,)
        assert owner._one("PRAGMA journal_mode") == ("delete",)
        assert owner._one("PRAGMA synchronous") == (2,)
        seen.append(True)
        yield from reversed(values(205))

    with PreparedInitialCollectionTags(
        supplied(), connection_factory=admission.connect, on_storage_open=retained.append
    ) as owner:
        assert seen == [True] and owner.tag_set_identity == identity
        assert tuple(tag for batch in owner.iter_batches() for tag in batch) == values(205)
    assert owner.cleanup_complete
    assert admission.status.connections == admission.status.cursors == 0


@pytest.mark.parametrize("after", [False, True])
def test_settings_cursor_failure_retains_tag_owner_before_any_input(after):
    factory, consumed = Factory(cursor_fault="PRAGMA database_list", after=after), []

    def supplied():
        consumed.append(True)
        yield "tag"

    with pytest.raises(OSError, match="cursor close unconfirmed"):
        factory.prepare(supplied())
    owner = factory.handles[0]
    assert not consumed and not owner.cleanup_complete
    assert factory.population.active == 1
    assert factory.population.connections[0].close_calls == 0
    uncertain = next(cursor for cursor in factory.cursors if cursor.close_calls == 1)
    owner.close()
    assert uncertain.close_calls == 2
    factory.assert_closed()
