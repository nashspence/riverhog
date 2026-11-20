from contextlib import ExitStack

from riverhog_provenance import reclaim_retired_journal_workspace as reclaim
from riverhog_provenance.journal_store import JournalStore

from tests.support.validation_workspace import leased_workspace
from tests.support.workspace_reclamation import retained_file_receivers as retained_file_receivers


def test_real_result_reader_prevents_the_owner_from_entering_reclamation(
    tmp_path, *, journal_constructions, retained_file_receivers
):
    root, attempt = tmp_path / "root", "a" * 32
    owner = ExitStack()
    path = owner.enter_context(leased_workspace(root, attempt, create=True))
    try:
        store = JournalStore.open_retained(
            path,
            create=True,
            on_storage_close=owner.close,
            construction=journal_constructions.retain(),
        )
    except BaseException:
        owner.close()
        raise
    store.freeze()
    reader = store.borrow()
    store.close()
    try:
        with leased_workspace(root, attempt) as busy:
            assert busy is None
        assert reader.db.execute("SELECT 1").fetchone() == (1,)
    finally:
        reader.close()
    with leased_workspace(root, attempt) as retired:
        while not reclaim(retired, receiver=retained_file_receivers()).files_reclaimed:
            pass
    assert not (path / "snapshot.sqlite3").exists() and (path / ".lease").is_file()
