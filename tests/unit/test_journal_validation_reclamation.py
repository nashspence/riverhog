from contextlib import ExitStack

from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
from riverhog_provenance import reclaim_retired_journal_workspace as reclaim
from riverhog_provenance.journal_store import JournalStore


def test_real_result_reader_prevents_the_owner_from_entering_reclamation(tmp_path):
    root, attempt = tmp_path / "root", "a" * 32
    owner = ExitStack()
    path = owner.enter_context(lease_journal_validation_workspace(root, attempt, create=True))
    try:
        store = JournalStore.open_retained(path, create=True, on_storage_close=owner.close)
    except BaseException:
        owner.close()
        raise
    store.freeze()
    reader = store.borrow()
    store.close()
    try:
        with lease_journal_validation_workspace(root, attempt) as busy:
            assert busy is None
        assert reader.db.execute("SELECT 1").fetchone() == (1,)
    finally:
        reader.close()
    with lease_journal_validation_workspace(root, attempt) as retired:
        while not reclaim(retired).files_reclaimed:
            pass
    assert not (path / "snapshot.sqlite3").exists() and (path / ".lease").is_file()
