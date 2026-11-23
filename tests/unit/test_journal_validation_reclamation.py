from contextlib import ExitStack

from riverhog_provenance import reclaim_retired_journal_workspace as reclaim
from riverhog_provenance._generation_storage import NativeGenerationStorage
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


def test_original_generation_reader_keeps_workspace_lease_through_bounded_retirement(
    tmp_path, *, journal_constructions, retained_file_receivers
):
    root, attempt = tmp_path / "root", "a" * 32
    owner = ExitStack()
    path = owner.enter_context(leased_workspace(root, attempt, create=True))
    original = journal_constructions.retain()
    reader = None
    try:
        store = NativeGenerationStorage.open(
            path,
            construction=original,
            page_size=512,
            reader_limit=1,
            metadata_octets=8192,
            create=True,
            on_storage_close=owner.close,
        )

        def image(epoch):
            store.roots.begin("original-grant:1")
            store.roots.replace("original-grant:1", 1, b"x" * 512)
            return store.roots.freeze("original-grant:1")

        with store.execution():
            selected = store.epoch.apply(image, receive=lambda result: None)
        reader = store.roots.borrow("original-grant:1", expected_root=selected)
        assert not original.close_step()
        assert not original.received and not store.storage_received
        with leased_workspace(root, attempt) as busy:
            assert busy is None
        assert reader.read(after=0, page_limit=1).pages == ((1, b"x" * 512),)
        reader.close()
        assert original.close_step() and store.storage_received
    finally:
        if reader is not None:
            reader.close()
        assert original.close_step()
        owner.close()
    before = (path / "generations.sqlite3").stat().st_size
    removed = 0
    with leased_workspace(root, attempt) as retired:
        while True:
            progress = reclaim(retired, octet_budget=4096, receiver=retained_file_receivers())
            assert progress.removed_octets <= 4096 and progress.removed_files <= 1
            removed += progress.removed_octets
            if progress.files_reclaimed:
                break
    assert removed == before
    assert not (path / "generations.sqlite3").exists() and (path / ".lease").is_file()
