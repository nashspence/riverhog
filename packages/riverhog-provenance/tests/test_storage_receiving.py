from __future__ import annotations

import pytest
from riverhog_provenance.journal_store import JournalStore


@pytest.mark.parametrize("callback_failure", [None, "before", "after"])
def test_storage_receiving_waits_for_last_reader_and_actual_callback(
    tmp_path, callback_failure, journal_constructions
):
    directory = tmp_path / "retained"
    directory.mkdir(mode=0o700)
    original = JournalStore.open_retained(
        directory, create=True, construction=journal_constructions.retain()
    )
    calls, effects = [], []

    def receive():
        calls.append(1)
        if callback_failure == "before" and len(calls) == 1:
            raise InterruptedError("original callback receiving is unconfirmed")
        effects.append(1)
        if callback_failure == "after" and len(calls) == 1:
            raise InterruptedError("original callback receiving reply was lost")

    original._storage.on_close = receive
    original.freeze()
    first, last = original.borrow(), original.borrow()
    try:
        original.close()
        assert original.cleanup_complete and not original.storage_received
        assert not first.storage_received and not last.storage_received and not calls
        first.close()
        assert first.cleanup_complete and not first.storage_received and not calls
        if callback_failure is not None:
            with pytest.raises(InterruptedError):
                last.close()
            assert not last.cleanup_complete and not last.storage_received
            assert not original.storage_received and not first.storage_received
            assert original._storage._database_closed
            assert len(effects) == int(callback_failure == "after")
        last.close()
        assert last.cleanup_complete
        assert original.storage_received and first.storage_received and last.storage_received
        assert len(calls) == (1 if callback_failure is None else 2)
        assert directory.exists()
    finally:
        first.close()
        last.close()
        original.close()
