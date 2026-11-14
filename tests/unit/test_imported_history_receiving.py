from __future__ import annotations

import pytest

from tests.unit.test_archive_catalog_finalizer import _lose_commit_reply
from tests.unit.test_imported_history_finalization import (
    _observe_terminal,
    _source_material,
    _staged_imports,
)
from tests.unit.test_selected_upload_journal_prefix import _advance_member, _owner, _state


@pytest.mark.parametrize("committed", [False, True])
def test_imported_terminal_receives_original_commit_or_rebuilds_rolled_back_attempt(
    tmp_path, monkeypatch, committed
):
    material = _source_material()
    with _staged_imports(tmp_path, monkeypatch, material, fanout=1, members=1) as case:
        _advance_member(case)
        assert _state(case) == (1, False, None)
        original = _owner(case)
        with _observe_terminal(case, monkeypatch) as first:
            with _lose_commit_reply(monkeypatch, committed=committed):
                with pytest.raises(OSError, match="catalog commit acknowledgement"):
                    original.advance()
        assert original.has_pending and not original.result_resources.pending
        assert _state(case) == (1, committed, None)
        assert len(first.corpora) == len(first.closures) == len(first.imports) == 1
        for corpus in first.corpora:
            assert corpus._closed and corpus._connection_closed and not corpus._owns_directory
            assert not corpus._directory.exists()
        for closure in first.closures:
            assert closure._connection_closed and closure._directory_removed
            assert not closure._root.exists()

        with _observe_terminal(case, monkeypatch) as received:
            assert original.advance()
        if committed:
            assert not received.reads and not received.validations
            assert not received.imports and not received.corpora and not received.closures
            assert received.advances == 0
        else:
            assert received.reads == first.reads
            assert len(received.imports) == 1
            assert received.corpora[0] is not first.corpora[0]
            assert received.closures[0] is not first.closures[0]
            assert received.corpora[0]._connection_closed
            assert not received.corpora[0]._owns_directory
            assert received.closures[0]._connection_closed
            assert received.closures[0]._directory_removed
        assert not original.has_pending and not original.result_resources.pending
        assert _state(case) == (1, True, None)
        with _observe_terminal(case, monkeypatch) as duplicate:
            assert original.advance()
        assert not duplicate.reads and not duplicate.imports and duplicate.advances == 0
