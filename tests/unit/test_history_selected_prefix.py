"""Transfer authenticates the selected history prefix without consuming a later tail."""

from __future__ import annotations

import hashlib
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_client.processing.history_transfer import CanonicalHistoryTransfer
from riverhog_provenance import BoundedSourceObserver, BytesSource, create_journal, validate_journal


@pytest.mark.parametrize("valid", [True, False])
def test_history_transfer_validates_only_the_exact_selected_receiver_prefix(valid: bool) -> None:
    observed = BoundedSourceObserver().observe(BytesSource(b"source"))
    content = create_journal(
        observed.graph_fragment(), recorded_by_agent_id=observed.observer_agent_id
    )
    selected = HistoryJournalAnchor.from_mapping(validate_journal(content).anchor)
    prefix = content if valid else b"x" * len(content)
    suffix = b"unselected later prefix"

    class Api:
        def get_collection_upload_session_provenance_journal(self, collection_id, journal_id):
            return SimpleNamespace(
                state="sealed",
                bytes=len(prefix) + len(suffix),
                sha256=hashlib.sha256(prefix + suffix).hexdigest(),
            )

        @contextmanager
        def stream_collection_upload_session_provenance_journal(self, collection_id, journal_id):
            def chunks():
                yield prefix
                pytest.fail("the transfer must not consume an unselected history tail")

            yield chunks()

    transfer = CanonicalHistoryTransfer(Api(), 1)
    if valid:
        transfer._journal(None, selected)
    else:
        with pytest.raises(ValueError, match="conflicts"):
            transfer._journal(None, selected)
