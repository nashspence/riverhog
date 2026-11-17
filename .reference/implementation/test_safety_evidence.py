"""Current-owner evidence; production placement is the Native retained-session tests."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    IncompleteSourceError,
    create_journal,
    validate_journal,
)
from riverhog_provenance import _session_checkpoint as checkpoint_owner
from riverhog_provenance._session_checkpoint import StaleValidationCheckpoint
from riverhog_provenance_contracts import ContractCatalog
from safety_evidence import CUTS, ExactSource, open_session, run_retained_journal_kill

from tests.support.native_construction import JournalConstructions


@pytest.fixture(scope="module")
def original_journal():
    catalog = ContractCatalog()
    observation = BoundedSourceObserver(catalog=catalog).observe(
        BytesSource(b"#1000 actual retained Native SIGKILL fixture\x00\xff")
    )
    return create_journal(
        observation.graph_fragment(),
        recorded_by_agent_id=observation.observer_agent_id,
        catalog=catalog,
    )


@pytest.mark.skipif(
    os.name != "posix", reason="actual POSIX SIGKILL witness; Windows has its own gate"
)
@pytest.mark.parametrize("cut", CUTS)
def test_actual_child_kill_preserves_atomic_source_and_parent(tmp_path, original_journal, cut):
    result = run_retained_journal_kill(tmp_path / cut, original_journal, cut)
    with validate_journal(original_journal) as direct:
        digest = hashlib.sha256(
            json.dumps(direct.materialize(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    assert result["result_sha256"] == digest
    output = os.environ.get("RIVERHOG_REFERENCE_SAFETY_OUTPUT")
    if output:
        path = Path(output)
        path.mkdir(parents=True, exist_ok=True)
        target = path / ("safety_" + cut + ".json")
        with target.open("x", encoding="utf-8") as retained:
            json.dump(result, retained, indent=2, sort_keys=True)
            retained.write("\n")


@pytest.mark.parametrize(
    "mismatch", ["source_version", "source_current", "worker_epoch", "algorithm"]
)
def test_changed_authority_cannot_adopt_or_rewrite_original(
    tmp_path, original_journal, monkeypatch, mismatch
):
    from riverhog_provenance.journal_session import JournalValidationBudget

    catalog = ContractCatalog()
    directory = tmp_path / "original"
    directory.mkdir(mode=0o700)
    source = ExactSource(original_journal, catalog)
    originals = JournalConstructions()
    try:
        with open_session(directory, source, catalog, originals, create=True, epoch=0) as session:
            session.advance(JournalValidationBudget(rows=1, source_octets=7))
    finally:
        originals.close()
    database = directory / "snapshot.sqlite3"
    before = database.read_bytes()
    rejected = ExactSource(
        original_journal, catalog, version="8" if mismatch == "source_version" else "7"
    )
    rejected.current = mismatch != "source_current"
    epoch = 0 if mismatch == "worker_epoch" else 1
    if mismatch == "algorithm":
        monkeypatch.setattr(checkpoint_owner, "ALGORITHM", "reference-unsupported-algorithm/v0")
    failures = (ValueError, RuntimeError, IncompleteSourceError, StaleValidationCheckpoint)
    rejection = JournalConstructions()
    try:
        with pytest.raises(failures):
            open_session(directory, rejected, catalog, rejection, create=False, epoch=epoch)
    finally:
        rejection.close()
    assert database.read_bytes() == before
    assert not rejected.calls
    assert not rejection.frames


def test_reference_child_refuses_an_unsupported_cut_before_storage(tmp_path, original_journal):
    with pytest.raises(ValueError):
        run_retained_journal_kill(tmp_path / "absent", original_journal, "unsupported")
    assert not (tmp_path / "absent").exists()
