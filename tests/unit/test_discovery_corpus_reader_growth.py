from __future__ import annotations

import pytest
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner
from riverhog_provenance import JournalSummary

from tests.unit.test_canonical_discovery_corpus_source import case as case
from tests.unit.test_canonical_discovery_corpus_source import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_corpus_source import original as original
from tests.unit.test_canonical_discovery_corpus_source import qualified as qualified
from tests.unit.test_canonical_discovery_corpus_source import seed
from tests.unit.test_discovery_corpus_replay import prepare_source
from tests.unit.test_discovery_validation_owner import unit
from tests.unit.test_discovery_validation_owner_growth import Recorder, Slots, dimensions


@pytest.mark.parametrize("retained", [1, 1025])
@pytest.mark.parametrize("lost_reply", [False, True])
@pytest.mark.growth_witness(
    "riverhog-discovery-corpus-reader-release-lifecycle/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationOwner.close",
)
def test_actual_borrower_release_and_retry_preserve_composed_owner_close_bound(
    case, qualified, retained, lost_reply, monkeypatch, record_property
):
    samples, recorder = [], Recorder()
    app = DiscoveryValidationOwner(case.factory, case.root)
    app._slots = Slots(recorder)
    case.pool, case.rec, case.readers = app, recorder, []
    actual, failures = JournalSummary.close, 2
    try:
        with recorder.connect_only():
            with unit(app, case) as work:
                selection, _ = seed(work.metadata, qualified)
                case.readers.append(selection.history.reader)
                prepare_source(work, selection)
                work.corpus_replay(selection)
                slot = work._slot
                with work.metadata.transaction() as (db, key):
                    source = db.execute(
                        "SELECT journal_id,source_key,source_binding_json,anchor_json "
                        "FROM discovery_corpus_source_inputs "
                        "WHERE build_id=? AND admission_sha256=?",
                        (key.build_id, key.admission_sha256),
                    ).fetchone()
                    for ordinal in range(retained):
                        db.execute(
                            "INSERT INTO discovery_corpus_source_inputs VALUES(?,?,?,?,?,?)",
                            ("independent-" + str(ordinal), key.admission_sha256, *source),
                        ).close()
                borrowed = slot.journal.session.result()
                slot.replay_summary = borrowed

            def uncertain(summary):
                nonlocal failures
                if summary is borrowed and failures:
                    failures -= 1
                    if lost_reply:
                        actual(summary)
                    raise OSError("source release unconfirmed")
                return actual(summary)

            with monkeypatch.context() as patch:
                patch.setattr(JournalSummary, "close", uncertain)
                for _retry in range(2):
                    values = dimensions(case, retained=retained, width=0)
                    with recorder.observe(case, cold=True):
                        with pytest.raises(OSError, match="unconfirmed"):
                            app.close()
                    samples.append(recorder.sample(DiscoveryValidationOwner.close, values))
                    assert app.active_build_ids() == (case.build_id,)
                    assert slot.replay_summary is borrowed
                values = dimensions(case, retained=retained, width=0)
                with recorder.observe(case, cold=True):
                    app.close()
                samples.append(recorder.sample(DiscoveryValidationOwner.close, values))
                assert app.active_build_ids() == () and slot.replay_summary is None
    finally:
        app.close()
    record_property("growth_samples", samples)
