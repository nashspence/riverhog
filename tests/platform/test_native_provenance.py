from __future__ import annotations

import hashlib
from pathlib import Path

from riverhog_provenance import create_journal, new_id, validate_journal

from tests.provenance_observer import native_provenance_observer


def test_live_platform_observer_records_exact_opaque_state(tmp_path: Path) -> None:
    payload = tmp_path / "native-observation.bin"
    content = b"Riverhog live platform provenance\x00\xff\n"
    payload.write_bytes(content)
    observer = native_provenance_observer()

    result = observer.observe(observer.source(payload, host_id=new_id()))
    graph = result.graph_fragment()
    journal = create_journal(
        graph, recorded_by_agent_id=result.observer_agent_id, catalog=result.catalog
    )
    summary = validate_journal(journal, catalog=result.catalog)

    assert summary.graph == graph
    assert len(summary.states) == 1
    assert summary.states[0]["id"] == result.state_id
    assert summary.states[0]["occurrence_id"] == result.occurrence_id
    assert result.occurrence["artifact_id"] == result.artifact_id
    assert result.observation["content"] == {
        "size_bytes": str(len(content)),
        "digests": [{"algorithm": "sha-256", "value": hashlib.sha256(content).hexdigest()}],
    }
    assert result.observation["consistency"]["level"] == "verified_unchanged"
    assert result.observation["profiles"]
    assert graph["contexts"]
    assert graph["locator_bindings"]
    assert "path" not in result.artifact
    assert "path" not in result.occurrence
