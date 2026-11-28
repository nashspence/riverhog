"""Reproduce why exact replay binding alone cannot replace stage restoration.

Run from a clean checkout of the audited base. The probes never call the proposed
prepared predicate; they compare existing owner restoration with existing cold
certificate construction. PostgreSQL compilation is not database execution.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.repo.resolve()
    base = "60612eded1e0e4e5188c28a84697435eb969fad0"

    def git(*arguments: str) -> bytes:
        return subprocess.check_output(["git", "-C", str(root), *arguments])

    assert git("rev-parse", "HEAD").decode().strip() == base
    assert not git("status", "--porcelain"), "Use an unmodified audited checkout"
    sys.path[:0] = [str(root), *[str(p) for p in root.rglob("src") if p.is_dir()]]

    from sqlalchemy import update
    from sqlalchemy.dialects import postgresql, sqlite

    from riverhog_core.services import collection_uploads as source_owner
    from riverhog_core.services import upload_journal_certificates as certificates
    from riverhog_core.services import upload_journal_epochs as epochs
    from riverhog_core.services import upload_journal_rank as rank
    from riverhog_core.services import upload_journal_replay as replay
    from riverhog_core.services import upload_journal_topology as topology
    from riverhog_core.services import upload_journal_union as union
    from tests.unit.test_journal_capture import retained
    from tests.unit.test_journal_summary_consumers import source
    from tests.unit.test_staged_journal_range import JOURNAL
    from tests.unit.test_upload_journal_certificates import ready

    source_identities = {}
    for module in (source_owner, certificates, epochs, rank, replay, topology, union):
        path = Path(module.__file__).resolve()
        relative = str(path.relative_to(root))
        raw = path.read_bytes()
        assert raw == git("show", f"{base}:{relative}"), relative
        source_identities[relative] = hashlib.sha256(raw).hexdigest()

    observations = []
    queries = []
    fixture_directories = []

    class Result:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def one_or_none(self):
            return None

    class Session:
        def execute(self, query):
            queries.append(query)
            return Result()

    @contextmanager
    def capture_query(_factory):
        yield Session()

    for field, checker in (
        ("algorithm", epochs._begin_scan),
        ("topology_algorithm", topology._begin),
    ):
        with tempfile.TemporaryDirectory(prefix="prep-cold-audit-") as tmp:
            fixture_directories.append(Path(tmp))
            with retained(Path(tmp), source(journal_id=JOURNAL)) as (engine, factory):
                work, _ = ready(factory)
                catalog = source_owner.admission_provenance_catalog()
                current, raw = replay._probe(factory, work)
                assert current and raw is not None
                with factory() as session, session.begin():
                    session.execute(
                        update(certificates.Scan).values({field: "wrong-algorithm"})
                    )
                rejected = None
                try:
                    checker(factory, work)
                except RuntimeError as error:
                    rejected = str(error)
                assert rejected
                assert replay.verify_upload_journal_replay_binding(factory, work, catalog)
                cold = certificates.UploadJournalGraphCertificates(
                    factory, work, catalog=catalog
                )
                assert cold.verify_current()
                observations.append(
                    {
                        "field": field,
                        "original_restoration_rejected": rejected,
                        "binding_verifier_current": True,
                        "cold_certificate_froze_changed_manifest": True,
                        "catalog_connections_checked_out": engine.pool.checkedout(),
                    }
                )
                assert engine.pool.checkedout() == 0
                if not queries:
                    with patch.object(replay, "read_snapshot", capture_query):
                        replay._probe(factory, work)
            assert engine.pool.checkedout() == 0

    assert all(not path.exists() for path in fixture_directories)
    assert not git("status", "--porcelain")
    for relative, digest in source_identities.items():
        assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == digest
    dialects = {
        name: str(
            queries[0].compile(dialect=dialect(), compile_kwargs={"literal_binds": False})
        )
        for name, dialect in (("sqlite", sqlite.dialect), ("postgresql", postgresql.dialect))
    }
    output = {
        "base": base,
        "source_relationship": "all imported Core owner files match the clean audited base",
        "source_identities_sha256": source_identities,
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "cold_shortcut_counterexamples": observations,
        "point_query_dialects": dialects,
        "postgresql_executed": False,
        "all_temporary_fixture_directories_removed": True,
        "scope": "metadata admission counterexamples; no incorrect semantic output claim",
    }
    args.output.write_text(json.dumps(output, indent=2) + "\n")
    print(
        json.dumps(
            {
                "base": base,
                "observations": observations,
                "postgresql_compiled": True,
                "postgresql_executed": False,
                "output": str(args.output),
            }
        )
    )


if __name__ == "__main__":
    main()
