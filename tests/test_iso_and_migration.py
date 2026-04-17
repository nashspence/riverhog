from __future__ import annotations

import sqlite3
from pathlib import Path

from .helpers import create_job, force_flush, register_iso, seal_job, upload_job_file
from .mock_data import document_archive_files


def test_registered_iso_download_tracks_progress(app_factory):
    with app_factory() as harness:
        job_id = create_job(harness, description="downloadable iso archive")
        for sample in document_archive_files():
            upload_job_file(harness, job_id, sample)

        sealed = seal_job(harness, job_id)
        disc_id = (sealed["closed_discs"] or force_flush(harness))[0]

        iso_bytes = b"SIMULATED-ISO-DATA" * 4096
        register_iso(harness, disc_id, iso_bytes)

        create_session = harness.client.post(
            f"/v1/discs/{disc_id}/download-sessions",
            headers=harness.auth_headers(),
        )
        assert create_session.status_code == 200
        session_id = create_session.json()["session_id"]

        download = harness.client.get(
            f"/v1/discs/downloads/{session_id}/content",
            headers=harness.auth_headers(),
        )
        assert download.status_code == 200
        assert download.content == iso_bytes
        assert download.headers["content-disposition"].endswith(f'"{disc_id}.iso"')

        stream_messages = harness.redis_messages(harness.progress.download_stream_name(session_id))
        statuses = [fields["status"] for _, fields in stream_messages]
        assert statuses[0] == "ready"
        assert statuses[-1] == "completed"
        assert "streaming" in statuses[1:-1]
        assert int(stream_messages[-1][1]["bytes_sent"]) == len(iso_bytes)

        with harness.session() as session:
            download_session = session.get(harness.models.DownloadSession, session_id)
            assert download_session is not None
            assert download_session.status == "completed"
            assert download_session.bytes_sent == len(iso_bytes)


def test_schema_migration_adds_critical_columns(module_factory):
    def prepare_legacy_db(env: dict[str, str], _base_dir):
        sqlite_path = env["SQLITE_PATH"]
        Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(sqlite_path)
        conn.executescript(
            """
            CREATE TABLE jobs (
              id TEXT PRIMARY KEY,
              status TEXT NOT NULL,
              description TEXT,
              sealed_at DATETIME,
              created_at DATETIME NOT NULL
            );
            CREATE TABLE discs (
              id TEXT PRIMARY KEY,
              status TEXT NOT NULL,
              description TEXT,
              root_abs_path TEXT NOT NULL,
              contents_hash TEXT NOT NULL,
              total_root_bytes INTEGER NOT NULL,
              cached_root_abs_path TEXT,
              iso_abs_path TEXT,
              iso_size_bytes INTEGER,
              created_at DATETIME NOT NULL
            );
            CREATE TABLE disc_entries (
              id TEXT PRIMARY KEY,
              disc_id TEXT NOT NULL,
              relative_path TEXT NOT NULL,
              kind TEXT NOT NULL,
              size_bytes INTEGER NOT NULL,
              sha256 TEXT NOT NULL
            );
            """
        )
        conn.commit()
        conn.close()

    with module_factory(before_import=prepare_legacy_db) as modules:
        modules.db.Base.metadata.create_all(bind=modules.db.engine)
        modules.db.migrate_schema()

        conn = sqlite3.connect(modules.sqlite_path)
        try:
            job_columns = {row[1] for row in conn.execute("PRAGMA table_info(jobs)")}
            disc_columns = {row[1] for row in conn.execute("PRAGMA table_info(discs)")}
            disc_entry_columns = {row[1] for row in conn.execute("PRAGMA table_info(disc_entries)")}
        finally:
            conn.close()

        assert "keep_buffer_after_archive" in job_columns
        assert "burn_confirmed_at" in disc_columns
        assert "stored_size_bytes" in disc_entry_columns
        assert "stored_sha256" in disc_entry_columns
