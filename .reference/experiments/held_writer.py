"""Observe real app-key authentication while its SQLite writer remains held."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from datetime import timedelta
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--expect", required=True, choices=("busy", "authenticated"))
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    root = args.source.resolve()
    source_sha = subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
    ).strip()
    status = subprocess.check_output(
        ["git", "-C", str(root), "status", "--porcelain=v1", "--untracked-files=all"], text=True
    )
    if status:
        raise RuntimeError("witness requires a clean source checkout")
    sys.path.insert(0, str(root))

    from riverhog_core.app_permissions import CATALOG_READ, ApplicationAccess, Principal
    from riverhog_core.catalog_db import (
        dispose_session_factory,
        initialize_db,
        make_session_factory,
        session_scope,
    )
    from riverhog_core.catalog_models import AppKeyRecord
    from riverhog_core.runtime_config import RuntimeConfig
    from riverhog_core.services import app_keys
    from sqlalchemy.exc import OperationalError
    from time_formats import format_utc_timestamp, utc_now

    from tests.support.runtime_config import write_test_runtime_config

    module_path = Path(app_keys.__file__).resolve()
    if module_path != root / "riverhog/src/riverhog_core/services/app_keys.py":
        raise RuntimeError("environment imports app-key service from a different source")
    report: dict[str, object] = {
        "format": "riverhog-app-key-held-writer-reference/v1",
        "source_sha": source_sha,
        "source_clean": True,
        "service_source_sha256": hashlib.sha256(module_path.read_bytes()).hexdigest(),
        "witness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "python_version": sys.version.split()[0],
        "sqlite_version": sqlite3.sqlite_version,
        "sqlalchemy_version": importlib.metadata.version("SQLAlchemy"),
        "expected_active_outcome": args.expect,
        "timing_is_observation_only": True,
    }
    previous_config = os.environ.get("RIVERHOG_CONFIG")
    try:
        with tempfile.TemporaryDirectory(prefix="riverhog-key-writer-witness-") as temporary:
            directory = Path(temporary)
            os.environ["RIVERHOG_CONFIG"] = str(write_test_runtime_config(directory))
            database = directory / "catalog.sqlite3"
            config = RuntimeConfig.for_testing(database_url=f"sqlite:///{database}")
            initialize_db(config.database_url)
            factory = make_session_factory(config.database_url)
            try:
                service = app_keys.SqlAlchemyAppKeyService(config, session_factory=factory)
                grantor = Principal(
                    id="witness-bootstrap",
                    key_id=None,
                    access=frozenset(),
                    unrestricted_delegation=True,
                )
                keys = {
                    name: service.create(
                        app=f"witness-{name}",
                        access=(ApplicationAccess(CATALOG_READ),),
                        grantor=grantor,
                    )
                    for name in ("active", "revoked", "expired", "deleted")
                }
                service.revoke(app="witness-revoked", key_id=str(keys["revoked"]["id"]))
                with session_scope(factory) as session:
                    expired = session.get(AppKeyRecord, str(keys["expired"]["id"]))
                    assert expired is not None
                    expired.expires_at = format_utc_timestamp(utc_now() - timedelta(days=1))
                    deleted = session.get(AppKeyRecord, str(keys["deleted"]["id"]))
                    assert deleted is not None
                    session.delete(deleted)
                ready = threading.Event()
                release = threading.Event()
                done = threading.Event()
                writer_errors: list[str] = []

                def writer() -> None:
                    connection = None
                    try:
                        connection = sqlite3.connect(database)
                        connection.execute("BEGIN IMMEDIATE")
                        ready.set()
                        release.wait()
                        connection.rollback()
                    except BaseException as error:
                        writer_errors.append(type(error).__name__)
                    finally:
                        if connection is not None:
                            connection.close()
                        done.set()
                        ready.set()

                original_writer = threading.Thread(target=writer, name="witness-original-writer")
                original_writer.start()
                try:
                    if not ready.wait(timeout=10) or done.is_set():
                        raise RuntimeError("original writer did not acquire its transaction")
                    denied = {"invalid": service.authenticate("invalid-witness-token") is None}
                    denied.update(
                        {
                            name: service.authenticate(str(keys[name]["token"])) is None
                            for name in ("revoked", "expired", "deleted")
                        }
                    )
                    started = time.perf_counter()
                    try:
                        principal = service.authenticate(str(keys["active"]["token"]))
                        report["active_outcome"] = (
                            "authenticated" if principal is not None else "denied"
                        )
                    except OperationalError as error:
                        code = getattr(error.orig, "sqlite_errorcode", None)
                        report["active_outcome"] = (
                            "busy" if isinstance(code, int) and code & 255 == 5 else "other-error"
                        )
                        report["sqlite_errorcode"] = code
                    report["active_seconds"] = time.perf_counter() - started
                    report["denied_while_writer_held"] = denied
                    report["original_writer_alive_after_authentication"] = (
                        original_writer.is_alive() and not done.is_set() and not release.is_set()
                    )
                    probe = sqlite3.connect(database, timeout=0)
                    try:
                        try:
                            probe.execute("BEGIN IMMEDIATE")
                        except sqlite3.OperationalError as error:
                            report["writer_still_held_sqlite_errorcode"] = error.sqlite_errorcode
                        else:
                            report["writer_still_held_sqlite_errorcode"] = None
                            probe.rollback()
                    finally:
                        probe.close()
                    with factory() as session:
                        record = session.get(AppKeyRecord, str(keys["active"]["id"]))
                        assert record is not None
                        report["usage_omitted_while_held"] = record.last_used_at is None
                finally:
                    release.set()
                    original_writer.join()
                    report["original_writer_joined"] = not original_writer.is_alive()
                    report["writer_errors"] = writer_errors
                principal = service.authenticate(str(keys["active"]["token"]))
                report["authenticated_after_release"] = principal is not None
                with factory() as session:
                    record = session.get(AppKeyRecord, str(keys["active"]["id"]))
                    assert record is not None
                    report["usage_recorded_after_release"] = record.last_used_at is not None
                report["passed"] = (
                    report["active_outcome"] == args.expect
                    and all(denied.values())
                    and report["original_writer_alive_after_authentication"] is True
                    and report["writer_still_held_sqlite_errorcode"] == sqlite3.SQLITE_BUSY
                    and report["usage_omitted_while_held"] is True
                    and report["original_writer_joined"] is True
                    and not writer_errors
                    and report["authenticated_after_release"] is True
                    and report["usage_recorded_after_release"] is True
                )
            finally:
                dispose_session_factory(factory)
        report["workspace_removed"] = not directory.exists()
    finally:
        if previous_config is None:
            os.environ.pop("RIVERHOG_CONFIG", None)
        else:
            os.environ["RIVERHOG_CONFIG"] = previous_config
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    print(json.dumps(report, sort_keys=True))
    return 0 if report["passed"] and report["workspace_removed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
