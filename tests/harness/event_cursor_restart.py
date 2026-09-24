"""Read persistent event fixtures through real API routes in separate processes.

Owner services seed events; this is SQLite/ASGI fixture evidence, not a deployed
server, production database, mutation-lifecycle, or consumer-checkpoint proof.
"""

from __future__ import annotations

import hashlib
import json
import runpy
import sys
from contextlib import ExitStack
from pathlib import Path

from fastapi.testclient import TestClient

from tests.operation_observer import TimeoutNeutralTestClient


def exercise(application: str, phase: str, root: Path) -> dict[str, object]:
    with ExitStack() as cleanup:
        if application == "riverhog":
            from riverhog_api.app import create_app
            from riverhog_core.app_permissions import (
                EVENTS_READ_ALL,
                ApplicationAccess,
                Principal,
            )
            from riverhog_protocol.lifecycle_events import COLLECTION_FINALIZED

            from tests.unit.test_lifecycle_event_api import _finalized_event_data
            from tests.unit.test_operation_lifecycle_api import _api, _container

            container = _container(root)
            cleanup.callback(container.close)
            token_path = root / "token"
            if phase == "prepare":
                key = container.app_keys.create(
                    app="cursor-fixture",
                    access=[ApplicationAccess(EVENTS_READ_ALL)],
                    grantor=Principal(
                        id="bootstrap",
                        key_id=None,
                        access=frozenset(),
                        unrestricted_delegation=True,
                    ),
                )
                token_path.write_text(str(key["token"]))
            app = create_app(container=container)
            transport = cleanup.enter_context(TestClient(app))
            api = _api(transport, token_path.read_text())
            cleanup.callback(api.close)
            page = api.list_lifecycle_events

            def emit(number: int) -> str:
                container.lifecycle_events.emit(
                    owner_principal_id="cursor-fixture",
                    type=COLLECTION_FINALIZED,
                    subject=str(number),
                    data=_finalized_event_data(number, "cursor-fixture"),
                )
                return str(number)

        elif application == "stove0":
            from stove0_api.app import create_app
            from stove0_api_client import Stove0ApiClient

            from tests.unit.db_helpers import sqlite_url

            parity = runpy.run_path(
                str(
                    Path(__file__).resolve().parents[2]
                    / "some-implementations/stove0/application/tests/test_stove0_api_parity.py"
                )
            )
            _composition = parity["_composition"]
            _fixture_work = parity["_fixture_work"]

            composition = _composition(sqlite_url(root / "stove0.sqlite3"))
            app = create_app(composition)
            transport = cleanup.enter_context(
                TestClient(
                    app,
                    headers={"Authorization": "Bearer stove0-test-token"},
                )
            )
            api = Stove0ApiClient(
                "http://testserver", "stove0-test-token", allow_insecure_http=True
            )
            api._client = TimeoutNeutralTestClient(transport)
            page = api.list_events

            def emit(number: int) -> str:
                work = _fixture_work(recipe_id=f"cursor-fixture-{number}/v1")
                composition.work.create_or_resume(work)
                return work.work_id

        elif application == "a-riverhog-ftp-spool":
            from a_riverhog_ftp_spool.app import FtpSpoolComposition, create_app
            from a_riverhog_ftp_spool.config import FtpSpoolConfig, SourceConfig
            from a_riverhog_ftp_spool.landing import FtpSpool
            from a_riverhog_ftp_spool_client import RiverhogFtpSpoolClient
            from a_riverhog_ftp_spool_client.events import CLAIM_REGISTERED

            class _RiverhogApi:
                def close(self) -> None:
                    pass

                def list_archive_stores(self, *, page_size: int) -> dict[str, object]:
                    del page_size
                    return {"items": []}

            source = SourceConfig(
                id="cursor-fixture",
                root=root / "ftp-landing",
                ingest_source="ftp:cursor-fixture",
                provenance="omit",
                provenance_omission_reason="Fixture has no host provenance.",
            )
            config = FtpSpoolConfig(
                host_id="cursor-fixture",
                riverhog_base_url="https://riverhog.invalid",
                riverhog_token="riverhog-token",
                api_token="ftp-test-token",
                sources=(source,),
            )
            owner = FtpSpool(_RiverhogApi(), config)  # type: ignore[arg-type]
            app = create_app(FtpSpoolComposition(config, owner.api, owner))
            transport = cleanup.enter_context(
                TestClient(app, headers={"Authorization": "Bearer ftp-test-token"})
            )
            api = RiverhogFtpSpoolClient(
                "http://testserver", "ftp-test-token", allow_insecure_http=True
            )
            api._http = TimeoutNeutralTestClient(transport)
            cleanup.callback(api.close)

            def page(*, after: str | None = None, limit: int = 100):
                return api.list_ftp_spool_events(source.id, after=after, limit=limit)

            def emit(number: int) -> str:
                claim_id = hashlib.sha256(f"cursor-fixture:{number}".encode()).hexdigest()
                owner._record_claim_event(
                    source,
                    {
                        "claim_id": claim_id,
                        "source_event_id": f"cursor-fixture:{number}",
                        "files": [{"bytes": number}],
                    },
                    event_type=CLAIM_REGISTERED,
                )
                return claim_id

        else:
            raise ValueError(f"unknown event fixture: {application}")

        path = (
            "/v1/sources/{source_id}/events"
            if application == "a-riverhog-ftp-spool"
            else "/v1/events"
        )
        operation_id = app.openapi()["paths"][path]["get"]["operationId"]
        if phase == "prepare":
            subjects = [emit(number) for number in range(1, 4)]
            return {
                "operation_id": operation_id,
                "subjects": subjects,
                "history": page(limit=100).model_dump(mode="json"),
                "first": page(limit=1).model_dump(mode="json"),
            }
        if phase != "resume":
            raise ValueError(f"unknown event fixture phase: {phase}")
        before = json.loads((root / "before.json").read_text())
        first = page(after=before["first"]["next_cursor"], limit=1)
        second = page(after=first.next_cursor, limit=1)
        empty = page(after=second.next_cursor, limit=1)
        subject = emit(4)
        appended = page(after=empty.next_cursor, limit=1)
        terminal = page(after=appended.next_cursor, limit=1)
        return {
            "operation_id": operation_id,
            "unread": [first.model_dump(mode="json"), second.model_dump(mode="json")],
            "empty": empty.model_dump(mode="json"),
            "appended_subject": subject,
            "appended": appended.model_dump(mode="json"),
            "terminal": terminal.model_dump(mode="json"),
        }


if __name__ == "__main__":
    print(json.dumps(exercise(sys.argv[1], sys.argv[2], Path(sys.argv[3]))))
