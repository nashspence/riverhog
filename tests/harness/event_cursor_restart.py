"""Read persistent event fixtures through real API routes in separate processes.

Owner services seed events; this is SQLite/ASGI fixture evidence, not a deployed
server, production database, mutation-lifecycle, or consumer-checkpoint proof.
"""

from __future__ import annotations

import json
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
                ApplicationPrincipal,
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
                    grantor=ApplicationPrincipal(
                        app="bootstrap",
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
                    owner_app="cursor-fixture",
                    type=COLLECTION_FINALIZED,
                    subject=str(number),
                    data=_finalized_event_data(number, "cursor-fixture"),
                )
                return str(number)

        elif application == "stove0":
            from stove0_api.app import create_app
            from stove0_api_client import Stove0ApiClient

            from reference.stove0.application.tests.test_stove0_api_parity import (
                _composition,
                _fixture_work,
            )
            from tests.unit.db_helpers import sqlite_url

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

        else:
            raise ValueError(f"unknown event fixture: {application}")

        operation_id = app.openapi()["paths"]["/v1/events"]["get"]["operationId"]
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
