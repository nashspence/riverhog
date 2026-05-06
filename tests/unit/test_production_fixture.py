from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from pytest import MonkeyPatch

from tests.fixtures.data import IMAGE_FIXTURES
from tests.fixtures.production import ProductionSystem


@dataclass(frozen=True, slots=True)
class _Response:
    status_code: int
    payload: dict[str, object]
    text: str = ""

    def json(self) -> dict[str, object]:
        return self.payload


def test_operator_disc_label_check_reads_finalized_image_copies(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    requested_paths: list[tuple[str, str]] = []

    def fake_request(
        self: ProductionSystem,
        method: str,
        path: str,
        **_kwargs: object,
    ) -> _Response:
        requested_paths.append((method, path))
        return _Response(status_code=200, payload={"copies": []})

    monkeypatch.setattr(ProductionSystem, "request", fake_request)
    system = ProductionSystem(
        workspace=tmp_path,
        webdav_url="",
        server=cast(Any, None),
        base_url="",
        db_path=tmp_path / "arc.db",
        fixture_path=tmp_path / "arc_disc_fixture.json",
        collections=cast(Any, None),
        fetches=cast(Any, None),
        state=cast(Any, None),
        planning=cast(Any, None),
        copies=cast(Any, None),
    )

    assert not system.operator_disc_label_is_recorded()
    assert requested_paths == [
        ("GET", f"/v1/images/{IMAGE_FIXTURES[0].volume_id}/copies")
    ]
