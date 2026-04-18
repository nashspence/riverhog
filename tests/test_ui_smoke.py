from __future__ import annotations

from fastapi.testclient import TestClient

from ui.app import main as ui_main


def test_dashboard_and_detail_pages_render_with_api_data(monkeypatch):
    collection = {
        "collection_id": "demo-collection",
        "status": "open",
        "description": "demo collection",
        "keep_buffer_after_archive": False,
        "file_count": 2,
        "directory_count": 1,
        "created_at": "2026-04-18T00:00:00Z",
        "sealed_at": None,
    }
    container = {
        "container_id": "DEMO-001",
        "status": "inactive",
        "description": None,
        "total_root_bytes": 1024,
        "contents_hash": "abc123",
        "entry_count": 3,
        "active_root_present": False,
        "iso_present": True,
        "iso_size_bytes": 4096,
        "burn_confirmed_at": None,
        "created_at": "2026-04-18T00:00:00Z",
    }

    def fake_load_json(path: str):
        if path == "/v1/collections":
            return {"collections": [collection]}, None
        if path == "/v1/containers":
            return {"containers": [container]}, None
        if path == "/v1/collections/demo-collection/tree":
            return {
                "nodes": [
                    {
                        "path": "docs/file.txt",
                        "kind": "file",
                        "size_bytes": 10,
                        "active": True,
                        "source": "buffer",
                        "container_ids": [],
                        "status": "active",
                    }
                ]
            }, None
        if path == "/v1/containers/DEMO-001/tree":
            return {
                "nodes": [
                    {
                        "path": "README.txt",
                        "kind": "file",
                        "size_bytes": 10,
                        "active": False,
                        "source": "container",
                        "container_ids": ["DEMO-001"],
                        "status": "inactive",
                    }
                ]
            }, None
        return None, "missing"

    monkeypatch.setattr(ui_main, "_load_json", fake_load_json)
    monkeypatch.setattr(ui_main, "_collection_summary", lambda collection_id: (collection, None))
    monkeypatch.setattr(ui_main, "_container_summary", lambda container_id: (container, None))

    with TestClient(ui_main.app) as client:
        dashboard = client.get("/")
        assert dashboard.status_code == 200
        assert "demo-collection" in dashboard.text
        assert "DEMO-001" in dashboard.text

        collection_page = client.get("/collections/demo-collection")
        assert collection_page.status_code == 200
        assert "docs/file.txt" in collection_page.text
        assert "Upload Files" in collection_page.text

        container_page = client.get("/containers/DEMO-001")
        assert container_page.status_code == 200
        assert "README.txt" in container_page.text
        assert "Create activation session" in container_page.text
