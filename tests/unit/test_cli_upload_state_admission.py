from __future__ import annotations

import json
import os
import re
import shutil
import weakref
from contextlib import contextmanager

import pytest
from a_riverhog_cli import directory_upload, main
from riverhog_client import PrimaryStatePolicy
from typer.testing import CliRunner


@pytest.fixture
def source(tmp_path, monkeypatch):
    root = tmp_path / "source"
    root.mkdir()
    (root / "member.bin").write_bytes(b"exact original")
    state = tmp_path / "state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    policy = PrimaryStatePolicy(scope={"service": "fixture-service", "account": "fixture-account"})
    monkeypatch.setenv("A_RIVERHOG_CLI_PRIMARY_STATE", policy.model_dump_json())
    return root, state, policy


def forbidden(*args, **kwargs):
    pytest.fail("unadmitted resume reached allocation or remote work")


def test_resume_missing_identity_does_not_create_directories_ids_primary_or_api(
    source, monkeypatch
):
    root, state, _policy = source
    monkeypatch.setattr(directory_upload, "_verified_upload_sources", forbidden)
    monkeypatch.setattr(directory_upload.secrets, "token_hex", forbidden)
    monkeypatch.setattr(directory_upload, "PrimaryStore", forbidden)
    monkeypatch.setattr(main, "client", forbidden)
    result = CliRunner().invoke(
        main.app, ["collection", "upload", "start", str(root), "--idempotency-key", "retained"]
    )
    assert isinstance(result.exception, FileNotFoundError)
    assert "resume cannot allocate replacements" in str(result.exception)
    assert not state.exists()


@pytest.mark.parametrize("initialize", [None, 0, 1, "true"])
def test_primary_upload_requires_boolean_initialization_before_touching_state(
    source, monkeypatch, initialize
):
    root, state, policy = source
    monkeypatch.setattr(directory_upload, "_verified_upload_sources", forbidden)
    monkeypatch.setattr(directory_upload, "PrimaryStore", forbidden)
    with pytest.raises(ValueError, match="explicit boolean authority"):
        with directory_upload.prepare_primary_upload(
            root, "retained", policy, initialize=initialize
        ):
            pytest.fail("invalid initialization entered publication")
    assert not state.exists()


@pytest.mark.parametrize(
    "key,initialize,expected",
    [(None, False, True), (None, True, True), ("retained", False, False), ("fresh", True, True)],
)
def test_cli_generated_and_supplied_keys_select_explicit_state_authority(
    source, monkeypatch, key, initialize, expected
):
    root, state, _policy = source
    calls = []

    class Boundary(RuntimeError):
        pass

    @contextmanager
    def admitted(path, selected_key, policy, *, initialize, windows=None):
        assert windows is None
        calls.append((path, selected_key, initialize))
        raise Boundary("state authority observed")
        yield  # pragma: no cover

    monkeypatch.setattr(main, "prepare_primary_upload", admitted)
    monkeypatch.setattr(main, "client", forbidden)
    arguments = ["collection", "upload", "start", str(root)]
    if key is not None:
        arguments += ["--idempotency-key", key]
    if initialize:
        arguments += ["--initialize-state"]
    result = CliRunner().invoke(main.app, arguments)
    assert isinstance(result.exception, Boundary)
    assert len(calls) == 1 and calls[0][0] == root and calls[0][2] is expected
    if key is None:
        assert re.fullmatch("[0-9a-f]{32}", calls[0][1])
    else:
        assert calls[0][1] == key
    assert not state.exists()


@pytest.mark.skipif(
    os.name != "posix", reason="actual retained POSIX CLI identity and primary store"
)
def test_fresh_state_cannot_replace_retained_identity_and_resume_keeps_originals(source):
    root, _state, policy = source
    with directory_upload.prepare_primary_upload(root, "retained", policy, initialize=True) as (
        original,
        _store,
    ):
        pass
    path = directory_upload._state_path(root, "retained", create_parent=False)
    exact = path.read_bytes()
    with pytest.raises(FileExistsError, match="conflicts with retained identity"):
        with directory_upload.prepare_primary_upload(root, "retained", policy, initialize=True):
            pytest.fail("fresh initialization entered retained state")
    with directory_upload.prepare_primary_upload(root, "retained", policy, initialize=False) as (
        resumed,
        _store,
    ):
        assert resumed == original
    assert path.read_bytes() == exact
    assert json.loads(exact)["sources"][0]["artifact_id"] == original.sources[0].artifact_id


@pytest.mark.skipif(
    os.name != "posix", reason="actual retained POSIX CLI identity and primary store"
)
@pytest.mark.parametrize("missing", ["identity", "primary"])
def test_missing_retained_half_never_allocates_replacement_ids_or_primary(
    source, monkeypatch, missing
):
    root, _state, policy = source
    with directory_upload.prepare_primary_upload(root, "retained", policy, initialize=True):
        pass
    path = directory_upload._state_path(root, "retained", create_parent=False)
    original = path.read_bytes()
    primary = path.with_suffix(".primary")
    if missing == "identity":
        path.unlink()
        retained = {
            p.relative_to(primary): p.read_bytes() for p in primary.rglob("*") if p.is_file()
        }
    else:
        shutil.rmtree(primary)
    monkeypatch.setattr(directory_upload.secrets, "token_hex", forbidden)
    with pytest.raises(FileNotFoundError):
        with directory_upload.prepare_primary_upload(root, "retained", policy, initialize=False):
            pytest.fail("missing retained state entered publication")
    if missing == "identity":
        assert not path.exists()
        assert retained == {
            p.relative_to(primary): p.read_bytes() for p in primary.rglob("*") if p.is_file()
        }
    else:
        assert path.read_bytes() == original and not primary.exists()


@pytest.mark.skipif(
    os.name != "posix", reason="actual retained POSIX CLI identity and primary store"
)
def test_identity_loss_after_primary_open_still_cannot_allocate_replacements(source, monkeypatch):
    root, _state, policy = source
    with directory_upload.prepare_primary_upload(root, "retained", policy, initialize=True):
        pass
    path = directory_upload._state_path(root, "retained", create_parent=False)
    actual = directory_upload.PrimaryStore
    opened = []

    @contextmanager
    def lost(*args, **options):
        with actual(*args, **options) as store:
            opened.append(True)
            path.unlink()
            yield store

    monkeypatch.setattr(directory_upload, "PrimaryStore", lost)
    monkeypatch.setattr(directory_upload.secrets, "token_hex", forbidden)
    with pytest.raises(FileNotFoundError, match="resume cannot allocate replacements"):
        with directory_upload.prepare_primary_upload(root, "retained", policy, initialize=False):
            pytest.fail("lost identity entered publication")
    assert opened == [True] and not path.exists()


@pytest.mark.parametrize("key", ["", " leading-space"])
def test_invalid_supplied_key_is_never_replaced_by_a_generated_request(source, monkeypatch, key):
    root, state, _policy = source
    monkeypatch.setattr(main.uuid, "uuid4", forbidden)
    monkeypatch.setattr(main, "client", forbidden)
    monkeypatch.setattr(directory_upload, "PrimaryStore", forbidden)
    result = CliRunner().invoke(
        main.app,
        [
            "collection",
            "upload",
            "start",
            str(root),
            "--idempotency-key",
            key,
            "--initialize-state",
        ],
    )
    assert isinstance(result.exception, ValueError)
    assert "idempotency key must be nonempty and canonical" in str(result.exception)
    assert not state.exists()


@pytest.mark.skipif(
    os.name != "posix", reason="actual retained POSIX CLI identity and primary store"
)
@pytest.mark.parametrize("initialize", [True, False])
def test_source_verification_inventory_is_released_before_publication(
    source, monkeypatch, initialize
):
    root, _state, policy = source
    if not initialize:
        with directory_upload.prepare_primary_upload(root, "retained", policy, initialize=True):
            pass
    verified = directory_upload._verified_upload_sources
    retained = []

    class Inventory(list):
        pass

    def observed(*args, **kwargs):
        selected, rows = verified(*args, **kwargs)
        inventory = Inventory(rows)
        retained.append(weakref.ref(inventory))
        return selected, inventory

    monkeypatch.setattr(directory_upload, "_verified_upload_sources", observed)
    with directory_upload.prepare_primary_upload(
        root, "retained", policy, initialize=initialize
    ) as (upload, store):
        assert len(retained) == 1 and retained[0]() is None
        assert not store.publication_started
        assert upload.sources[0].producer_file().source.read_bytes() == b"exact original"
        path = directory_upload._state_path(root, "retained", create_parent=False)
        original = path.read_bytes()
        assert json.loads(original)["sources"][0]["artifact_id"] == upload.sources[0].artifact_id
    with directory_upload.prepare_primary_upload(root, "retained", policy, initialize=False) as (
        replayed,
        store,
    ):
        assert replayed == upload and not store.publication_started
        assert retained[-1]() is None
    assert path.read_bytes() == original
