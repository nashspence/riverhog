from __future__ import annotations

import json
import os
import re
import shutil
import weakref
from contextlib import contextmanager
from pathlib import Path

import pytest
from a_riverhog_cli import directory_upload, main
from riverhog_client import PrimaryStatePolicy
from typer.testing import CliRunner

from tests.support.cli_upload import read_upload_sources


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
    assert (
        json.loads(exact)["sources"][0]["artifact_id"]
        == read_upload_sources(original)[0].artifact_id
    )


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

    @contextmanager
    def observed(*args, **kwargs):
        with verified(*args, **kwargs) as (selected, rows):
            retained.append(weakref.ref(rows))
            yield selected, rows

    monkeypatch.setattr(directory_upload, "_verified_upload_sources", observed)
    with directory_upload.prepare_primary_upload(
        root, "retained", policy, initialize=initialize
    ) as (upload, store):
        assert len(retained) == 1 and retained[0]() is None
        assert not store.publication_started
        assert (
            read_upload_sources(upload)[0].producer_file().source.read_bytes() == b"exact original"
        )
        path = directory_upload._state_path(root, "retained", create_parent=False)
        original = path.read_bytes()
        assert (
            json.loads(original)["sources"][0]["artifact_id"]
            == read_upload_sources(upload)[0].artifact_id
        )
    with directory_upload.prepare_primary_upload(root, "retained", policy, initialize=False) as (
        replayed,
        store,
    ):
        assert replayed == upload and not store.publication_started
        assert retained[-1]() is None
    assert path.read_bytes() == original


@pytest.mark.skipif(os.name != "posix", reason="actual disposable POSIX metadata ordering")
@pytest.mark.parametrize("members", [1, 3, 128])
def test_posix_metadata_spools_before_ids_and_closes_before_publication(
    source, tmp_path, monkeypatch, members
):
    from types import SimpleNamespace

    root, state, policy = source
    (root / "member.bin").unlink()
    (root / "nested").mkdir()
    for number in reversed(range(members)):
        (root / "nested" / f"雪-{number:04}.bin").write_bytes(b"" if number % 2 else b"equal")
    policy = policy.model_copy(update={"inventory_order_bytes": 64 * 1024})
    expected = sorted(root.rglob("*.bin"), key=lambda path: path.relative_to(root).as_posix())
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    opened = []
    temporary = directory_upload.tempfile.TemporaryDirectory

    def tracked(*args, **kwargs):
        result = temporary(*args, dir=scratch, **kwargs)
        opened.append(Path(result.name))
        return result

    actual = directory_upload._source_fixity
    hashed = []
    token = directory_upload.secrets.token_hex
    view = directory_upload.uuid.uuid4

    def fixity(path):
        assert not state.exists()
        hashed.append(path)
        return actual(path)

    def allocate(size):
        assert len(hashed) == members
        return token(size)

    def allocate_view():
        assert len(hashed) == members
        return view()

    def no_inventory(*args, **kwargs):
        pytest.fail("publication preparation materialized the complete source inventory")

    monkeypatch.setattr(directory_upload.tempfile, "TemporaryDirectory", tracked)
    monkeypatch.setattr(directory_upload, "_source_paths", no_inventory)
    monkeypatch.setattr(directory_upload, "preview_upload", no_inventory)
    monkeypatch.setattr(directory_upload, "_source_fixity", fixity)
    monkeypatch.setattr(directory_upload, "secrets", SimpleNamespace(token_hex=allocate))
    monkeypatch.setattr(directory_upload, "uuid", SimpleNamespace(uuid4=allocate_view))
    with directory_upload.prepare_primary_upload(root, "retained", policy, initialize=True) as (
        original,
        store,
    ):
        assert original.source_count == members and len(hashed) == members
        assert opened and all(not path.exists() for path in opened)
        assert not store.publication_started
        values = read_upload_sources(original)
        assert [value.path for value in values] == expected
        ids = tuple(value.artifact_id for value in values)
        assert len(set(ids)) == members
    identity = directory_upload._state_path(root, "retained", create_parent=False)
    exact = identity.read_bytes()
    hashed.clear()

    def cold_fixity(path):
        hashed.append(path)
        return actual(path)

    monkeypatch.setattr(directory_upload, "_source_fixity", cold_fixity)
    monkeypatch.setattr(directory_upload, "secrets", SimpleNamespace(token_hex=no_inventory))
    monkeypatch.setattr(directory_upload, "uuid", SimpleNamespace(uuid4=no_inventory))
    with directory_upload.prepare_primary_upload(root, "retained", policy, initialize=False) as (
        replayed,
        store,
    ):
        assert replayed == original and len(hashed) == members
        assert all(not path.exists() for path in opened)
        assert tuple(value.artifact_id for value in read_upload_sources(replayed)) == ids
        assert not store.publication_started
    assert identity.read_bytes() == exact and not list(scratch.iterdir())


@pytest.mark.skipif(os.name != "posix", reason="actual disposable POSIX metadata ordering")
@pytest.mark.parametrize("failure", ["empty", "symlink", "hash", "capacity", "identity"])
def test_posix_metadata_failure_closes_scratch_and_preserves_allocation_boundary(
    source, tmp_path, monkeypatch, failure
):
    root, state, policy = source
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    temporary = directory_upload.tempfile.TemporaryDirectory
    opened = []

    def tracked(*args, **kwargs):
        result = temporary(*args, dir=scratch, **kwargs)
        opened.append(Path(result.name))
        return result

    monkeypatch.setattr(directory_upload.tempfile, "TemporaryDirectory", tracked)
    if failure == "empty":
        (root / "member.bin").unlink()
    elif failure == "symlink":
        (root / "link").symlink_to(root / "member.bin")
    elif failure == "hash":

        def failed_hash(path):
            raise OSError("source read interrupted")

        monkeypatch.setattr(directory_upload, "_source_fixity", failed_hash)
    elif failure == "capacity":
        policy = policy.model_copy(update={"inventory_order_bytes": 8192})
    else:

        def failed_identity(*args, **kwargs):
            raise OSError("identity publication interrupted")

        monkeypatch.setattr(directory_upload, "_prepare_verified_upload", failed_identity)
    if failure != "identity":
        monkeypatch.setattr(directory_upload, "_new_upload_identity", forbidden)
        monkeypatch.setattr(directory_upload, "PrimaryStore", forbidden)
    from riverhog_client import PrimaryStateCapacity

    error = (
        ValueError
        if failure in {"empty", "symlink"}
        else PrimaryStateCapacity
        if failure == "capacity"
        else OSError
    )
    with pytest.raises(error):
        with directory_upload.prepare_primary_upload(root, "retained", policy, initialize=True):
            pytest.fail("failed metadata preparation reached publication")
    assert opened and all(not path.exists() for path in opened)
    assert not list(scratch.iterdir())
    if failure != "identity":
        assert not state.exists()


@pytest.mark.skipif(os.name != "posix", reason="actual disposable POSIX metadata ordering")
def test_posix_metadata_scope_admits_physical_scratch_and_closes_on_interruption(
    source, tmp_path, monkeypatch
):
    root, state, _policy = source
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    temporary = directory_upload.tempfile.TemporaryDirectory
    opened = []

    def tracked(*args, **kwargs):
        result = temporary(*args, dir=scratch, **kwargs)
        opened.append(Path(result.name))
        return result

    monkeypatch.setattr(directory_upload.tempfile, "TemporaryDirectory", tracked)
    with pytest.raises(RuntimeError, match="consumer interrupted"):
        with directory_upload._verified_upload_sources(root, "retained", byte_limit=64 * 1024) as (
            _root,
            current,
        ):
            assert len(current) == 1
            assert (
                sum(path.stat().st_size for path in opened[0].rglob("*") if path.is_file())
                <= 64 * 1024
            )
            escaped = iter(current)
            assert next(escaped)["bytes"] == len(b"exact original")
            raise RuntimeError("consumer interrupted")
    assert all(not path.exists() for path in opened)
    from riverhog_client import PrimaryStateError

    with pytest.raises(PrimaryStateError, match="closed"):
        next(escaped)
    assert not state.exists()


@pytest.mark.skipif(os.name != "posix", reason="actual disposable POSIX metadata ordering")
def test_posix_metadata_resolves_process_temporary_placement_before_storage_selection(
    source, tmp_path, monkeypatch
):
    root, state, _policy = source
    parent = tmp_path / "actual-temporary"
    parent.mkdir()
    alias = tmp_path / "temporary-alias"
    alias.symlink_to(parent, target_is_directory=True)
    temporary = directory_upload.tempfile.TemporaryDirectory
    physical = directory_upload.posix_primary_files
    selected = []

    def placed(*args, **kwargs):
        return temporary(*args, dir=alias, **kwargs)

    def opened(location, **kwargs):
        assert location == location.resolve()
        assert not any(path.is_symlink() for path in (location, *location.parents))
        selected.append(location)
        return physical(location, **kwargs)

    monkeypatch.setattr(directory_upload.tempfile, "TemporaryDirectory", placed)
    monkeypatch.setattr(directory_upload, "posix_primary_files", opened)
    with directory_upload._verified_upload_sources(root, "retained", byte_limit=64 * 1024) as (
        _root,
        current,
    ):
        assert len(current) == 1
        assert list(current)[0]["bytes"] == len(b"exact original")
    assert selected and not selected[0].parent.exists()
    assert alias.is_symlink() and not list(parent.iterdir()) and not state.exists()
