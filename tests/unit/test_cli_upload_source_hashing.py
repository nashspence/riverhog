from __future__ import annotations

import errno
import hashlib
import os
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from a_riverhog_cli import directory_upload


def _forbid_identity(monkeypatch, tmp_path):
    state = tmp_path / "uncreated-state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))

    def forbidden(*args, **kwargs):
        pytest.fail("unstable source reached fresh identity allocation")

    monkeypatch.setattr(directory_upload, "_new_upload_identity", forbidden)
    if os.name != "posix":
        # Exercise common source rejection before allocation without selecting
        # a POSIX filesystem owner on this platform. Accepted NTFS workflows
        # use the separate actual native application binding.
        @contextmanager
        def ordered(rows, *, byte_limit):
            for _ in rows:
                pass
            pytest.fail("unstable source completed metadata verification")
            yield  # pragma: no cover

        @contextmanager
        def physical(*args, **kwargs):
            yield SimpleNamespace(ordered_rows=ordered)

        monkeypatch.setattr(directory_upload, "posix_primary_files", physical)
    return state


def _mutating_digest(monkeypatch, change):
    actual = hashlib.sha256
    observations = []

    class Digest:
        def __init__(self, *args, **kwargs):
            self.inner = actual(*args, **kwargs)

        def update(self, content):
            self.inner.update(content)
            observations.append(len(content))
            if len(observations) > 6:
                pytest.fail("source hash kept chasing a growing EOF")
            change(len(observations))

        def hexdigest(self):
            return self.inner.hexdigest()

    monkeypatch.setattr(directory_upload, "hashlib", SimpleNamespace(sha256=Digest))
    return observations


@pytest.mark.parametrize("size", [0, 4, 8 * 1024 * 1024 + 33])
def test_source_fixity_hashes_exact_bytes_with_one_bounded_read_window(tmp_path, monkeypatch, size):
    root = tmp_path / "source"
    root.mkdir()
    source = root / "member"
    payload = b"x" * size
    source.write_bytes(payload)
    expected = hashlib.sha256(payload).hexdigest()
    actual = os.read
    reads = []

    def measured(descriptor, count):
        content = actual(descriptor, count)
        reads.append((count, len(content)))
        return content

    monkeypatch.setattr(directory_upload.os, "read", measured)
    assert directory_upload.preview_upload(root) == (
        {"relative_components": ["member"], "bytes": size, "sha256": expected},
    )
    assert reads and all(0 < count <= 8 * 1024 * 1024 for count, _ in reads)
    assert sum(observed for _, observed in reads) == size
    assert reads[-1][1] == 0


@pytest.mark.parametrize("mutation", ["grow", "truncate", "same-length"])
def test_source_mutation_during_hash_refuses_identity_and_closes_the_actual_descriptor(
    tmp_path, monkeypatch, mutation
):
    root = tmp_path / "source"
    root.mkdir()
    source = root / "member"
    source.write_bytes(b"original")
    stamp = source.stat()
    state = _forbid_identity(monkeypatch, tmp_path)
    actual_open = os.open
    descriptors = []

    def opened(path, flags, *args, **kwargs):
        descriptor = actual_open(path, flags, *args, **kwargs)
        if Path(path) == source:
            descriptors.append(descriptor)
        return descriptor

    monkeypatch.setattr(directory_upload.os, "open", opened)

    def change(first):
        if first != 1:
            return
        source.write_bytes(
            {"grow": b"original plus growth", "truncate": b"", "same-length": b"replaced"}[mutation]
        )
        os.utime(source, ns=(stamp.st_atime_ns, stamp.st_mtime_ns + 2_000_000_000))

    _mutating_digest(monkeypatch, change)
    with pytest.raises(ValueError, match="source changed while hashing"):
        directory_upload.prepare_upload(root, "original-request", ordering_bytes=64 * 1024)
    assert not state.exists()
    assert len(descriptors) == 1
    with pytest.raises(OSError) as error:
        os.fstat(descriptors[0])
    assert error.value.errno == errno.EBADF


def test_continuously_growing_source_has_a_bounded_failed_read_prefix(tmp_path, monkeypatch):
    root = tmp_path / "source"
    root.mkdir()
    source = root / "member"
    source.write_bytes(b"original")
    admitted = source.stat().st_size
    state = _forbid_identity(monkeypatch, tmp_path)
    actual_read = os.read
    observed = []

    def measured(descriptor, count):
        content = actual_read(descriptor, count)
        observed.append(len(content))
        return content

    monkeypatch.setattr(directory_upload.os, "read", measured)

    def append(_call):
        with source.open("ab") as handle:
            handle.write(b"x")

    hashed = _mutating_digest(monkeypatch, append)
    with pytest.raises(ValueError, match="source changed while hashing"):
        directory_upload.prepare_upload(root, "original-request", ordering_bytes=64 * 1024)
    assert sum(observed) <= admitted + 1 and sum(hashed) <= admitted
    assert len(observed) <= 2
    assert not state.exists()


def test_source_replacement_between_observation_and_open_refuses_identity(tmp_path, monkeypatch):
    root = tmp_path / "source"
    root.mkdir()
    source = root / "member"
    source.write_bytes(b"original")
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"replaced")
    stamp = source.stat()
    os.utime(replacement, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
    state = _forbid_identity(monkeypatch, tmp_path)
    actual_os_open, actual_path_open = os.open, Path.open
    replaced = False

    def replace():
        nonlocal replaced
        if not replaced:
            replacement.replace(source)
            replaced = True

    def opened(path, flags, *args, **kwargs):
        if Path(path) == source:
            replace()
        return actual_os_open(path, flags, *args, **kwargs)

    def path_open(path, *args, **kwargs):
        if path == source:
            replace()
        return actual_path_open(path, *args, **kwargs)

    monkeypatch.setattr(directory_upload.os, "open", opened)
    monkeypatch.setattr(Path, "open", path_open)
    with pytest.raises(ValueError, match="source changed while hashing"):
        directory_upload.prepare_upload(root, "original-request", ordering_bytes=64 * 1024)
    assert replaced and not state.exists()


def test_short_source_read_cannot_publish_an_inconsistent_byte_count(tmp_path, monkeypatch):
    root = tmp_path / "source"
    root.mkdir()
    source = root / "member"
    source.write_bytes(b"original")
    state = _forbid_identity(monkeypatch, tmp_path)
    monkeypatch.setattr(directory_upload.os, "read", lambda _descriptor, _count: b"")
    with pytest.raises(ValueError, match="source changed while hashing"):
        directory_upload.prepare_upload(root, "original-request", ordering_bytes=64 * 1024)
    assert not state.exists()


def _stat_with_ctime(value, changed):
    return SimpleNamespace(
        st_mode=value.st_mode,
        st_dev=value.st_dev,
        st_ino=value.st_ino,
        st_size=value.st_size,
        st_mtime_ns=value.st_mtime_ns,
        st_ctime_ns=changed,
    )


def test_source_fixity_compares_change_time_within_each_stat_api(tmp_path, monkeypatch):
    root = tmp_path / "source"
    root.mkdir()
    source = root / "member"
    source.write_bytes(b"original")
    actual_fstat = os.fstat

    def descriptor_stat(descriptor):
        value = actual_fstat(descriptor)
        return _stat_with_ctime(value, value.st_ctime_ns + 1_000_000_000)

    monkeypatch.setattr(directory_upload.os, "fstat", descriptor_stat)
    assert directory_upload.preview_upload(root) == (
        {
            "relative_components": ["member"],
            "bytes": 8,
            "sha256": hashlib.sha256(b"original").hexdigest(),
        },
    )


@pytest.mark.parametrize("domain", ["path", "descriptor"])
def test_source_fixity_keeps_each_change_time_check_when_apis_differ(tmp_path, monkeypatch, domain):
    root = tmp_path / "source"
    root.mkdir()
    source = root / "member"
    source.write_bytes(b"original")
    state = _forbid_identity(monkeypatch, tmp_path)
    changed = [False]
    actual_fstat, actual_lstat = os.fstat, Path.lstat
    original_source = source.stat()

    def descriptor_stat(descriptor):
        value = actual_fstat(descriptor)
        if (value.st_dev, value.st_ino) != (original_source.st_dev, original_source.st_ino):
            return value
        delta = 1_000_000_000 + (3_000_000_000 if changed[0] and domain == "descriptor" else 0)
        return _stat_with_ctime(value, value.st_ctime_ns + delta)

    def path_stat(path, *args, **kwargs):
        value = actual_lstat(path, *args, **kwargs)
        if path == source:
            delta = 3_000_000_000 if changed[0] and domain == "path" else 0
            return _stat_with_ctime(value, value.st_ctime_ns + delta)
        return value

    monkeypatch.setattr(directory_upload.os, "fstat", descriptor_stat)
    monkeypatch.setattr(Path, "lstat", path_stat)
    hashed = _mutating_digest(monkeypatch, lambda _: changed.__setitem__(0, True))
    with pytest.raises(ValueError, match="source changed while hashing") as error:
        directory_upload.prepare_upload(root, "original-request", ordering_bytes=64 * 1024)
    assert str(error.value) == f"upload source changed while hashing ({domain}: ctime)"
    assert hashed == [8] and not state.exists()
