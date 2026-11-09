from __future__ import annotations

import os
import subprocess
import sys
import threading

import pytest
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX root-inode creation custody")


def lock_root(root):
    import fcntl

    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def test_busy_root_inode_does_not_create_a_marker_or_selected_directory(tmp_path):
    root = tmp_path / "owned"
    root.mkdir(mode=0o700)
    descriptor = lock_root(root)
    try:
        with lease_journal_validation_workspace(
            root, "a" * 32, create=True, creation_current=lambda: True
        ) as selected:
            assert selected is None
            assert not (root / ".coordinator").exists()
            assert not (root / ("a" * 32)).exists()
    finally:
        os.close(descriptor)
    with lease_journal_validation_workspace(
        root, "a" * 32, create=True, creation_current=lambda: True
    ) as selected:
        assert selected == root / ("a" * 32)


def test_successful_body_releases_root_inode_and_allows_an_independent_sibling(tmp_path):
    root = tmp_path / "owned"
    with lease_journal_validation_workspace(
        root, "a" * 32, create=True, creation_current=lambda: True
    ) as first:
        assert first is not None
        descriptor = lock_root(root)
        os.close(descriptor)
        with lease_journal_validation_workspace(
            root, "b" * 32, create=True, creation_current=lambda: True
        ) as sibling:
            assert sibling == root / ("b" * 32)


def test_busy_marker_body_releases_root_inode_before_returning_to_caller(tmp_path):
    import fcntl

    root = tmp_path / "owned"
    root.mkdir(mode=0o700)
    marker = root / ".coordinator"
    marker.touch(mode=0o600)
    descriptor = os.open(marker, os.O_RDWR | os.O_NOFOLLOW)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with lease_journal_validation_workspace(
            root, "a" * 32, create=True, creation_current=lambda: True
        ) as selected:
            assert selected is None
            independent = lock_root(root)
            os.close(independent)
    finally:
        os.close(descriptor)


def test_stale_creation_releases_root_inode_and_never_creates_selected_namespace(tmp_path):
    root = tmp_path / "owned"
    with lease_journal_validation_workspace(
        root, "a" * 32, create=True, creation_current=lambda: False
    ) as selected:
        assert selected is None
        independent = lock_root(root)
        os.close(independent)
        assert not (root / ("a" * 32)).exists()


def test_actual_creator_before_marker_excludes_absence_receiver_until_stopped(
    tmp_path, monkeypatch
):
    root = tmp_path / "owned"
    arrived, release, stopped = threading.Event(), threading.Event(), threading.Event()
    current = [True]
    outcomes, errors = [], []
    actual = os.open

    def paused(name, *args, **kwargs):
        if name == ".coordinator":
            arrived.set()
            if not release.wait(10):
                raise TimeoutError("pre-coordinator creator was not released")
        return actual(name, *args, **kwargs)

    def writer():
        try:
            with lease_journal_validation_workspace(
                root, "a" * 32, create=True, creation_current=lambda: current[0]
            ) as selected:
                outcomes.append(selected)
        except BaseException as error:
            errors.append(type(error).__name__)
        finally:
            stopped.set()

    with monkeypatch.context() as patch:
        patch.setattr(os, "open", paused)
        thread = threading.Thread(target=writer)
        thread.start()
        try:
            assert arrived.wait(10)
            assert root.is_dir() and not (root / ".coordinator").exists()
            assert not (root / ("a" * 32)).exists()
            with pytest.raises(BlockingIOError):
                lock_root(root)
            current[0] = False
        finally:
            release.set()
            thread.join(20)
        assert stopped.is_set() and not thread.is_alive()
        assert not errors and outcomes == [None]
    assert not (root / ("a" * 32)).exists()
    descriptor = lock_root(root)
    os.close(descriptor)


def test_actual_process_exit_before_marker_releases_root_inode_without_creating_controls(tmp_path):
    root = tmp_path / "owned"
    script = """import os,sys
from pathlib import Path
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
actual=os.open
def interrupted(name,*args,**kwargs):
    if name==".coordinator":os._exit(86)
    return actual(name,*args,**kwargs)
os.open=interrupted
with lease_journal_validation_workspace(
    Path(sys.argv[1]),"a"*32,create=True,creation_current=lambda:True
):
    raise AssertionError("interrupted creator entered body")
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(root)], capture_output=True, check=False, timeout=30
    )
    assert result.returncode == 86, result.stderr.decode()[-1000:]
    assert root.is_dir() and not (root / ".coordinator").exists()
    assert not (root / ("a" * 32)).exists()
    descriptor = lock_root(root)
    os.close(descriptor)
