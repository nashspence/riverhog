import hashlib
import os
import subprocess
import sys
from pathlib import Path

import pytest
from riverhog_client import _prepared_primary_posix as owner

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX filesystem backend")


def test_exact_no_clobber_and_cold_verified_reader(tmp_path: Path):
    root = tmp_path.resolve() / "state"
    original = b"original primary" * 12345
    with owner.PrimaryFiles(root, initialize=True) as files:
        file = files.put("journal", (original[:17], original[17:]), byte_limit=len(original))
        assert (file.bytes, file.sha256) == (len(original), hashlib.sha256(original).hexdigest())
        with pytest.raises(FileExistsError):
            files.put("journal", (b"replacement",), byte_limit=1024)
        with owner.PrimaryFiles(tmp_path.resolve() / "independent", initialize=True) as independent:
            assert independent.put("marker", (b"one",), byte_limit=3).bytes == 3
    with owner.PrimaryFiles(root, initialize=False) as files:
        with files.verified(file) as stream:
            assert stream.read() == original
        assert files.read_control("journal", byte_limit=len(original)) == original
        with pytest.raises(owner.PrimaryStateCapacity):
            files.read_control("journal", byte_limit=len(original) - 1)


def test_capacity_expected_identity_and_zero_write_fail_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    with owner.PrimaryFiles(tmp_path.resolve() / "state", initialize=True) as files:
        with pytest.raises(owner.PrimaryStateCapacity):
            files.put("journal", (b"123", b"4"), byte_limit=3)
        with pytest.raises(owner.PrimaryStateError):
            files.put("journal", (b"one",), byte_limit=3, expected=(3, "f" * 64))
        with monkeypatch.context() as patch:
            patch.setattr(owner.os, "write", lambda *_args: 0)
            with pytest.raises(OSError, match="short write"):
                files.put("journal", (b"one",), byte_limit=3)
        assert sorted(p.name for p in files.root.iterdir()) == [".owner"]


def test_partial_writes_are_completed_and_hash_exact_octets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    write = os.write
    actual = b"primary-journal" * 40
    with owner.PrimaryFiles(tmp_path.resolve() / "state", initialize=True) as files:
        with monkeypatch.context() as patch:
            patch.setattr(owner.os, "write", lambda descriptor, value: write(descriptor, value[:3]))
            file = files.put("journal", (actual,), byte_limit=len(actual))
        with files.verified(file) as stream:
            assert stream.read() == actual


@pytest.mark.parametrize("boundary", [1, 2, 3])
def test_fsync_failure_is_not_an_accepted_barrier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: int
):
    root = tmp_path.resolve() / "state"
    sync = os.fsync
    count = 0
    with owner.PrimaryFiles(root, initialize=True) as files:

        def failing(descriptor):
            nonlocal count
            count += 1
            if count == boundary:
                raise OSError("fsync failure")
            return sync(descriptor)

        with monkeypatch.context() as patch:
            patch.setattr(owner.os, "fsync", failing)
            with pytest.raises(OSError, match="fsync failure"):
                files.put("journal", (b"original",), byte_limit=8)
        assert count >= boundary
        assert (root / "journal").exists() == (boundary != 1)
    with owner.PrimaryFiles(root, initialize=False) as files:
        if boundary != 1:
            file = owner.PrimaryByteFile("journal", 8, hashlib.sha256(b"original").hexdigest())
            with files.verified(file) as stream:
                assert stream.read() == b"original"


@pytest.mark.parametrize("corruption", ["truncated", "changed", "missing", "symlink"])
def test_corrupt_pending_file_never_yields_a_replay_reader(tmp_path: Path, corruption: str):
    root = tmp_path.resolve() / "state"
    with owner.PrimaryFiles(root, initialize=True) as files:
        file = files.put("journal", (b"original",), byte_limit=8)
    if corruption == "truncated":
        (root / "journal").write_bytes(b"ori")
    elif corruption == "changed":
        (root / "journal").write_bytes(b"change!!")
    elif corruption == "missing":
        (root / "journal").unlink()
    else:
        (root / "journal").unlink()
        (root / "journal").symlink_to(tmp_path.resolve() / "unrelated")
    with owner.PrimaryFiles(root, initialize=False) as files:
        with pytest.raises((owner.PrimaryStateError, OSError)):
            with files.verified(file):
                pytest.fail("corrupt primary reached a replay consumer")


def test_lifetime_owner_is_exclusive_until_the_old_process_exits(tmp_path: Path):
    root = tmp_path.resolve() / "state"
    code = (
        "import sys,time;from pathlib import Path;"
        "from riverhog_client._prepared_primary_posix import PrimaryFiles;"
        "files=PrimaryFiles(Path(sys.argv[1]),initialize=True);"
        "files.put('prepared',(b'original',),byte_limit=8);"
        "print('ready',flush=True);time.sleep(300)"
    )
    env = dict(os.environ)
    process = subprocess.Popen(
        [sys.executable, "-c", code, str(root)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().strip() == "ready"
        with pytest.raises(owner.PrimaryStateBusy):
            owner.PrimaryFiles(root, initialize=False)
        with pytest.raises(owner.PrimaryStateBusy):
            owner.PrimaryFiles(root, initialize=False)
        process.kill()
        assert process.wait(timeout=10) != 0
        with owner.PrimaryFiles(root, initialize=False) as files:
            assert files.read_control("prepared", byte_limit=8) == b"original"
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        process.stdout.close()
        process.stderr.close()


def test_missing_owner_and_replaced_directory_are_not_fresh_allocations(tmp_path: Path):
    root = tmp_path.resolve() / "state"
    with owner.PrimaryFiles(root, initialize=True) as files:
        root.rename(tmp_path.resolve() / "old")
        root.mkdir(mode=0o700)
        with pytest.raises(owner.PrimaryStateError):
            files.check_current()
        with pytest.raises(FileNotFoundError):
            owner.PrimaryFiles(root, initialize=False)
    root.rmdir()
    root.symlink_to(tmp_path.resolve() / "old", target_is_directory=True)
    with pytest.raises(owner.PrimaryStateError):
        owner.PrimaryFiles(root, initialize=False)


def test_member_directories_share_the_exact_construction_owner(tmp_path: Path):
    root = tmp_path.resolve() / "state"
    files = owner.PrimaryFiles(root, initialize=True)
    with files.child("members", initialize=True) as members:
        with members.child("ab", initialize=True) as shard:
            with shard.child("abcd", initialize=True) as member:
                file = member.put("journal", (b"original",), byte_limit=8)
                with pytest.raises(owner.PrimaryStateBusy):
                    owner.PrimaryFiles(root, initialize=False)
                files.close()
                with pytest.raises(owner.PrimaryStateBusy):
                    owner.PrimaryFiles(root, initialize=False)
                with pytest.raises(owner.PrimaryStateError):
                    member.read_control("journal", byte_limit=8)
    with owner.PrimaryFiles(root, initialize=False) as files:
        with files.child("members", initialize=False) as members:
            with members.child("ab", initialize=False) as shard:
                with shard.child("abcd", initialize=False) as member:
                    with member.verified(file) as stream:
                        assert stream.read() == b"original"


def test_control_updates_compare_original_bytes_and_never_create_missing_winners(tmp_path):
    root = tmp_path.resolve() / "state"
    original = b'{"sequence":"0"}\n'
    selected = b'{"sequence":"1"}\n'
    with owner.PrimaryFiles(root, initialize=True) as files:
        files.put_control("usage", original, byte_limit=100)
        with pytest.raises(FileExistsError):
            files.put_control("usage", selected, byte_limit=100)
        with pytest.raises(owner.PrimaryStateError, match="comparison"):
            files.put_control("usage", selected, expected=original[:-1], byte_limit=100)
        with pytest.raises(owner.PrimaryStateError, match="comparison"):
            files.put_control("missing", selected, expected=original, byte_limit=100)
        assert not files.exists("missing")
        assert files.read_control("usage", byte_limit=100) == original
        files.put_control("usage", selected, expected=original, byte_limit=100)
    with owner.PrimaryFiles(root, initialize=False) as files:
        assert files.read_control("usage", byte_limit=100) == selected
        with pytest.raises(owner.PrimaryStateError, match="comparison"):
            files.remove_control("usage", expected=original)
        assert files.read_control("usage", byte_limit=100) == selected
        files.remove_control("usage", expected=selected)
        files.remove_control("usage", expected=selected)
        assert not files.exists("usage")


@pytest.mark.parametrize("boundary", [1, 2])
def test_uncertain_control_switch_preserves_only_the_old_or_selected_exact_body(
    tmp_path, monkeypatch, boundary
):
    root = tmp_path.resolve() / "state"
    sync = os.fsync
    count = 0
    with owner.PrimaryFiles(root, initialize=True) as files:
        files.put_control("usage", b"original", byte_limit=100)

        def failing(descriptor):
            nonlocal count
            count += 1
            if count == boundary:
                raise OSError("control switch fsync failure")
            return sync(descriptor)

        with monkeypatch.context() as patch:
            patch.setattr(owner.os, "fsync", failing)
            with pytest.raises(OSError, match="control switch fsync"):
                files.put_control("usage", b"selected", expected=b"original", byte_limit=100)
    with owner.PrimaryFiles(root, initialize=False) as files:
        expected = b"original" if boundary == 1 else b"selected"
        assert files.read_control("usage", byte_limit=100) == expected
        files.put_control("usage", b"next", expected=expected, byte_limit=100)
        assert files.read_control("usage", byte_limit=100) == b"next"
        assert not files.exists("candidate-usage")


def test_control_admission_rejects_invalid_input_before_writing_any_candidate(tmp_path):
    with owner.PrimaryFiles(tmp_path.resolve() / "state", initialize=True) as files:
        files.put_control("usage", b"original", byte_limit=8)
        with pytest.raises(owner.PrimaryStateCapacity):
            files.put_control("usage", b"too large", expected=b"original", byte_limit=8)
        for body, expected in [(bytearray(b"x"), None), (b"x", bytearray(b"original"))]:
            with pytest.raises(TypeError, match="original bytes"):
                files.put_control("usage", body, expected=expected, byte_limit=8)
        for invalid in [-1, True, 1.5]:
            with pytest.raises(ValueError, match="admission"):
                files.put_control("usage", b"x", expected=b"original", byte_limit=invalid)
        with pytest.raises(owner.PrimaryStateError):
            files.remove_control("usage", expected=b"")
        assert files.read_control("usage", byte_limit=8) == b"original"
        assert sorted(p.name for p in files.root.iterdir()) == [".owner", "usage"]
