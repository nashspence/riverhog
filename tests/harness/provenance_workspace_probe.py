"""Exercise disk-backed validation scratch before expensive native proofs."""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from tempfile import gettempdir

from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.scratch_workspace import process_workspace, scratch_directory

_HOLD_WORKSPACE = """
import sys
from pathlib import Path
from riverhog_core.scratch_workspace import process_workspace

with process_workspace(Path(sys.argv[1])) as owner:
    print(owner, flush=True)
    sys.stdin.readline()
"""


def check() -> dict[str, object]:
    # This fixture exceeds the separate 64 MiB /tmp mount; it is not an
    # acceptance limit on a journal, corpus, collection, or temporary workspace.
    assert Path(gettempdir()) == Path(os.environ["TMPDIR"])
    record = bytes(256 * 1024)
    count = 320
    base = Path(gettempdir())
    with process_workspace(base) as live_owner:
        with scratch_directory(prefix="riverhog-validation-workspace-") as temporary:
            root = Path(temporary)
            assert root.stat().st_mode & 0o777 == 0o700
            database = root / "validation.sqlite3"
            connection = sqlite3.connect(database)
            try:
                connection.execute("PRAGMA synchronous = OFF")
                connection.execute("PRAGMA cache_size = -512")
                connection.execute("PRAGMA temp_store = FILE")
                connection.execute("CREATE TABLE records (ordinal INTEGER PRIMARY KEY, value BLOB)")
                connection.executemany(
                    "INSERT INTO records VALUES (?, ?)",
                    ((ordinal, record) for ordinal in range(count)),
                )
                connection.commit()
                observed = connection.execute(
                    "SELECT count(*), sum(length(value)) FROM records"
                ).fetchone()
                assert observed == (count, count * len(record))
                assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
                size = database.stat().st_size
                assert size > 64 * 1024 * 1024
            finally:
                connection.close()
            child = subprocess.Popen(
                [sys.executable, "-c", _HOLD_WORKSPACE, str(base)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
            )
            try:
                assert child.stdout is not None
                receipt = child.stdout.readline().strip()
                if not receipt:
                    raise RuntimeError("scratch child exited without its workspace receipt")
                abandoned = Path(receipt)
                database.replace(abandoned / "validation.sqlite3")
                keep = live_owner / "keep"
                keep.write_bytes(b"live operation")
                child.kill()
                child.wait(timeout=10)
                assert abandoned.exists()
                with process_workspace(base):
                    assert not abandoned.exists()
                    assert keep.read_bytes() == b"live operation"
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=10)
                if child.stdin is not None:
                    child.stdin.close()
                if child.stdout is not None:
                    child.stdout.close()
        assert not root.exists()
    assert not live_owner.exists()
    return {
        "proof": "provenance-workspace",
        "sqlite_bytes": size,
        "removed": True,
        "hard_kill_reclaimed": True,
        "live_owner_preserved": True,
    }


if __name__ == "__main__":
    print(canonical_json_bytes(check()).decode("utf-8"))
