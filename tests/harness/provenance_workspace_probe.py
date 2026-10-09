"""Exercise disk-backed validation scratch before expensive native proofs."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir

from riverhog_canonical_json import canonical_json_bytes


def check() -> dict[str, object]:
    # This fixture exceeds the separate 64 MiB /tmp mount; it is not an
    # acceptance limit on a journal, corpus, collection, or temporary workspace.
    assert Path(gettempdir()) == Path(os.environ["TMPDIR"])
    record = bytes(256 * 1024)
    count = 320
    with TemporaryDirectory(prefix="riverhog-validation-workspace-") as temporary:
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
                "INSERT INTO records VALUES (?, ?)", ((ordinal, record) for ordinal in range(count))
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
    assert not root.exists()
    return {"proof": "provenance-workspace", "sqlite_bytes": size, "removed": True}


if __name__ == "__main__":
    print(canonical_json_bytes(check()).decode("utf-8"))
