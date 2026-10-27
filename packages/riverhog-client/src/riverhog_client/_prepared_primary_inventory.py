"""Disk-backed ordering of an owner inventory under its construction lease."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING

from riverhog_protocol import ArtifactMemberIdentityDocument

from riverhog_client._prepared_primary_types import PrimaryStateCapacity, PrimaryStateError

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_store import PrimaryStore
    from riverhog_client.producer import ProducerArtifactIdentity


def ordered_primary_inventory(
    store: PrimaryStore, members: Iterable[ProducerArtifactIdentity]
) -> Iterator[ProducerArtifactIdentity]:
    """Order once in bounded scratch; this is not a durable membership authority.

    Canonical opaque-ID ordering costs O(N log N) indexed work and O(N)
    retained scratch bytes, independently of point preparation/replay. Its fixed page cache
    and configured scratch admission do not retain the owner inventory in memory.
    """
    from riverhog_client.producer import ProducerArtifactIdentity

    page_size = 4096
    page_limit = (store.limits.inventory_order_bytes - 2 * page_size) // page_size
    if page_limit < 1:
        raise PrimaryStateCapacity("owner inventory ordering has no scratch admission")
    with store._mutex:
        try:
            opened = store._files.child("inventory-ordering", initialize=True)
        except FileExistsError:
            opened = store._files.child("inventory-ordering", initialize=False)
        with opened as files:
            # Only a previous interrupted ordering under this exact lease can own this file.
            files.remove("selection.sqlite3-journal")
            files.remove("selection.sqlite3")
            files.put("selection.sqlite3", (), byte_limit=1)
            files.check_current()
            connection = sqlite3.connect(files.root / "selection.sqlite3")
            try:
                connection.execute(f"PRAGMA page_size = {page_size}")
                connection.execute(f"PRAGMA max_page_count = {page_limit}")
                connection.execute("PRAGMA cache_size = -512")
                connection.execute("PRAGMA journal_mode = OFF")
                connection.execute("PRAGMA synchronous = OFF")
                connection.execute(
                    "CREATE TABLE members (artifact_id TEXT PRIMARY KEY, bytes TEXT NOT NULL, "
                    "sha256 TEXT NOT NULL) WITHOUT ROWID"
                )
                for supplied in members:
                    member = ArtifactMemberIdentityDocument.model_validate(
                        {
                            "artifact_id": supplied.artifact_id,
                            "bytes": str(supplied.bytes),
                            "sha256": supplied.sha256,
                        }
                    )
                    connection.execute(
                        "INSERT INTO members VALUES (?, ?, ?)",
                        (str(member.artifact_id), str(member.bytes), member.sha256),
                    )
                connection.commit()
                files.sync()
                for artifact_id, byte_count, digest in connection.execute(
                    "SELECT artifact_id, bytes, sha256 FROM members ORDER BY artifact_id"
                ):
                    yield ProducerArtifactIdentity(artifact_id, int(byte_count), digest)
            except sqlite3.IntegrityError as exc:
                raise PrimaryStateError("owner inventory contains duplicate artifact IDs") from exc
            except sqlite3.OperationalError as exc:
                if getattr(exc, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL:
                    raise PrimaryStateCapacity(
                        "owner inventory ordering exceeds scratch admission"
                    ) from exc
                raise
            finally:
                connection.close()
                files.remove("selection.sqlite3")
