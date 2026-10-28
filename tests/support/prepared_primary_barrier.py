"""Fixed-width barrier progression, quota envelope and retained-original witnesses."""

from __future__ import annotations

import ctypes
from contextlib import nullcontext

from riverhog_client import PrimaryStateError

from tests.support.sqlite_vfs_faults import _PF, WriteLimits

STARTS = (0, 127, 32767, (1 << 32) - 1, (1 << 63) - 1, (1 << 64) - 3)
DAMAGED = {"short": b"\0" * 7, "text": "00000000", "integer": 1}
_CONTROL = b"original control"
_PAYLOAD = b"original retained payload"


def _prepare(files):
    files.prepare_controls(("original",), byte_limit=8192)
    files.put_control("original", _CONTROL, byte_limit=8192)
    return files.put_bytes("journal", (_PAYLOAD,), byte_limit=len(_PAYLOAD))


def _originals(files, payload):
    assert files.read_control("original", byte_limit=8192) == _CONTROL
    with files.verified(payload) as stream:
        assert stream.read() == _PAYLOAD


def _fixed_quota(files, shim):
    page = files._one("PRAGMA page_size")[0]
    addresses = [address for address, kind in shim.files.items() if kind == "database"]
    assert len(addresses) == 1
    sector = shim.io.sector(ctypes.cast(addresses[0], _PF))
    assert page == 4096 and sector > 0 and not sector & (sector - 1)
    files.check_current()
    with files._guard.path.open("rb") as stream:
        stream.seek(20)
        reserved = stream.read(1)[0]
    files.check_current()
    usable = page - reserved
    assert usable > 4
    pages = max(1, (sector + page - 1) // page) * (1 + 2 + (8 + usable - 5) // (usable - 4))
    journal = 2 * sector + pages * (page + 8) + 16
    limits = files._limits
    forward = WriteLimits(page, pages, limits.database_bytes, journal, 8 * journal, 16 * pages + 32)
    recovery = WriteLimits(
        page,
        1 + limits.journal_bytes // (page + 8),
        limits.database_bytes,
        limits.journal_bytes,
        4 * (limits.database_bytes + limits.journal_bytes),
        65536,
    )
    return shim.write_quota(forward=forward, recovery=recovery), {
        "page_bytes": page,
        "sector_bytes": sector,
        "reserved_bytes": reserved,
        "database_pages": pages,
        "journal_write_extent_bytes": journal,
    }


def progress(opened, start, *, shim=None):
    with opened(initialize=True) as files:
        payload = _prepare(files)
        files._db().execute(
            "UPDATE primary_format SET barrier=? WHERE id=1", (start.to_bytes(8, "big"),)
        ).close()
        before = files.space_usage()
        context, bound = (nullcontext(), None) if shim is None else _fixed_quota(files, shim)
        with context as quota:
            files.sync()
        assert files._one("SELECT typeof(barrier),length(barrier),barrier FROM primary_format") == (
            "blob",
            8,
            (start + 1).to_bytes(8, "big"),
        )
        assert files.space_usage() == before
        _originals(files, payload)
        measured = None if quota is None else quota.report()
        if measured is not None:
            assert measured["refusals"] == measured["native_failures"] == 0
            assert 0 < measured["forward"]["database_pages_attempted"] <= bound["database_pages"]
            assert (
                measured["forward"]["journal_write_extent_bytes"]
                <= bound["journal_write_extent_bytes"]
            )
    with opened(initialize=False) as files:
        assert files._one("SELECT barrier FROM primary_format") == ((start + 2).to_bytes(8, "big"),)
        _originals(files, payload)
        assert files.space_usage() == before
    return {
        "start": str(start),
        "after_sync": str(start + 1),
        "after_cold_barrier": str(start + 2),
        "fixed_extent_bytes": 8,
        "original_control_exact": True,
        "original_payload_exact": True,
        "database_and_journal_growth": False,
        "derived_bound": bound,
        "quota": measured,
        "quota_phase": "explicit runtime barrier after prepared control readiness",
        "initialization_and_cold_recovery_quota_certified": False,
    }


def reject(opened, damage):
    assert damage in ("exhausted", *DAMAGED)
    value = ((1 << 64) - 1).to_bytes(8, "big") if damage == "exhausted" else DAMAGED[damage]
    with opened(initialize=True) as files:
        _prepare(files)
        db = files._db()
        original = db.execute("SELECT body FROM primary_controls WHERE name='original'").fetchone()
        db.execute("PRAGMA ignore_check_constraints=1").close()
        db.execute("UPDATE primary_format SET barrier=? WHERE id=1", (value,)).close()
        db.execute("PRAGMA ignore_check_constraints=0").close()
        try:
            files.sync()
        except PrimaryStateError as exc:
            assert "barrier" in str(exc)
        else:
            raise AssertionError("invalid or exhausted barrier was accepted")
        assert files._root._poisoned
        assert db.execute("SELECT barrier FROM primary_format").fetchone() == (value,)
        assert (
            db.execute("SELECT body FROM primary_controls WHERE name='original'").fetchone()
            == original
        )
        assert db.execute("SELECT body FROM primary_chunks").fetchall() == [(_PAYLOAD,)]
    try:
        opened(initialize=False)
    except PrimaryStateError as exc:
        assert "barrier" in str(exc)
    else:
        raise AssertionError("cold open accepted an invalid or exhausted barrier")
    return {
        "damage": damage,
        "rejected_without_replacement": True,
        "owner_poisoned": True,
        "cold_open_rejected": True,
        "original_control_exact": True,
        "original_payload_exact": True,
    }
