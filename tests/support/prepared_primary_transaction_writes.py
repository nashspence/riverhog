"""Aggregate actual transaction write shapes without retaining a growing event trace."""

from __future__ import annotations

from contextlib import contextmanager


class TransactionWriteTotals:
    def __init__(self, *, tracked_pages):
        self.tracked_pages = tracked_pages
        self.completed = self.failed = 0
        self.maxima = {
            "database_pages_written": 0,
            "database_write_bytes": 0,
            "journal_write_bytes": 0,
            "journal_write_extent_bytes": 0,
            "other_write_bytes": 0,
        }

    def attach(self, files, shim):
        original = files._transaction

        @contextmanager
        def transaction(*, payload=False, preparing=False, increase=None):
            with shim.transaction_writes(
                page_bytes=4096, tracked_pages=self.tracked_pages
            ) as trace:
                completed = False
                try:
                    with original(payload=payload, preparing=preparing, increase=increase) as db:
                        yield db
                    completed = True
                finally:
                    self.completed += int(completed)
                    self.failed += int(not completed)
                    for key, value in trace.report().items():
                        self.maxima[key] = max(self.maxima[key], value)

        files._transaction = transaction
        return files

    def report(self):
        return {
            "completed_transactions": self.completed,
            "failed_transactions": self.failed,
            "maxima": dict(self.maxima),
            "tracked_page_capacity": self.tracked_pages,
            "includes_initialization": False,
            "includes_open_barriers": False,
            "cache_peak_certified": False,
            "dirty_page_admission_certified": False,
            "scope": "successful VFS write delegations within each runtime SQL transaction",
        }
