from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager

import pytest
import retained_resource
import sqlite_admission
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import RetainedCorpusValidator, StaleCorpusWorkspace
from riverhog_provenance import corpus_workspace as owner
from test_corpus_order_growth import edges_for, insert
from test_retained_corpus_faults import EmptySource, UncertainClose
from test_retained_corpus_workspace import open_workspace
from test_source_anchor_growth import record


def octets(value):
    if isinstance(value, (tuple, list)):
        return sum(octets(item) for item in value)
    if isinstance(value, bytes):
        return len(value)
    return 0 if value is None else len(str(value).encode())


class Cursor:
    def __init__(self, cursor, observer):
        self.cursor, self.observer = cursor, observer
        self.closed = False
        observer.live += 1
        observer.maximum_live = max(observer.maximum_live, observer.live)

    @property
    def rowcount(self):
        return self.cursor.rowcount

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is not None:
            self.observer.metrics["returned_rows"] += 1
            self.observer.metrics["returned_octets"] += octets(row)
            for value in row:
                if isinstance(value, bytes):
                    self.observer.checkpoint_octets = max(
                        self.observer.checkpoint_octets, len(value)
                    )
        return row

    def close(self):
        if not self.closed:
            self.cursor.close()
            self.closed = True
            self.observer.live -= 1


class Connection:
    """Keep the existing outer Native-close fault boundary observable."""

    def __init__(self, db, observer):
        self.db, self.observer = db, observer

    def __getattr__(self, name):
        return getattr(self.db, name)

    def execute(self, query, parameters=()):
        self.observer.metrics["sql_statements"] += 1
        self.observer.metrics["parameter_octets"] += octets(parameters)
        assert not query.startswith("SELECT") or "FROM assertion_payloads" not in query
        return Cursor(self.db.execute(query, parameters), self.observer)

    def commit(self):
        self.observer.metrics["sql_statements"] += 1
        self.db.commit()

    def rollback(self):
        self.observer.metrics["sql_statements"] += 1
        self.db.rollback()

    def close(self):
        self.observer.metrics["closed_connections"] += 1
        self.observer.close_depth += 1
        try:
            self.db.close()
        finally:
            self.observer.close_depth -= 1


class Observer:
    def __init__(self):
        self.metrics = defaultdict(int)
        self.live, self.maximum_live = 0, 0
        self.close_depth = 0
        self.checkpoint_octets = 0

    @contextmanager
    def observe(self, monkeypatch, source, workspace=None, *, observe_sql=True):
        driver, checkpoint, restore = sqlite_admission._connect, owner._checkpoint, owner._restore
        execute = sqlite_admission._Cursor.execute
        close = sqlite_admission._Connection.close
        transaction = sqlite_admission._Connection._transaction
        current = source.verify_current
        fstat, stat, lstat = owner.os.fstat, owner.os.stat, owner.os.lstat
        resource_type = retained_resource.Resource
        resources, cursors = [], []

        def retain_resource(original):
            resources.append((original, original.metrics))
            return original

        def allocate(kind):
            return retain_resource(resource_type(kind))

        if workspace is not None:
            retain_resource(workspace._construction.guard)
            retain_resource(workspace._construction.directory)

        def inspected(operation):
            def inspect(*args, **kwargs):
                self.metrics["file_identity_reads"] += 1
                return operation(*args, **kwargs)

            return inspect

        def opened(*args, **kwargs):
            self.metrics["opened_connections"] += 1
            return driver(*args, **kwargs)

        def executing(cursor, query, parameters=()):
            self.metrics["sql_statements"] += 1
            self.metrics["parameter_octets"] += octets(parameters)
            assert not query.startswith("SELECT") or "FROM assertion_payloads" not in query
            if cursor not in cursors:
                cursors.append(cursor)
            self.live = sum(not item._cleanup_complete for item in cursors)
            self.maximum_live = max(self.maximum_live, self.live)
            return execute(cursor, query, parameters)

        def read_rows(actual, many):
            def reading(cursor, *args, **kwargs):
                rows = actual(cursor, *args, **kwargs)
                values = rows if many else (() if rows is None else (rows,))
                for row in values:
                    self.metrics["returned_rows"] += 1
                    self.metrics["returned_octets"] += octets(row)
                    for value in row:
                        if isinstance(value, bytes):
                            self.checkpoint_octets = max(self.checkpoint_octets, len(value))
                return rows

            return reading

        def transacted(connection, method):
            self.metrics["sql_statements"] += 1
            return transaction(connection, method)

        def closed(connection):
            if not self.close_depth and not connection.cleanup_complete:
                self.metrics["closed_connections"] += 1
            return close(connection)

        def checked():
            self.metrics["source_checks"] += 1
            return current()

        def encoded(corpus):
            value = checkpoint(corpus)
            self.metrics["checkpoint_encoded_octets"] += len(value)
            self.checkpoint_octets = max(self.checkpoint_octets, len(value))
            return value

        def decoded(corpus, raw):
            self.metrics["checkpoint_decoded_octets"] += len(raw)
            self.checkpoint_octets = max(self.checkpoint_octets, len(raw))
            return restore(corpus, raw)

        with monkeypatch.context() as patch:
            patch.setattr(sqlite_admission, "_connect", opened)
            if observe_sql:
                patch.setattr(sqlite_admission._Cursor, "execute", executing)
                for name in ("fetchone", "fetchmany", "fetchall"):
                    patch.setattr(
                        sqlite_admission._Cursor,
                        name,
                        read_rows(getattr(sqlite_admission._Cursor, name), name != "fetchone"),
                    )
                patch.setattr(sqlite_admission._Connection, "_transaction", transacted)
            patch.setattr(sqlite_admission._Connection, "close", closed)
            patch.setattr(retained_resource, "Resource", allocate)
            patch.setattr(owner, "_checkpoint", encoded)
            patch.setattr(owner, "_restore", decoded)
            patch.setattr(source, "verify_current", checked)
            patch.setattr(owner.os, "fstat", inspected(fstat))
            patch.setattr(owner.os, "stat", inspected(stat))
            patch.setattr(owner.os, "lstat", inspected(lstat))
            try:
                yield
            finally:
                for original, before in resources:
                    after = original.metrics
                    for metric, counter in (
                        ("descriptor_opens", "open_calls"),
                        ("descriptor_closes", "close_calls"),
                        ("fsync_calls", "sync_calls"),
                        ("file_identity_reads", "fstat_calls"),
                    ):
                        self.metrics[metric] += after[counter] - before[counter]
        if observe_sql:
            self.live = sum(not cursor._cleanup_complete for cursor in cursors)
        assert self.live == 0 and self.maximum_live <= 1

    def sample(self, root, source, rows, *, counter_octets=2):
        return record(
            growth_binding(getattr(root, "__func__", root)),
            {
                "binding_octets": len(source.binding.to_bytes()),
                "checkpoint_octets": self.checkpoint_octets,
                "counter_octets": counter_octets,
                "retained_rows": rows,
            },
            self.metrics,
        )


@pytest.mark.growth_witness(
    "native-retained-corpus-transaction-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_workspace.RetainedCorpusValidator._operation",
)
def test_owner_checkpoint_work_does_not_scan_retained_inputs(
    journal_constructions, tmp_path, catalog, monkeypatch, record_property
):
    samples = []
    for count in (0, 1, 17, 129):
        for width in (1, 17, 129):
            path = tmp_path / (str(count) + "-" + str(width))
            path.mkdir(mode=0o700)
            source = EmptySource(catalog)
            with open_workspace(
                path, source, create=True, journal_constructions=journal_constructions
            ) as workspace:
                with workspace._operation():
                    insert(workspace._corpus, edges_for("chain", count), "derivation")
                version = str(10 ** (width - 1))
                workspace._db.execute("UPDATE corpus_workspace SET version=?", (version,))
                workspace._db.commit()
                observer = Observer()
                with observer.observe(monkeypatch, source, workspace):
                    with workspace._operation():
                        pass
                samples.append(
                    observer.sample(
                        RetainedCorpusValidator._operation,
                        source,
                        count,
                        counter_octets=max(2, width),
                    )
                )
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "native-retained-corpus-open-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_workspace.RetainedCorpusValidator.open_retained",
)
def test_actual_owner_create_reopen_and_rejection_are_metadata_work(
    journal_constructions, tmp_path, catalog, monkeypatch, record_property
):
    samples = []
    for count in (0, 1, 17, 129):
        path = tmp_path / str(count)
        path.mkdir(mode=0o700)
        source = EmptySource(catalog)
        observer = Observer()
        retained = []

        def retain(handle, observed=observer, owned=retained):
            observed.metrics["opening_callbacks"] += 1
            owned.append(handle)

        with observer.observe(monkeypatch, source):
            workspace = open_workspace(
                path,
                source,
                create=True,
                catalog=catalog,
                on_storage_open=retain,
                journal_constructions=journal_constructions,
            )
        assert retained == [workspace]
        samples.append(observer.sample(RetainedCorpusValidator.open_retained, source, 0))
        # Subsequent actual SQL belongs to fixture setup, not this opening sample.
        with workspace._operation():
            insert(workspace._corpus, edges_for("chain", count), "derivation")
        workspace.close()
        observer = Observer()
        with observer.observe(monkeypatch, source):
            workspace = open_workspace(
                path, source, epoch=1, catalog=catalog, journal_constructions=journal_constructions
            )
        samples.append(observer.sample(RetainedCorpusValidator.open_retained, source, count))
        workspace.close()
        observer = Observer()
        with observer.observe(monkeypatch, source):
            with pytest.raises(ValueError, match="newer"):
                open_workspace(
                    path,
                    source,
                    epoch=1,
                    catalog=catalog,
                    journal_constructions=journal_constructions,
                )
        samples.append(observer.sample(RetainedCorpusValidator.open_retained, source, count))
        (path / "corpus.sqlite3").unlink()
        observer = Observer()
        with observer.observe(monkeypatch, source):
            with pytest.raises(StaleCorpusWorkspace, match="missing"):
                open_workspace(
                    path,
                    source,
                    epoch=2,
                    catalog=catalog,
                    journal_constructions=journal_constructions,
                )
        samples.append(observer.sample(RetainedCorpusValidator.open_retained, source, count))
        rejected = tmp_path / ("rejected-" + str(count))
        rejected.mkdir(mode=0o700)
        source = EmptySource(catalog)
        current = [True]
        source.verify_current = lambda state=current: state[0]
        observer = Observer()
        retained = []

        def retain_rejected(handle, observed=observer, owned=retained, state=current):
            observed.metrics["opening_callbacks"] += 1
            owned.append(handle)
            state[0] = False

        def release_rejected(observed=observer):
            observed.metrics["release_callbacks"] += 1

        with observer.observe(monkeypatch, source):
            with pytest.raises(owner.IncompleteSourceError):
                open_workspace(
                    rejected,
                    source,
                    create=True,
                    catalog=catalog,
                    on_storage_open=retain_rejected,
                    on_storage_close=release_rejected,
                    journal_constructions=journal_constructions,
                )
        assert len(retained) == 1 and retained[0]._storage_closed
        assert retained[0]._construction.borrow.active
        assert observer.metrics["opening_callbacks"] == 1
        assert observer.metrics["release_callbacks"] == 0
        samples.append(observer.sample(RetainedCorpusValidator.open_retained, source, count))
        # A failed constructor left the release callback with its caller.
        retained[0].close()
        assert retained[0]._construction.dependents_received
        release_rejected()
        assert observer.metrics["release_callbacks"] == 1
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "native-retained-corpus-close-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_workspace.RetainedCorpusValidator.close",
)
def test_actual_close_releases_only_its_native_handle(
    journal_constructions, tmp_path, catalog, monkeypatch, record_property
):
    samples = []
    for count in (0, 1, 17, 129, 1025):
        for failure in (None, "before", "lost-reply"):
            path = tmp_path / (str(count) + "-" + str(failure))
            path.mkdir(mode=0o700)
            source = EmptySource(catalog)
            active = [Observer()]

            def release(observers=active):
                observers[0].metrics["release_callbacks"] += 1

            workspace = open_workspace(
                path,
                source,
                create=True,
                on_storage_close=release,
                journal_constructions=journal_constructions,
            )
            with workspace._operation():
                insert(workspace._corpus, edges_for("chain", count), "derivation")
            db = workspace._db
            closing = (
                db if failure is None else UncertainClose(db, lost_reply=failure == "lost-reply")
            )
            for attempt in range(3):
                observer = active[0] = Observer()
                with observer.observe(monkeypatch, source, workspace):
                    workspace._corpus._db = Connection(closing, observer)
                    if failure is not None and attempt == 0:
                        with pytest.raises(OSError, match="unconfirmed"):
                            workspace.close()
                    else:
                        workspace.close()
                samples.append(observer.sample(RetainedCorpusValidator.close, source, count))
                expected_release = int(attempt == (0 if failure is None else 1))
                assert observer.metrics["release_callbacks"] == expected_release
                assert observer.metrics["closed_connections"] == int(
                    attempt == 0 or failure is not None and attempt == 1
                )
            assert (path / "corpus.sqlite3").is_file()
    record_property("growth_samples", samples)
