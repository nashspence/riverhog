from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager

import pytest
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
        self.cursor.close()
        self.observer.live -= 1


class Connection:
    def __init__(self, db, observer):
        self.db, self.observer = db, observer

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
        self.db.close()


class Observer:
    def __init__(self):
        self.metrics = defaultdict(int)
        self.live, self.maximum_live = 0, 0
        self.checkpoint_octets = 0

    @contextmanager
    def observe(self, monkeypatch, source, workspace=None):
        connect, checkpoint, restore = owner.sqlite3.connect, owner._checkpoint, owner._restore
        current = source.verify_current

        def opened(*args, **kwargs):
            self.metrics["opened_connections"] += 1
            return Connection(connect(*args, **kwargs), self)

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
            patch.setattr(owner.sqlite3, "connect", opened)
            patch.setattr(owner, "_checkpoint", encoded)
            patch.setattr(owner, "_restore", decoded)
            patch.setattr(source, "verify_current", checked)
            db = None if workspace is None else workspace._db
            if db is not None:
                patch.setattr(workspace, "_db", Connection(db, self))
            yield
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
    tmp_path, catalog, monkeypatch, record_property
):
    samples = []
    for count in (0, 1, 17, 129):
        for width in (1, 17, 129):
            path = tmp_path / (str(count) + "-" + str(width))
            path.mkdir()
            source = EmptySource(catalog)
            with open_workspace(path, source, create=True) as workspace:
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
    tmp_path, catalog, monkeypatch, record_property
):
    samples = []
    for count in (0, 1, 17, 129):
        path = tmp_path / str(count)
        path.mkdir()
        source = EmptySource(catalog)
        observer = Observer()
        with observer.observe(monkeypatch, source):
            workspace = open_workspace(path, source, create=True, catalog=catalog)
        samples.append(observer.sample(RetainedCorpusValidator.open_retained, source, 0))
        # Subsequent actual SQL belongs to fixture setup, not this opening sample.
        workspace._db = workspace._db.db
        workspace._corpus._db = workspace._db
        with workspace._operation():
            insert(workspace._corpus, edges_for("chain", count), "derivation")
        workspace.close()
        observer = Observer()
        with observer.observe(monkeypatch, source):
            workspace = open_workspace(path, source, epoch=1, catalog=catalog)
        samples.append(observer.sample(RetainedCorpusValidator.open_retained, source, count))
        workspace._db = workspace._db.db
        workspace._corpus._db = workspace._db
        workspace.close()
        observer = Observer()
        with observer.observe(monkeypatch, source):
            with pytest.raises(ValueError, match="newer"):
                open_workspace(path, source, epoch=1, catalog=catalog)
        samples.append(observer.sample(RetainedCorpusValidator.open_retained, source, count))
        (path / "corpus.sqlite3").unlink()
        observer = Observer()
        with observer.observe(monkeypatch, source):
            with pytest.raises(StaleCorpusWorkspace, match="missing"):
                open_workspace(path, source, epoch=2, catalog=catalog)
        samples.append(observer.sample(RetainedCorpusValidator.open_retained, source, count))
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "native-retained-corpus-close-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_workspace.RetainedCorpusValidator.close",
)
def test_actual_close_releases_only_its_native_handle(
    tmp_path, catalog, monkeypatch, record_property
):
    samples = []
    for count in (0, 1, 17, 129, 1025):
        for failure in (None, "before", "lost-reply"):
            path = tmp_path / (str(count) + "-" + str(failure))
            path.mkdir()
            source = EmptySource(catalog)
            active = [Observer()]

            def release(observers=active):
                observers[0].metrics["release_callbacks"] += 1

            workspace = open_workspace(path, source, create=True, on_storage_close=release)
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
