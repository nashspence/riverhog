from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace

import httpx
import pytest
from a_riverhog_ftp_spool.app import FtpSpoolComposition, create_app
from a_riverhog_ftp_spool.config import FtpSpoolConfig, SourceConfig
from a_riverhog_ftp_spool.landing import FtpSpool
from a_riverhog_ftp_spool.operational_state import FtpOperationalState
from a_riverhog_ftp_spool_client import RiverhogFtpSpoolClient
from fastapi.testclient import TestClient

_CONNECT = sqlite3.connect


class Probe:
    """Observe real SQLite and retain credit until cleanup is acknowledged."""

    def __init__(self):
        self.active = self.peak = self.attempts = 0
        self.cursor_active = self.cursor_peak = 0
        self.connections = []
        self.cursors = []
        self.owners = []
        self.statements = []
        self.fail_statement = None
        self.fail_fetch = False
        self.fail_commit = None
        self.fail_cursor_close = None
        self.unreturned = False
        self.refuse = False

    def retain(self, owner):
        self.owners.append(owner)

    def __call__(self, path):
        self.attempts += 1
        if self.refuse:
            raise RuntimeError("connection admission refused")
        probe = self

        class Cursor(sqlite3.Cursor):
            released = False
            close_calls = 0

            def execute(self, statement, parameters=()):
                probe.statements.append(statement)
                if probe.fail_statement and statement.lstrip().startswith(probe.fail_statement):
                    raise InterruptedError("statement interrupted")
                return super().execute(statement, parameters)

            def executescript(self, statement):
                probe.statements.append(statement)
                if probe.fail_statement and statement.lstrip().startswith(probe.fail_statement):
                    raise InterruptedError("script interrupted")
                return super().executescript(statement)

            def fetchone(self):
                if probe.fail_fetch:
                    raise InterruptedError("fetch interrupted")
                return super().fetchone()

            def fetchall(self):
                if probe.fail_fetch:
                    raise InterruptedError("fetch interrupted")
                return super().fetchall()

            def close(self):
                self.close_calls += 1
                if probe.fail_cursor_close is not None:
                    after = probe.fail_cursor_close
                    probe.fail_cursor_close = None
                    if after:
                        super().close()
                    raise OSError("cursor close unconfirmed")
                super().close()
                if not self.released:
                    self.released = True
                    probe.cursor_active -= 1

        class Connection(sqlite3.Connection):
            released = False
            close_failure = None
            close_calls = 0

            def _cursor(self):
                cursor = self.cursor(factory=Cursor)
                probe.cursors.append(cursor)
                probe.cursor_active += 1
                probe.cursor_peak = max(probe.cursor_peak, probe.cursor_active)
                return cursor

            def execute(self, statement, parameters=()):
                assert any(owner._db is self for owner in probe.owners), (
                    "SQL ran before actual-owner retention"
                )
                cursor = self._cursor()
                try:
                    return cursor.execute(statement, parameters)
                except BaseException:
                    # A factory owns any cursor it does not return.
                    sqlite3.Cursor.close(cursor)
                    cursor.released = True
                    probe.cursor_active -= 1
                    raise

            def executescript(self, statement):
                assert any(owner._db is self for owner in probe.owners)
                cursor = self._cursor()
                try:
                    return cursor.executescript(statement)
                except BaseException:
                    sqlite3.Cursor.close(cursor)
                    cursor.released = True
                    probe.cursor_active -= 1
                    raise

            def commit(self):
                if probe.fail_commit is not None:
                    after = probe.fail_commit
                    probe.fail_commit = None
                    if after:
                        super().commit()
                    raise InterruptedError("commit acknowledgement interrupted")
                super().commit()

            def close(self):
                self.close_calls += 1
                if self.close_failure is not None:
                    after = self.close_failure
                    self.close_failure = None
                    if after:
                        super().close()
                    raise OSError("connection close unconfirmed")
                super().close()
                if not self.released:
                    self.released = True
                    probe.active -= 1

        db = _CONNECT(path, factory=Connection)
        self.connections.append(db)
        self.active += 1
        self.peak = max(self.peak, self.active)
        if self.unreturned:
            raise OSError("connection construction unconfirmed")
        return db


def opened(path, probe):
    return FtpOperationalState(path, connection_factory=probe, on_storage_open=probe.retain)


def config(tmp_path):
    return FtpSpoolConfig(
        primary_state={"scope": {"service": "fixture-service", "account": "fixture-account"}},
        host_id="urn:uuid:00000000-0000-4000-8000-000000000001",
        riverhog_base_url="https://riverhog.invalid",
        riverhog_token="fixture-token",
        api_token="fixture-token",
        provenance_observer="a-riverhog-linux-provenance-observer",
        sources=(SourceConfig(id="intake", root=tmp_path / "intake", ingest_source="ftp:intake"),),
    )


def test_state_admission_precedes_settings_and_preserves_committed_custody(tmp_path):
    path = tmp_path / "state.sqlite3"
    probe = Probe()
    with opened(path, probe) as state:
        assert probe.owners == [state]
        assert probe.statements == ["PRAGMA foreign_keys = ON", "PRAGMA synchronous = FULL"]
        assert state.one("PRAGMA foreign_keys") == (1,)
        assert state.one("PRAGMA synchronous") == (2,)
        state.executescript("CREATE TABLE custody (id INTEGER PRIMARY KEY, payload BLOB);")
        written = state.execute("INSERT INTO custody(payload) VALUES (?)", (b"original",))
        assert (written.rowcount, written.lastrowid) == (1, 1)
        state.commit()
        state.execute("INSERT INTO custody(payload) VALUES (?)", (b"uncommitted",))
        assert state.one("SELECT payload FROM custody WHERE id = ?", (1,)) == (b"original",)
        assert probe.cursor_active == 0
    assert state.cleanup_complete and path.is_file()
    with opened(path, probe) as restarted:
        assert restarted.all("SELECT payload FROM custody ORDER BY id LIMIT 10") == [(b"original",)]
    assert probe.active == probe.cursor_active == 0
    assert probe.peak == probe.cursor_peak == 1


@pytest.mark.parametrize("count", [1, 129, 385])
def test_persistent_state_pages_close_cursors_with_linear_work(tmp_path, count):
    path = tmp_path / "state.sqlite3"
    probe = Probe()
    with opened(path, probe) as state:
        state.executescript("CREATE TABLE custody (id INTEGER PRIMARY KEY, payload BLOB);")
        for ordinal in range(1, count + 1):
            state.execute("INSERT INTO custody VALUES (?, ?)", (ordinal, str(ordinal).encode()))
        state.commit()
        after = 0
        seen = []
        while after < count:
            page = state.all(
                "SELECT id, payload FROM custody WHERE id > ? ORDER BY id LIMIT ?", (after, 32)
            )
            assert 1 <= len(page) <= 32
            assert probe.cursor_active == 0
            seen.extend(page)
            after = page[-1][0]
        assert seen == [(i, str(i).encode()) for i in range(1, count + 1)]
    assert sum(s.startswith("INSERT") for s in probe.statements) == count
    assert sum(s.startswith("SELECT") for s in probe.statements) == (count + 31) // 32
    assert probe.cursor_peak == probe.peak == 1
    assert probe.cursor_active == probe.active == 0


@pytest.mark.parametrize("unreturned", [False, True])
def test_factory_refusal_retains_persistent_files_and_unreturned_connection(tmp_path, unreturned):
    path = tmp_path / "state.sqlite3"
    path.write_bytes(b"preexisting-state")
    marker = tmp_path / "custody-marker"
    marker.write_bytes(b"original")
    probe = Probe()
    probe.unreturned = unreturned
    probe.refuse = not unreturned
    with pytest.raises((OSError, RuntimeError)):
        opened(path, probe)
    assert not probe.owners and not probe.statements
    assert path.read_bytes() == b"preexisting-state"
    assert marker.read_bytes() == b"original"
    assert probe.active == int(unreturned)
    if unreturned:
        probe.connections[0].close()
    assert probe.active == 0


def test_default_connect_failure_does_not_remove_persistent_files(monkeypatch, tmp_path):
    path = tmp_path / "state.sqlite3"
    path.write_bytes(b"original-state")
    sidecar = path.with_suffix(".sqlite3-journal")
    sidecar.write_bytes(b"original-journal")

    def refuse(_path):
        raise OSError("open refused")

    monkeypatch.setattr(sqlite3, "connect", refuse)
    with pytest.raises(OSError, match="open refused"):
        FtpOperationalState(path)
    assert path.read_bytes() == b"original-state"
    assert sidecar.read_bytes() == b"original-journal"


@pytest.mark.parametrize("stage", ["retention", "settings"])
@pytest.mark.parametrize("after", [None, False, True])
def test_failed_initialization_retains_actual_owner_until_confirmed_close(tmp_path, stage, after):
    path = tmp_path / "state.sqlite3"
    probe = Probe()

    def retain(state):
        probe.retain(state)
        probe.connections[0].close_failure = after
        if stage == "retention":
            raise InterruptedError("retention interrupted")
        probe.fail_statement = "PRAGMA foreign_keys"

    with pytest.raises((InterruptedError, OSError)):
        FtpOperationalState(path, connection_factory=probe, on_storage_open=retain)
    state = probe.owners[0]
    assert state.closing and path.exists()
    assert state.cleanup_complete == (after is None)
    assert probe.active == int(after is not None)
    assert probe.connections[0].close_calls == 1
    before = list(probe.statements)
    for work in [
        lambda: state.one("SELECT 1"),
        lambda: state.all("SELECT 1"),
        lambda: state.execute("SELECT 1"),
        lambda: state.executescript("SELECT 1;"),
        state.commit,
        state.__enter__,
    ]:
        with pytest.raises(RuntimeError):
            work()
    assert probe.statements == before
    state.close()
    assert state.cleanup_complete and probe.active == 0
    assert probe.connections[0].close_calls == 1 + int(after is not None)


@pytest.mark.parametrize("after", [False, True])
def test_retention_explicit_unknown_close_has_no_implicit_retry(tmp_path, after):
    probe = Probe()

    def retain(state):
        probe.retain(state)
        probe.connections[0].close_failure = after
        state.close()

    with pytest.raises(OSError):
        FtpOperationalState(
            tmp_path / "state.sqlite3", connection_factory=probe, on_storage_open=retain
        )
    assert not probe.statements
    assert probe.connections[0].close_calls == 1 and probe.active == 1
    probe.owners[0].close()
    assert probe.active == 0


def test_retention_cannot_return_closed_owner(tmp_path):
    probe = Probe()

    def retain(state):
        probe.retain(state)
        state.close()

    with pytest.raises(RuntimeError, match="closed"):
        FtpOperationalState(
            tmp_path / "state.sqlite3", connection_factory=probe, on_storage_open=retain
        )
    assert not probe.statements and probe.active == 0
    assert probe.connections[0].close_calls == 1


@pytest.mark.parametrize("after", [False, True])
@pytest.mark.parametrize("operation", ["settings", "one", "all", "write", "script"])
def test_uncertain_cursor_close_freezes_work_and_precedes_connection_close(
    tmp_path, after, operation
):
    probe = Probe()
    if operation == "settings":
        probe.fail_cursor_close = after
        with pytest.raises(OSError):
            opened(tmp_path / "state.sqlite3", probe)
        state = probe.owners[0]
    else:
        state = opened(tmp_path / "state.sqlite3", probe)
        probe.fail_cursor_close = after
        work = {
            "one": lambda: state.one("SELECT 1"),
            "all": lambda: state.all("SELECT 1"),
            "write": lambda: state.execute("CREATE TABLE custody (id INTEGER)"),
            "script": lambda: state.executescript("CREATE TABLE custody (id INTEGER);"),
        }[operation]
        with pytest.raises(OSError):
            with state:
                work()
    assert state.closing and not state.cleanup_complete
    assert probe.cursor_active == probe.active == 1
    assert probe.connections[0].close_calls == 0
    cursor = probe.cursors[-1]
    assert cursor.close_calls == 1
    before = list(probe.statements)
    with pytest.raises(RuntimeError):
        state.commit()
    with pytest.raises(RuntimeError):
        state.one("SELECT 2")
    assert probe.statements == before
    state.close()
    assert cursor.close_calls == 2
    assert probe.active == probe.cursor_active == 0


@pytest.mark.parametrize("after", [False, True])
def test_context_does_not_retry_explicit_uncertain_connection_close(tmp_path, after):
    probe = Probe()
    state = opened(tmp_path / "state.sqlite3", probe)
    probe.connections[0].close_failure = after
    with pytest.raises(OSError):
        with state:
            state.close()
    assert probe.connections[0].close_calls == 1 and probe.active == 1
    state.close()
    state.close()
    assert state.cleanup_complete and probe.active == 0
    assert probe.connections[0].close_calls == 2


@pytest.mark.parametrize("mode", ["one", "all"])
def test_interrupted_fetch_cleans_returned_cursor_and_preserves_persistent_state(tmp_path, mode):
    probe = Probe()
    path = tmp_path / "state.sqlite3"
    with pytest.raises(InterruptedError):
        with opened(path, probe) as state:
            probe.fail_fetch = True
            getattr(state, mode)("SELECT 1")
    assert path.is_file() and state.cleanup_complete
    assert probe.active == probe.cursor_active == 0


@pytest.mark.parametrize("after", [False, True])
def test_commit_interruption_preserves_actual_durable_outcome(tmp_path, after):
    probe = Probe()
    path = tmp_path / "state.sqlite3"
    with pytest.raises(InterruptedError):
        with opened(path, probe) as state:
            state.executescript("CREATE TABLE custody (id INTEGER PRIMARY KEY);")
            state.execute("INSERT INTO custody VALUES (7)")
            probe.fail_commit = after
            state.commit()
    assert probe.active == 0 and state.cleanup_complete
    with opened(path, probe) as restarted:
        assert restarted.all("SELECT id FROM custody LIMIT 2") == ([(7,)] if after else [])


def test_ftp_spool_forwards_each_actual_owner_and_preserves_restart_state(tmp_path):
    probe = Probe()
    cfg = config(tmp_path)
    adapter = FtpSpool(
        object(), cfg, state_connection_factory=probe, on_state_storage_open=probe.retain
    )
    source = cfg.sources[0]
    assert len(probe.owners) == 2
    assert not adapter.retained_state_storage
    with adapter._open_state(source) as state:
        assert adapter.retained_state_storage == (state,)
        assert state.one("PRAGMA user_version") == (3,)
        generation = state.one(
            "SELECT value FROM adapter_state WHERE key = 'completion_generation'"
        )
    assert not adapter.retained_state_storage
    adapter._save_reconcile_cursor(source, 17)
    restarted = FtpSpool(
        object(), cfg, state_connection_factory=probe, on_state_storage_open=probe.retain
    )
    with restarted._open_state(source) as state:
        assert state.one("SELECT value FROM adapter_state WHERE key = 'reconcile_after'") == ("17",)
        assert (
            state.one("SELECT value FROM adapter_state WHERE key = 'completion_generation'")
            == generation
        )
    assert probe.active == probe.cursor_active == 0 and probe.peak == probe.cursor_peak == 1


@pytest.mark.parametrize("after", [False, True])
def test_spool_retains_failed_owner_and_blocks_new_connections_until_explicit_cleanup(
    tmp_path, after
):
    probe = Probe()
    cfg = config(tmp_path)
    adapter = FtpSpool(
        object(), cfg, state_connection_factory=probe, on_state_storage_open=probe.retain
    )
    source = cfg.sources[0]
    with pytest.raises(OSError):
        with adapter._open_state(source) as state:
            probe.connections[-1].close_failure = after
    assert adapter.retained_state_storage == (state,)
    assert state.cleanup_unconfirmed
    attempts = probe.attempts
    for _ in range(3):
        with pytest.raises(RuntimeError, match="cleanup is unconfirmed"):
            adapter._claim_summary(source)
    assert probe.attempts == attempts and probe.active == 1
    state.close()
    assert not adapter.retained_state_storage
    assert adapter._claim_summary(source) == (0, 0)
    assert probe.active == 0


@pytest.mark.parametrize("after", [False, True])
def test_spool_constructor_schema_failure_retains_actual_state_without_erasing_custody(
    tmp_path, after
):
    probe = Probe()
    cfg = config(tmp_path)
    adapter = FtpSpool(object(), cfg)
    source = cfg.sources[0]
    adapter._save_reconcile_cursor(source, 29)

    def retain(state):
        probe.retain(state)
        probe.fail_statement = "CREATE TABLE"
        probe.connections[-1].close_failure = after

    with pytest.raises(OSError):
        FtpSpool(object(), cfg, state_connection_factory=probe, on_state_storage_open=retain)
    state = probe.owners[-1]
    assert probe.active == 1 and not state.cleanup_complete
    state.close()
    assert probe.active == 0
    with adapter._open_state(source) as restarted:
        assert restarted.one("PRAGMA user_version") == (3,)
        assert restarted.one("SELECT value FROM adapter_state WHERE key = 'reconcile_after'") == (
            "29",
        )


def test_composition_forwards_state_admission_before_source_initialization(monkeypatch, tmp_path):
    from a_riverhog_ftp_spool import app

    observer = object()
    monkeypatch.setattr(app, "resolve_provenance_observer", lambda _selection: observer)
    probe = Probe()
    composition = app.FtpSpoolComposition.build(
        config(tmp_path),
        state_connection_factory=probe,
        on_state_storage_open=probe.retain,
    )
    try:
        assert len(probe.owners) == 2
        assert composition.adapter._provenance_observer is observer
        assert not composition.adapter.retained_state_storage
        assert probe.active == probe.cursor_active == 0
    finally:
        composition.api.close()


def test_spool_allows_independent_active_connections_without_hidden_population_cap(tmp_path):
    probe = Probe()
    cfg = config(tmp_path)
    adapter = FtpSpool(
        object(), cfg, state_connection_factory=probe, on_state_storage_open=probe.retain
    )
    with adapter._open_state(cfg.sources[0]) as first:
        with adapter._open_state(cfg.sources[0]) as second:
            assert first is not second
            assert set(adapter.retained_state_storage) == {first, second}
            assert first.one("SELECT 1") == second.one("SELECT 1") == (1,)
            assert probe.active == 2
        assert adapter.retained_state_storage == (first,)
    assert not adapter.retained_state_storage and probe.active == 0
    assert probe.peak == 2


@pytest.mark.parametrize("after", [False, True])
def test_spool_cursor_failure_stays_with_adapter_until_explicit_cleanup(tmp_path, after):
    probe = Probe()
    cfg = config(tmp_path)
    adapter = FtpSpool(
        object(), cfg, state_connection_factory=probe, on_state_storage_open=probe.retain
    )
    probe.fail_cursor_close = after
    with pytest.raises(OSError):
        adapter._claim_summary(cfg.sources[0])
    (state,) = adapter.retained_state_storage
    assert probe.connections[-1].close_calls == 0
    assert state.cleanup_unconfirmed
    attempts = probe.attempts
    with pytest.raises(RuntimeError, match="cleanup is unconfirmed"):
        adapter._completion_cursor(cfg.sources[0])
    assert probe.attempts == attempts
    state.close()
    assert adapter._claim_summary(cfg.sources[0]) == (0, 0)
    assert probe.active == probe.cursor_active == 0


@pytest.mark.parametrize("after", [False, True])
def test_spool_commit_acknowledgement_loss_preserves_exact_resume_cursor(tmp_path, after):
    probe = Probe()
    cfg = config(tmp_path)
    adapter = FtpSpool(
        object(), cfg, state_connection_factory=probe, on_state_storage_open=probe.retain
    )
    source = cfg.sources[0]
    adapter._save_reconcile_cursor(source, 6)
    probe.fail_commit = after
    with pytest.raises(InterruptedError):
        adapter._save_reconcile_cursor(source, 7)
    assert not adapter.retained_state_storage and probe.active == 0
    with adapter._open_state(source) as state:
        assert state.one("SELECT value FROM adapter_state WHERE key = 'reconcile_after'") == (
            "7" if after else "6",
        )


@pytest.mark.parametrize("boundary", ["cursor", "connection"])
def test_http_status_remains_available_during_another_normal_state_close(
    tmp_path, monkeypatch, boundary
):
    probe = Probe()
    cfg = config(tmp_path)
    adapter = FtpSpool(
        object(), cfg, state_connection_factory=probe, on_state_storage_open=probe.retain
    )
    entered, release = Event(), Event()
    retained = []

    def finish_state():
        with adapter._open_state(cfg.sources[0]) as state:
            retained.append(state)
            actual = probe.connections[-1] if boundary == "connection" else probe.cursors[-1]
            original = type(actual).close

            def waiting(resource):
                entered.set()
                if not release.wait(5):
                    raise RuntimeError("test did not release actual state cleanup")
                return original(resource)

            monkeypatch.setattr(type(actual), "close", waiting)
            if boundary == "cursor":
                assert state.one("SELECT 41") == (41,)

    composition = FtpSpoolComposition(cfg, SimpleNamespace(close=lambda: None), adapter)
    http = TestClient(create_app(composition), raise_server_exceptions=False)

    def forward(request):
        response = http.request(request.method, request.url.path, headers=request.headers)
        return httpx.Response(
            response.status_code,
            headers=response.headers,
            content=response.content,
            request=request,
        )

    try:
        with RiverhogFtpSpoolClient(
            base_url="https://ftp-spool.invalid",
            token=cfg.api_token,
            transport=httpx.MockTransport(forward),
        ) as client:
            with ThreadPoolExecutor(max_workers=1) as worker:
                result = worker.submit(finish_state)
                assert entered.wait(5)
                try:
                    assert adapter.retained_state_storage == (retained[0],)
                    assert not retained[0].cleanup_unconfirmed
                    assert retained[0].closing is (boundary == "connection")
                    status = client.get_ftp_spool_status()
                    assert len(status.sources) == 1
                    assert status.sources[0].claims == 0
                    assert type(status.sources[0].claims) is int
                    assert adapter.retained_state_storage == (retained[0],)
                    assert probe.active == 1
                finally:
                    release.set()
                result.result(timeout=5)
        assert not adapter.retained_state_storage
        assert probe.active == probe.cursor_active == 0
        assert adapter._state_path(cfg.sources[0]).is_file()
    finally:
        release.set()
        http.close()


@pytest.mark.parametrize("after", [False, True])
def test_failed_state_cleanup_stays_blocking_during_an_explicit_retry(tmp_path, monkeypatch, after):
    probe = Probe()
    cfg = config(tmp_path)
    adapter = FtpSpool(
        object(), cfg, state_connection_factory=probe, on_state_storage_open=probe.retain
    )
    source = cfg.sources[0]
    adapter._save_reconcile_cursor(source, 31)
    entered, release = Event(), Event()

    def fail_close():
        with pytest.raises(OSError, match="close unconfirmed"):
            with adapter._open_state(source) as state:
                assert state.one(
                    "SELECT value FROM adapter_state WHERE key = 'reconcile_after'"
                ) == ("31",)
                probe.connections[-1].close_failure = after
        return state, probe.connections[-1]

    with ThreadPoolExecutor(max_workers=1) as worker:
        state, connection = worker.submit(fail_close).result(timeout=5)
        assert state.cleanup_unconfirmed and not state.cleanup_complete
        original = type(connection).close

        def waiting(resource):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("test did not release explicit cleanup")
            return original(resource)

        monkeypatch.setattr(type(connection), "close", waiting)
        result = worker.submit(state.close)
        assert entered.wait(5)
        try:
            attempts = probe.attempts
            assert state.cleanup_unconfirmed
            with pytest.raises(RuntimeError, match="cleanup is unconfirmed"):
                adapter._claim_summary(source)
            assert probe.attempts == attempts
            assert adapter.retained_state_storage == (state,)
        finally:
            release.set()
        result.result(timeout=5)
    assert state.cleanup_complete and not state.cleanup_unconfirmed
    with adapter._open_state(source) as restarted:
        assert restarted.one("SELECT value FROM adapter_state WHERE key = 'reconcile_after'") == (
            "31",
        )
        assert restarted.one("PRAGMA user_version") == (3,)
    assert not adapter.retained_state_storage and probe.active == probe.cursor_active == 0
