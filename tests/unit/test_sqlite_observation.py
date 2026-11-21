import sqlite3
from contextlib import closing

import pytest
from sqlite_admission import SqliteProgressConnection, observe_sqlite_execution

from tests.support.sqlite_observation import connect_observed


@pytest.mark.parametrize("owned", [False, True])
def test_observation_keeps_the_original_factory_and_actual_vm_callbacks(owned):
    constructed = []
    instructions = 0

    def tick():
        nonlocal instructions
        instructions += 1
        return 0

    class Original(SqliteProgressConnection if owned else sqlite3.Connection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            constructed.append(self)

    class Observation(sqlite3.Connection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.set_progress_handler(tick, 1)

    with closing(
        connect_observed(
            sqlite3.connect, Observation, ":memory:", factory=Original, cached_statements=0
        )
    ) as db:
        assert constructed == [db] and isinstance(db, Original) and isinstance(db, Observation)
        query = (
            "WITH RECURSIVE work(n) AS (VALUES(0) UNION ALL "
            "SELECT n+1 FROM work WHERE n<4096) SELECT sum(n) FROM work"
        )

        def result():
            with closing(db.execute(query)) as rows:
                return rows.fetchone()

        assert result() == (4096 * 4097 // 2,)
        count = instructions
        assert count > 1024
        if owned:
            instructions = 0
            checks = []
            with observe_sqlite_execution(db, lambda: checks.append(None)):
                assert result() == (4096 * 4097 // 2,)
            assert instructions == count and len(checks) > 1


def test_observation_without_an_explicit_factory_uses_one_original_connection():
    calls = []

    class Observation(sqlite3.Connection):
        pass

    def driver(*args, **kwargs):
        db = sqlite3.connect(*args, **kwargs)
        calls.append(db)
        return db

    with closing(connect_observed(driver, Observation, ":memory:")) as db:
        assert calls == [db] and isinstance(db, Observation)
        with closing(db.execute("SELECT 1")) as rows:
            assert rows.fetchone() == (1,)
