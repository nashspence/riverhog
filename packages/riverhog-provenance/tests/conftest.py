from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from riverhog_provenance import BoundedSourceObserver, BytesSource, create_journal
from riverhog_provenance_contracts import ContractCatalog


@pytest.fixture(scope="session")
def catalog():
    return ContractCatalog()


@pytest.fixture
def observation(catalog):
    return BoundedSourceObserver(catalog=catalog).observe(
        BytesSource(b"opaque primary bytes\x00\xff")
    )


@pytest.fixture
def graph(observation):
    return copy.deepcopy(observation.graph_fragment())


@pytest.fixture
def who(observation):
    return observation.observer_agent_id


@pytest.fixture
def journal(observation, catalog):
    return create_journal(
        observation.graph_fragment(),
        recorded_by_agent_id=observation.observer_agent_id,
        catalog=catalog,
    )


@pytest.fixture
def parallel_snapshot_reads():
    def verify(summary):
        expected = summary.materialize()
        origins = dict(summary.assertion_entries)
        barrier = Barrier(8)

        def read(_):
            with summary.borrow() as reader:
                barrier.wait(timeout=30)
                for _ in range(8):
                    assert reader.materialize() == expected
                    assert dict(reader.assertion_entries) == origins
                    assert reader.anchor == expected["journal"]

        with ThreadPoolExecutor(max_workers=8) as readers:
            list(readers.map(read, range(8)))

    return verify
