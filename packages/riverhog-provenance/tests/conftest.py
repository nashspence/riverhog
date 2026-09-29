from __future__ import annotations

import copy

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
