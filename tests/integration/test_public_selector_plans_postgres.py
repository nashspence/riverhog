from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass
from uuid import uuid4

import pytest
from riverhog_api.app import create_app as create_riverhog_app
from riverhog_core.catalog_db import create_catalog_engine, initialize_db
from riverhog_core.services.collections import _collection_list_statement
from sqlalchemy import text
from sqlalchemy.engine import Engine, make_url
from stove0_core.persistence import stove0_state_schema

from scripts.operation_qualification import (
    create_adapter_contract_app,
    create_stove0_contract_app,
)
from tests.support.qualification.database_selector_plans import (
    DATABASE_FILTER_SELECTORS,
    DATABASE_PLAN_OPERATIONS,
    NON_PLAN_QUERY_OPERATIONS,
    PlanCase,
    catalog_sync_plan_cases,
    index_names,
    node_types,
    plan_cases,
    riverhog_plan_statement,
    seed_selector_relations,
    seed_stove0_selector_relations,
)

pytestmark = pytest.mark.integration
_ROWS = 16384


@dataclass(frozen=True)
class _QualifiedEngines:
    riverhog: Engine
    stove0: Engine


@pytest.fixture(scope="module")
def qualified_engines() -> Iterator[_QualifiedEngines]:
    value = os.getenv("RIVERHOG_TEST_POSTGRES_URL", "").strip()
    if not value:
        pytest.skip("RIVERHOG_TEST_POSTGRES_URL is required")
    suffix = uuid4().hex
    schemas = (f"riverhog_selector_plans_{suffix}", f"stove0_selector_plans_{suffix}")
    admin = create_catalog_engine(value)
    with admin.begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public"))
        for schema in schemas:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    riverhog_url = make_url(value).update_query_dict(
        {"options": f"-csearch_path={schemas[0]},public"}
    )
    stove0_url = make_url(value).update_query_dict(
        {"options": f"-csearch_path={schemas[1]},public"}
    )
    riverhog_database_url = riverhog_url.render_as_string(hide_password=False)
    stove0_database_url = stove0_url.render_as_string(hide_password=False)
    initialize_db(riverhog_database_url)
    stove0_state_schema(stove0_database_url).upgrade()
    riverhog_engine = create_catalog_engine(riverhog_database_url)
    stove0_engine = create_catalog_engine(stove0_database_url)
    seed_selector_relations(riverhog_engine, rows=_ROWS)
    seed_stove0_selector_relations(stove0_engine, rows=_ROWS)
    try:
        yield _QualifiedEngines(riverhog=riverhog_engine, stove0=stove0_engine)
    finally:
        riverhog_engine.dispose()
        stove0_engine.dispose()
        with admin.begin() as connection:
            for schema in schemas:
                connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def _openapi_operations(schema: dict[str, object]) -> dict[str, dict[str, object]]:
    paths = schema["paths"]
    assert isinstance(paths, dict)
    return {
        str(operation["operationId"]): operation
        for path_item in paths.values()
        if isinstance(path_item, dict)
        for method, operation in path_item.items()
        if method in {"delete", "get", "patch", "post", "put"}
        if isinstance(operation, dict)
    }


def _query_selectors(operation: dict[str, object]) -> set[str]:
    parameters = operation.get("parameters", [])
    assert isinstance(parameters, list)
    return {
        str(parameter["name"])
        for parameter in parameters
        if isinstance(parameter, dict) and parameter.get("in") == "query"
    }


def _query_enum(
    schema: dict[str, object],
    operation: dict[str, object],
    name: str,
) -> set[str]:
    parameters = operation.get("parameters", [])
    assert isinstance(parameters, list)
    parameter = next(
        item
        for item in parameters
        if isinstance(item, dict) and item.get("in") == "query" and item.get("name") == name
    )
    candidate = parameter["schema"]
    assert isinstance(candidate, dict)
    components = schema["components"]
    assert isinstance(components, dict)
    component_schemas = components["schemas"]
    assert isinstance(component_schemas, dict)

    def values(value: dict[str, object]) -> set[str]:
        reference = value.get("$ref")
        if reference is not None:
            resolved = component_schemas[str(reference).rsplit("/", 1)[-1]]
            assert isinstance(resolved, dict)
            return values(resolved)
        enum = value.get("enum")
        if isinstance(enum, list):
            return {str(item) for item in enum}
        variants = value.get("anyOf", [])
        assert isinstance(variants, list)
        return {
            item
            for variant in variants
            if isinstance(variant, dict) and variant.get("type") != "null"
            for item in values(variant)
        }

    return values(candidate)


def test_every_repo_query_selector_is_classified_and_every_database_selector_is_planned() -> None:
    schemas = {
        "riverhog": create_riverhog_app().openapi(),
        "stove0": create_stove0_contract_app().openapi(),
        "riverhog-ftp-adapter": create_adapter_contract_app().openapi(),
    }
    operations = {
        (application, operation_id): operation
        for application, schema in schemas.items()
        for operation_id, operation in _openapi_operations(schema).items()
    }
    observed = {
        key: _query_selectors(operation)
        for key, operation in operations.items()
        if _query_selectors(operation)
    }

    assert set(observed) == set(DATABASE_PLAN_OPERATIONS) | set(NON_PLAN_QUERY_OPERATIONS)
    for key, expected in NON_PLAN_QUERY_OPERATIONS.items():
        assert observed[key] == expected

    cases = plan_cases()
    case_ids = {case.id for case in cases}
    assert len(case_ids) == len(cases)
    expected_case_ids: set[str] = set()
    representatives: dict[str, tuple[str, str]] = {}
    filter_names = {"key": "key_id", "collection": "collection"}
    for key, prefix in DATABASE_PLAN_OPERATIONS.items():
        selectors = observed[key]
        expected_selectors = set(DATABASE_FILTER_SELECTORS[prefix])
        if "sort" in selectors or "order" in selectors:
            expected_selectors |= {"order", "sort"}
        if "page_size" in selectors or "page_token" in selectors:
            expected_selectors |= {"page_size", "page_token"}
        assert selectors == expected_selectors, key
        representatives.setdefault(prefix, key)

    for prefix, key in representatives.items():
        for selector in DATABASE_FILTER_SELECTORS[prefix]:
            expected_case_ids.add(f"{prefix}.filter.{filter_names.get(selector, selector)}")
        application, operation_id = key
        schema = schemas[application]
        operation = operations[(application, operation_id)]
        if "sort" in _query_selectors(operation):
            for sort in _query_enum(schema, operation, "sort"):
                for order in _query_enum(schema, operation, "order"):
                    expected_case_ids.add(f"{prefix}.sort.{sort}.{order}")

    assert case_ids == expected_case_ids


@pytest.mark.parametrize(
    "case",
    (*plan_cases(), *catalog_sync_plan_cases()),
    ids=lambda case: case.id,
)
def test_every_public_database_selector_has_its_declared_postgres_operator(
    qualified_engines: _QualifiedEngines,
    case: PlanCase,
) -> None:
    qualified_engine = getattr(qualified_engines, case.database)
    statement = case.statement  # type: ignore[assignment]
    if ".filter." in case.id:
        # Filter and order vocabularies are independent public selectors. Prove
        # the filter's physical operator without letting the representative
        # page order win a small-fixture cost tie.
        statement = statement.order_by(None)
    statement = statement.limit(100)
    compiled = statement.compile(
        dialect=qualified_engine.dialect,
        compile_kwargs={"literal_binds": True},
    )
    with qualified_engine.begin() as connection:
        # This exhaustive matrix proves that every public selector has a usable
        # physical operator. Natural planner choices and scale are qualified
        # separately at multiple cardinalities; low-cardinality fixtures must
        # not turn this structural proof into a cost-estimator coincidence.
        connection.exec_driver_sql("SET LOCAL enable_seqscan = off")
        if ".sort." in case.id:
            connection.exec_driver_sql("SET LOCAL enable_bitmapscan = off")
        elif ".filter." in case.id:
            connection.exec_driver_sql("SET LOCAL enable_indexscan = off")
        payload = connection.exec_driver_sql(
            f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {compiled}"
        ).scalar_one()
    indexes = index_names(payload)
    nodes = node_types(payload)

    if not (indexes & case.expected_indexes or nodes & case.expected_nodes):
        pytest.fail(
            f"{case.id}: expected one of indexes {sorted(case.expected_indexes)} or nodes "
            f"{sorted(case.expected_nodes)}; used indexes {sorted(indexes)} and nodes "
            f"{sorted(nodes)}"
        )


def test_collection_query_has_indexed_identity_and_description_projections(
    qualified_engines: _QualifiedEngines,
) -> None:
    statement = (
        riverhog_plan_statement(
            _collection_list_statement(
                q="16384",
                encryption_format=None,
                passphrase_id=None,
                tags=(),
                sort="id",
                order="asc",
                principal=None,
            ),
            order="asc",
        )
        .order_by(None)
        .limit(100)
    )
    compiled = statement.compile(
        dialect=qualified_engines.riverhog.dialect,
        compile_kwargs={"literal_binds": True},
    )
    with qualified_engines.riverhog.begin() as connection:
        connection.exec_driver_sql("SET LOCAL enable_seqscan = off")
        connection.exec_driver_sql("SET LOCAL enable_indexscan = off")
        payload = connection.exec_driver_sql(
            f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {compiled}"
        ).scalar_one()

    assert {
        "ix_collections_search_trgm",
        "ix_collections_description_search_trgm",
    } <= index_names(payload)
