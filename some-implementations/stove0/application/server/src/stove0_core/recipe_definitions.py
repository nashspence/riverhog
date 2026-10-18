"""Retain exact compiled policy independently of mutable installation config."""

from __future__ import annotations

from collections import OrderedDict
from threading import Lock

from sqlalchemy import Column, ForeignKey, Index, MetaData, String, Table, Text, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine
from stove0_protocol import OperationIdentityRef, RecipeIdentityRef, canonical_json_bytes
from stove0_recipe_config.compiled import CompiledRecipe
from stove0_recipe_config.compiler import verify_compiled_recipe
from stove0_recipe_config.dependencies import OperationResource, RecipeDependencyClosure
from stove0_target_protocol import OperationContract
from time_formats import utc_timestamp_now


def declare_recipe_definitions(metadata: MetaData) -> Table:
    return Table(
        "stove0_recipe_definitions",
        metadata,
        Column("recipe_sha256", String(64), primary_key=True),
        Column("recipe_json", Text, nullable=False),
        Column("closure_json", Text, nullable=False),
        Column("updated_at", Text, nullable=False),
        Index("ix_stove0_recipe_definition_updated", "updated_at", "recipe_sha256"),
    )


def declare_retained_recipe_dependencies(metadata: MetaData, definitions: Table) -> Table:
    return Table(
        "stove0_retained_recipe_dependencies",
        metadata,
        Column(
            "recipe_sha256",
            String(64),
            ForeignKey(definitions.c.recipe_sha256, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("dependency_sha256", String(64), primary_key=True),
        Index("ix_stove0_retained_recipe_dependency", "dependency_sha256", "recipe_sha256"),
    )


def declare_retained_operations(metadata: MetaData, definitions: Table) -> Table:
    return Table(
        "stove0_retained_recipe_operations",
        metadata,
        Column(
            "recipe_sha256",
            String(64),
            ForeignKey(definitions.c.recipe_sha256, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("operation_sha256", String(64), primary_key=True),
        Column("operation_id", Text, nullable=False),
        Column("document_json", Text, nullable=False),
        Index(
            "ix_stove0_retained_operation_identity",
            "operation_id",
            "operation_sha256",
            "recipe_sha256",
        ),
    )


def exact_closure(
    recipe: CompiledRecipe, closure: RecipeDependencyClosure
) -> RecipeDependencyClosure:
    wanted = {(ref.kind, ref.id, ref.sha256) for ref in recipe.dependencies}
    return RecipeDependencyClosure(
        observers=tuple(
            resource
            for resource in closure.observers
            if ("observation-interface", resource.interface.id, resource.interface.interface_sha256)
            in wanted
        ),
        operations=tuple(
            resource
            for resource in closure.operations
            if ("operation-contract", resource.contract.id, resource.contract.contract_sha256)
            in wanted
        ),
        recipes=tuple(
            child
            for child in closure.recipes
            if ("compiled-recipe", child.id, child.sha256) in wanted
        ),
    )


class RetainedRecipeStore:
    def __init__(
        self, engine: Engine, table: Table, operations: Table, dependencies: Table
    ) -> None:
        self.engine, self.table, self.operations = engine, table, operations
        self.dependencies = dependencies
        self._validated: OrderedDict[
            tuple[str, str], tuple[CompiledRecipe, RecipeDependencyClosure]
        ] = OrderedDict()
        self._validated_lock = Lock()
        self._validated_bytes = 0
        self._cache_bytes = 8 * 1024 * 1024
        self._cache_entries = 32

    def retain_tree(self, recipe: CompiledRecipe, closure: RecipeDependencyClosure) -> None:
        exact = exact_closure(recipe, closure)
        self.retain(recipe, closure)
        for child in exact.recipes:
            self.retain(child, closure)

    def retain(self, recipe: CompiledRecipe, closure: RecipeDependencyClosure) -> None:
        closure = exact_closure(recipe, closure)
        verify_compiled_recipe(recipe, closure)
        recipe_json = canonical_json_bytes(recipe.model_dump(mode="json", by_alias=True)).decode(
            "utf-8"
        )
        closure_json = canonical_json_bytes(closure.model_dump(mode="json", by_alias=True)).decode(
            "utf-8"
        )
        t = self.table
        insert = pg_insert if self.engine.dialect.name == "postgresql" else sqlite_insert
        with self.engine.begin() as connection:
            connection.execute(
                insert(t)
                .values(
                    recipe_sha256=recipe.sha256,
                    recipe_json=recipe_json,
                    closure_json=closure_json,
                    updated_at=utc_timestamp_now(),
                )
                .on_conflict_do_update(
                    index_elements=[t.c.recipe_sha256],
                    set_={"updated_at": utc_timestamp_now()},
                )
            )
            row = (
                connection.execute(select(t).where(t.c.recipe_sha256 == recipe.sha256))
                .mappings()
                .one()
            )
            if (row["recipe_json"], row["closure_json"]) != (recipe_json, closure_json):
                raise ValueError(
                    "retained compiled definition or exact dependency closure was rebound"
                )
            for child in closure.recipes:
                connection.execute(
                    insert(self.dependencies)
                    .values(
                        recipe_sha256=recipe.sha256,
                        dependency_sha256=child.sha256,
                    )
                    .on_conflict_do_nothing()
                )
            o = self.operations
            for resource in closure.operations:
                document = canonical_json_bytes(
                    resource.model_dump(mode="json", by_alias=True)
                ).decode("utf-8")
                key = (
                    o.c.recipe_sha256 == recipe.sha256,
                    o.c.operation_sha256 == resource.contract.contract_sha256,
                )
                connection.execute(
                    insert(o)
                    .values(
                        recipe_sha256=recipe.sha256,
                        operation_sha256=resource.contract.contract_sha256,
                        operation_id=resource.contract.id,
                        document_json=document,
                    )
                    .on_conflict_do_nothing()
                )
                if connection.scalar(select(o.c.document_json).where(*key)) != document:
                    raise ValueError("retained exact operation dependency was rebound")

    def operation(self, reference: OperationIdentityRef) -> OperationContract:
        o = self.operations
        with self.engine.connect() as connection:
            document = connection.scalar(
                select(o.c.document_json)
                .where(
                    o.c.operation_id == reference.id,
                    o.c.operation_sha256 == reference.sha256,
                )
                .order_by(o.c.recipe_sha256)
                .limit(1)
            )
        if document is None:
            raise ValueError("exact operation is unavailable in retained recipe closures")
        contract = OperationResource.model_validate_json(document).contract
        if (contract.id, contract.contract_sha256) != (reference.id, reference.sha256):
            raise ValueError("retained operation differs from its exact bound identity")
        return contract

    def load(
        self, reference: RecipeIdentityRef
    ) -> tuple[CompiledRecipe, RecipeDependencyClosure] | None:
        t = self.table
        with self.engine.connect() as connection:
            row = (
                connection.execute(select(t).where(t.c.recipe_sha256 == reference.sha256))
                .mappings()
                .first()
            )
        if row is None:
            return None
        # Query the current row every time. Exact stored preimages qualify this
        # disposable validation cache; a changed row cannot borrow old authority.
        key = (row["recipe_json"], row["closure_json"])
        with self._validated_lock:
            retained = self._validated.get(key)
            if retained is not None:
                self._validated.move_to_end(key)
        if retained is None:
            recipe = CompiledRecipe.model_validate_json(key[0])
            closure = RecipeDependencyClosure.model_validate_json(key[1])
            if recipe.ref != reference:
                raise ValueError("retained recipe differs from the queued work's exact identity")
            verify_compiled_recipe(recipe, closure)
            retained = (recipe, closure)
            size = sum(len(document.encode("utf-8")) for document in key)
            # A larger valid definition is validated normally, never rejected.
            # Compute outside the lock so independent owners remain responsive.
            if size <= self._cache_bytes and self._cache_entries > 0:
                with self._validated_lock:
                    if key not in self._validated:
                        self._validated[key] = retained
                        self._validated_bytes += size
                    while (
                        self._validated_bytes > self._cache_bytes
                        or len(self._validated) > self._cache_entries
                    ):
                        old_key, _ = self._validated.popitem(last=False)
                        self._validated_bytes -= sum(
                            len(document.encode("utf-8")) for document in old_key
                        )
            else:
                return retained
        recipe, closure = retained
        if recipe.ref != reference:
            raise ValueError("retained recipe differs from the queued work's exact identity")
        # Frozen documents contain JSON mappings. Do not expose cached mappings
        # to callers that might accidentally mutate their own returned copy.
        return recipe.model_copy(deep=True), closure.model_copy(deep=True)
