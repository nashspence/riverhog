"""Resumable inclusive branch selection over the exact original invocation."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any, Literal, Self

from pydantic import JsonValue, model_validator
from riverhog_protocol.exact_scalar import NonnegativeDecimal
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    ForeignKey,
    Index,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    and_,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import Insert as PgInsert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import Insert as SqliteInsert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import ColumnElement
from stove0_protocol import (
    ArtifactSelectionRef,
    RecipeIdentityRef,
    WorkIdentity,
    canonical_json_bytes,
    canonical_json_sha256,
)
from stove0_protocol.compiled_evidence import CompiledFactProof
from stove0_protocol.input_groups import InputGroupSet
from stove0_protocol.models import Sha256, Stove0ProtocolModel
from stove0_protocol.predicates import (
    FactsQuantification,
    LocalName,
    Predicate,
    Truth,
    evaluate_predicate,
    facts_predicates,
)
from stove0_recipe_config.compiled import CompiledRoleSelection
from stove0_recipe_config.source import GroupSelection

from stove0_core._checkpoint_sha256 import CheckpointSHA256
from stove0_core.compiled_fact_evaluation import CompiledFactEvaluation
from stove0_core.compiled_input_scopes import CompiledInputScopes
from stove0_core.compiled_state_ports import CompiledStatePort
from stove0_core.work_state import ConcurrentWorkUpdate, WorkInapplicable


def _predicate_key(predicate: FactsQuantification) -> str:
    return canonical_json_sha256(predicate.model_dump(mode="json", by_alias=True))


class CompiledBranchConditionPayload(Stove0ProtocolModel):
    format: Literal["stove0-compiled-branch-condition/v1"] = "stove0-compiled-branch-condition/v1"
    work_id: Sha256
    recipe: RecipeIdentityRef
    branch_id: LocalName
    inventory: ArtifactSelectionRef
    candidate: ArtifactSelectionRef | None
    condition: Predicate
    evaluations: tuple[CompiledFactProof, ...]
    truth: Truth

    @model_validator(mode="after")
    def exact_evaluation(self) -> Self:
        keys = [_predicate_key(proof.binding.predicate) for proof in self.evaluations]
        required = {_predicate_key(predicate) for predicate in facts_predicates(self.condition)}
        if keys != sorted(required):
            raise ValueError("branch condition omits, duplicates or changes required predicates")
        answers = {}
        for key, proof in zip(keys, self.evaluations, strict=True):
            binding = proof.binding
            expected = self.inventory if binding.predicate.scope == "input" else self.candidate
            if expected is None or (
                binding.work_id,
                binding.recipe,
                binding.inventory,
                binding.scope,
            ) != (
                self.work_id,
                self.recipe,
                self.inventory,
                expected,
            ):
                raise ValueError("branch condition changed its exact invocation/candidate scope")
            answers[key] = proof.truth
        if self.truth != evaluate_predicate(
            self.condition, lambda predicate: answers[_predicate_key(predicate)]
        ):
            raise ValueError("branch condition truth contradicts its typed Boolean program")
        return self


class CompiledBranchCondition(CompiledBranchConditionPayload):
    condition_sha256: Sha256

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        if self.condition_sha256 != canonical_json_sha256(
            self.model_dump(
                mode="json",
                by_alias=True,
                exclude={"condition_sha256"},
            )
        ):
            raise ValueError("branch condition differs from its exact preimage")
        return self

    @classmethod
    def seal(cls, payload: CompiledBranchConditionPayload) -> Self:
        document = payload.model_dump(mode="json", by_alias=True)
        return cls.model_validate({**document, "condition_sha256": canonical_json_sha256(document)})


class CompiledBranchScopePayload(Stove0ProtocolModel):
    format: Literal["stove0-compiled-branch-selection/v1"] = "stove0-compiled-branch-selection/v1"
    work_id: Sha256
    recipe: RecipeIdentityRef
    branch_id: LocalName
    inventory: ArtifactSelectionRef
    selection: ArtifactSelectionRef
    group_source: InputGroupSet | None
    choice_count: NonnegativeDecimal
    selected_count: NonnegativeDecimal
    conditions_sha256: Sha256

    @model_validator(mode="after")
    def exact_domain(self) -> Self:
        if self.selected_count > self.choice_count:
            raise ValueError("selected candidates exceed the complete choice domain")
        if self.group_source is not None:
            if (
                self.group_source.work_id,
                self.group_source.recipe,
                self.group_source.inventory,
                self.group_source.group_count,
            ) != (
                self.work_id,
                self.recipe,
                self.inventory,
                self.choice_count,
            ):
                raise ValueError("branch choices changed their exact input group domain")
        elif self.choice_count > 1:
            raise ValueError("whole-input branch has more than one aggregate candidate")
        if bool(self.selected_count) != bool(self.selection.artifact_count):
            raise ValueError("branch candidate outcome differs from its nonempty member selection")
        return self


class CompiledBranchScope(CompiledBranchScopePayload):
    branch_selection_sha256: Sha256

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        if self.branch_selection_sha256 != canonical_json_sha256(
            self.model_dump(
                mode="json",
                by_alias=True,
                exclude={"branch_selection_sha256"},
            )
        ):
            raise ValueError("branch selection differs from its exact candidate decisions")
        return self

    @classmethod
    def seal(cls, payload: CompiledBranchScopePayload) -> Self:
        document = payload.model_dump(mode="json", by_alias=True)
        return cls.model_validate(
            {**document, "branch_selection_sha256": canonical_json_sha256(document)}
        )


def declare_compiled_branches(metadata: MetaData, planning: Table) -> dict[str, Table]:
    branch = Table(
        "stove0_compiled_branch_selections",
        metadata,
        Column(
            "work_id",
            String(129),
            ForeignKey(planning.c.work_id, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("branch_id", Text, primary_key=True),
        Column("binding_json", Text, nullable=False),
        Column("revision", BigInteger, nullable=False),
        Column("phase", String(16), nullable=False),
        Column("source_json", Text, nullable=False),
        Column("scope_sha256", String(64)),
        Column("selection_sha256", String(64)),
        Column("choice_count", BigInteger, nullable=False),
        Column("selected_count", BigInteger, nullable=False),
        Column("hash_state", Text, nullable=False),
        Column("authority_json", Text),
        CheckConstraint(
            "revision >= 1 AND choice_count >= 0 AND selected_count >= 0 "
            "AND selected_count <= choice_count",
            name="ck_stove0_branch_selection_counts",
        ),
        CheckConstraint(
            "phase IN ('init','candidate','facts','copy','seal','complete')",
            name="ck_stove0_branch_selection_phase",
        ),
        Index("ix_stove0_branch_selection_scopes", "scope_sha256"),
        Index("ix_stove0_branch_selection_results", "selection_sha256"),
    )
    choices = Table(
        "stove0_compiled_branch_choices",
        metadata,
        Column(
            "work_id",
            String(129),
            ForeignKey(planning.c.work_id, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("branch_id", Text, primary_key=True),
        Column("choice_ordinal", BigInteger, primary_key=True),
        Column("primary_id", Text),
        Column("scope_sha256", String(64), nullable=False),
        Column("condition_json", Text, nullable=False),
        Column("condition_sha256", String(64), nullable=False),
        Column("truth", String(16), nullable=False),
        UniqueConstraint(
            "work_id", "branch_id", "primary_id", name="uq_stove0_branch_primary_choice"
        ),
        CheckConstraint(
            "choice_ordinal >= 0 AND truth IN ('true','false')",
            name="ck_stove0_branch_choice_domain",
        ),
        Index("ix_stove0_branch_choice_scopes", "scope_sha256"),
    )
    return {"branches": branch, "choices": choices}


class CompiledBranchPlanning:
    def __init__(self, state: CompiledStatePort, tables: Mapping[str, Table]) -> None:
        self.state, self.engine, self.tables = state, state.engine, tables
        self.facts, self.scopes = CompiledFactEvaluation(state), CompiledInputScopes(state)

    def _insert(self, table: Table) -> PgInsert | SqliteInsert:
        return (
            pg_insert(table) if self.engine.dialect.name == "postgresql" else sqlite_insert(table)
        )

    def _key(self, table: Table, work_id: str, branch_id: str) -> ColumnElement[bool]:
        return and_(
            table.c.work_id == self.state.planning_key(work_id), table.c.branch_id == branch_id
        )

    def _change(
        self,
        row: Mapping[str, Any],
        *,
        writes: Callable[[Connection], None] | None = None,
        **changes: Any,
    ) -> None:
        table = self.tables["branches"]
        with self.engine.begin() as connection:
            current = (
                connection.execute(
                    select(table)
                    .where(self._key(table, row["work_id"], row["branch_id"]))
                    .with_for_update()
                )
                .mappings()
                .one()
            )
            if current["revision"] != row["revision"]:
                raise ConcurrentWorkUpdate("compiled branch selection advanced concurrently")
            if writes is not None:
                writes(connection)
            if isinstance(changes.get("source_json"), dict):
                changes["source_json"] = canonical_json_bytes(changes["source_json"]).decode(
                    "utf-8"
                )
            updated = connection.execute(
                update(table)
                .where(
                    self._key(table, row["work_id"], row["branch_id"]),
                    table.c.revision == row["revision"],
                )
                .values(revision=row["revision"] + 1, **changes)
            )
            if updated.rowcount != 1:
                raise ConcurrentWorkUpdate("compiled branch selection advanced concurrently")

    def step(
        self, work: WorkIdentity, branch_id: str
    ) -> CompiledBranchScope | WorkInapplicable | None:
        retained = self.state.recipe_definitions.load(work.recipe)
        if retained is None:
            raise ValueError("branch planning has no retained exact compiled closure")
        recipe, _ = retained
        if branch_id not in recipe.branches:
            raise ValueError("branch planning selected an undeclared branch")
        definition = recipe.branches[branch_id]
        planning = self.state.compiled_planning.ensure(work.work_id, recipe)
        inventory = self.state.compiled_planning.inventory_ref(work.work_id)
        if (
            inventory is None
            or planning["classified_count"] != inventory.artifact_count
            or planning["decision_ordinal"] != len(recipe.decisions)
            or planning["decision_json"] is not None
        ):
            raise ValueError("branch planning requires full classification and resolved decisions")
        binding: dict[str, JsonValue] = {
            "work_id": work.work_id,
            "recipe": work.recipe.model_dump(mode="json"),
            "branch_id": branch_id,
            "inventory": inventory.model_dump(mode="json"),
        }
        encoded, table = canonical_json_bytes(binding).decode("utf-8"), self.tables["branches"]
        with self.engine.begin() as connection:
            connection.execute(
                self._insert(table)
                .values(
                    work_id=self.state.planning_key(work.work_id),
                    branch_id=branch_id,
                    binding_json=encoded,
                    revision=1,
                    phase="init",
                    source_json="{}",
                    choice_count=0,
                    selected_count=0,
                    hash_state=CheckpointSHA256().export_state(),
                )
                .on_conflict_do_nothing()
            )
            row = dict(
                connection.execute(select(table).where(self._key(table, work.work_id, branch_id)))
                .mappings()
                .one()
            )
        if row["binding_json"] != encoded:
            raise ValueError("branch continuation changed its exact compiled invocation")
        if row["phase"] == "complete":
            authority = CompiledBranchScope.model_validate_json(row["authority_json"])
            if (
                authority.work_id,
                authority.recipe,
                authority.branch_id,
                authority.inventory,
                authority.selection.selection_sha256,
            ) != (
                work.work_id,
                work.recipe,
                branch_id,
                inventory,
                row["selection_sha256"],
            ) or self.state.load_selection_ref(
                authority.selection.selection_sha256
            ) != authority.selection:
                raise ValueError("branch selection lost its exact retained authority")
            return authority
        builders = self.state.metadata_selections
        builder_binding: dict[str, JsonValue] = {
            "format": "stove0-branch-members-builder/v1",
            **binding,
        }
        builder_id = canonical_json_sha256(builder_binding)
        aggregate = builders.ensure(builder_id, binding=builder_binding, source={"choice": 0})
        source = json.loads(row["source_json"])
        if row["phase"] == "init":
            if isinstance(definition.select, GroupSelection):
                groups = self.state.compiled_groups.step(work, definition.select.groups)
                if groups is None:
                    return None
                source = {"groups": groups.model_dump(mode="json"), "after_primary_id": ""}
            else:
                scope = self.scopes.subject_port(
                    work=work,
                    task_id="$branches",
                    port_id=branch_id,
                    binding=CompiledRoleSelection(roles=recipe.roles),
                    inventory=inventory,
                )
                if scope is None:
                    return None
                source = {"whole_scope": scope.model_dump(mode="json"), "whole_consumed": False}
            self._change(row, source_json=source, phase="candidate")
            return None
        if row["phase"] == "candidate":
            groups = InputGroupSet.model_validate(source["groups"]) if "groups" in source else None
            primary, candidate = None, None
            if groups is not None:
                members = self.state.compiled_groups.tables["members"]
                with self.engine.connect() as connection:
                    primary = connection.scalar(
                        select(members.c.primary_id)
                        .where(
                            members.c.work_id == self.state.planning_key(work.work_id),
                            members.c.group_id == groups.group_id,
                            members.c.primary_id > source["after_primary_id"],
                            members.c.associated_id == "",
                            members.c.member_ordinal.is_not(None),
                        )
                        .order_by(members.c.primary_id)
                        .limit(1)
                    )
                if primary is not None:
                    candidate = self.state.compiled_groups.candidate(groups, primary)
                    if candidate is None:
                        return None
            elif not source["whole_consumed"]:
                candidate = ArtifactSelectionRef.model_validate(source["whole_scope"])
            if candidate is None or candidate.artifact_count == 0:
                if aggregate["state"] == "collecting":
                    builders.append(
                        builder_id,
                        expected_revision=aggregate["revision"],
                        subjects=(),
                        source={"choice": row["choice_count"]},
                        complete=True,
                    )
                self._change(row, phase="seal", scope_sha256=None)
                return None
            source.update(
                candidate=candidate.model_dump(mode="json"),
                primary_id=primary,
                fact_ordinal=0,
                copy_continuation=None,
            )
            self._change(
                row, phase="facts", source_json=source, scope_sha256=candidate.selection_sha256
            )
            return None
        candidate = (
            ArtifactSelectionRef.model_validate(source["candidate"])
            if "candidate" in source
            else None
        )
        if row["phase"] == "facts":
            if candidate is None:
                raise ValueError("branch condition lost its exact candidate")
            leaves = {
                _predicate_key(predicate): predicate
                for predicate in facts_predicates(definition.when)
            }
            keys, ordinal = sorted(leaves), source["fact_ordinal"]
            if ordinal < len(keys):
                if (
                    self.facts.proof_step(work, leaves[keys[ordinal]], candidate=candidate)
                    is not None
                ):
                    self._change(row, source_json={**source, "fact_ordinal": ordinal + 1})
                return None
            if ordinal > len(keys):
                raise ValueError("branch condition continuation exceeded its exact predicate set")
            proofs: list[CompiledFactProof] = []
            for key in keys:
                evaluation = self.facts.proof_step(work, leaves[key], candidate=candidate)
                if evaluation is None:
                    raise ValueError("branch lost a completed exact predicate evaluation")
                proofs.append(evaluation)
            answers = {key: proof.truth for key, proof in zip(keys, proofs, strict=True)}
            truth = evaluate_predicate(
                definition.when, lambda predicate: answers[_predicate_key(predicate)]
            )
            proof = CompiledBranchCondition.seal(
                CompiledBranchConditionPayload(
                    work_id=work.work_id,
                    recipe=work.recipe,
                    branch_id=branch_id,
                    inventory=inventory,
                    candidate=candidate if isinstance(definition.select, GroupSelection) else None,
                    condition=definition.when,
                    evaluations=tuple(proofs),
                    truth=truth,
                )
            )
            if truth == Truth.INDETERMINATE:
                return WorkInapplicable(
                    code="branch-evidence-indeterminate",
                    message="Accepted evidence cannot resolve the required branch condition.",
                )
            choices = self.tables["choices"]
            document = canonical_json_bytes(proof.model_dump(mode="json")).decode("utf-8")
            digest = CheckpointSHA256.from_state(row["hash_state"])
            choice = canonical_json_bytes(
                {
                    "primary_id": source["primary_id"],
                    "selection": candidate.model_dump(mode="json"),
                    "condition_sha256": proof.condition_sha256,
                    "truth": truth.value,
                }
            )
            digest.update(b"stove0-branch-choices/v1\x00")
            digest.update(str(row["choice_count"]).encode("ascii") + b"\x00")
            digest.update(len(choice).to_bytes(8, "big") + choice)

            def record_choice(connection: Connection) -> None:
                connection.execute(
                    self._insert(choices)
                    .values(
                        work_id=self.state.planning_key(work.work_id),
                        branch_id=branch_id,
                        choice_ordinal=row["choice_count"],
                        primary_id=source["primary_id"],
                        scope_sha256=candidate.selection_sha256,
                        condition_json=document,
                        condition_sha256=proof.condition_sha256,
                        truth=truth.value,
                    )
                    .on_conflict_do_nothing()
                )
                stored = connection.scalar(
                    select(choices.c.condition_json).where(
                        self._key(choices, work.work_id, branch_id),
                        choices.c.choice_ordinal == row["choice_count"],
                    )
                )
                if stored != document:
                    raise ValueError("exact branch candidate condition was rebound")

            self._change(
                row,
                writes=record_choice,
                choice_count=row["choice_count"] + 1,
                selected_count=row["selected_count"] + int(truth == Truth.TRUE),
                hash_state=digest.export_state(),
                phase="copy" if truth == Truth.TRUE else "candidate",
                source_json=source if truth == Truth.TRUE else self._next_candidate(source),
                scope_sha256=candidate.selection_sha256 if truth == Truth.TRUE else None,
            )
            return None
        if row["phase"] == "copy":
            if candidate is None:
                raise ValueError("branch copy lost its exact candidate")
            page, cursor, complete = self.state.selection_artifact_page(
                candidate.selection_sha256, continuation=source["copy_continuation"], limit=100
            )
            # The selection builder's own CAS persists the byte/member cursor.
            # Use that cursor after a crash between append and branch advancement.
            committed = json.loads(aggregate["source_json"])
            if (
                committed.get("choice") == row["choice_count"]
                and committed.get("source_continuation") == source["copy_continuation"]
            ):
                cursor, complete = committed["next_continuation"], committed["complete"]
            else:
                builders.append(
                    builder_id,
                    expected_revision=aggregate["revision"],
                    subjects=page,
                    source={
                        "choice": row["choice_count"],
                        "source_continuation": source["copy_continuation"],
                        "next_continuation": cursor,
                        "complete": complete,
                    },
                    complete=False,
                )
            self._change(
                row,
                phase="candidate" if complete else "copy",
                source_json=self._next_candidate(source)
                if complete
                else {**source, "copy_continuation": cursor},
                scope_sha256=None if complete else candidate.selection_sha256,
            )
            return None
        if row["phase"] != "seal":
            raise ValueError("unknown compiled branch selection continuation")
        selection = builders.seal_step(builder_id)
        if selection is None:
            return None
        authority = CompiledBranchScope.seal(
            CompiledBranchScopePayload.model_validate(
                {
                    "work_id": work.work_id,
                    "recipe": work.recipe,
                    "branch_id": branch_id,
                    "inventory": inventory,
                    "selection": selection,
                    "group_source": InputGroupSet.model_validate(source["groups"])
                    if "groups" in source
                    else None,
                    "choice_count": str(row["choice_count"]),
                    "selected_count": str(row["selected_count"]),
                    "conditions_sha256": CheckpointSHA256.from_state(row["hash_state"]).hexdigest(),
                }
            )
        )
        self._change(
            row,
            phase="complete",
            selection_sha256=selection.selection_sha256,
            authority_json=canonical_json_bytes(authority.model_dump(mode="json")).decode("utf-8"),
        )
        return authority

    def _next_candidate(self, source: dict[str, Any]) -> dict[str, Any]:
        if "groups" in source:
            return {"groups": source["groups"], "after_primary_id": source["primary_id"]}
        return {"whole_scope": source["whole_scope"], "whole_consumed": True}

    def choice_page(
        self, authority: CompiledBranchScope, *, start_ordinal: int, limit: int = 100
    ) -> tuple[tuple[str, ArtifactSelectionRef, CompiledBranchCondition], ...]:
        if (
            type(start_ordinal) is not int
            or not 0 <= start_ordinal <= authority.choice_count
            or type(limit) is not int
            or not 1 <= limit <= 100
        ):
            raise ValueError("branch choice page requires an exact ordinal and bounded limit")
        b, c = self.tables["branches"], self.tables["choices"]
        with self.engine.connect() as connection:
            stored = connection.scalar(
                select(b.c.authority_json).where(
                    self._key(b, authority.work_id, authority.branch_id),
                    b.c.phase == "complete",
                )
            )
            if stored is None or CompiledBranchScope.model_validate_json(stored) != authority:
                raise ValueError("branch choices have no exact retained selection authority")
            rows = (
                connection.execute(
                    select(c)
                    .where(
                        self._key(c, authority.work_id, authority.branch_id),
                        c.c.choice_ordinal >= start_ordinal,
                    )
                    .order_by(c.c.choice_ordinal)
                    .limit(limit)
                )
                .mappings()
                .all()
            )
        if [row["choice_ordinal"] for row in rows] != list(
            range(start_ordinal, start_ordinal + len(rows))
        ):
            raise ValueError("branch choices have an ordinal gap")
        choices: list[tuple[str, ArtifactSelectionRef, CompiledBranchCondition]] = []
        for row in rows:
            condition = CompiledBranchCondition.model_validate_json(row["condition_json"])
            reference = condition.candidate
            if reference is None:
                reference = self.state.load_selection_ref(row["scope_sha256"])
                if reference is None:
                    raise ValueError("branch choice lost its exact sealed candidate")
            choices.append((row["primary_id"], reference, condition))
        return tuple(choices)
