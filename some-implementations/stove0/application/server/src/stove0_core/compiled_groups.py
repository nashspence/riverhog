"""Checkpoint exact relation grouping without materializing a collection in RAM."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from pydantic import JsonValue
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    ForeignKey,
    Index,
    MetaData,
    String,
    Table,
    Text,
    and_,
    case,
    literal,
    or_,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import Insert as PgInsert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import Insert as SqliteInsert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.schema import SchemaItem
from stove0_protocol import (
    ArtifactSelectionRef,
    WorkArtifactSubject,
    WorkIdentity,
    canonical_json_bytes,
)
from stove0_protocol.input_groups import (
    InputGroupMember,
    InputGroupPage,
    InputGroupSet,
    InputGroupSetPayload,
    update_input_group_commitment,
)
from stove0_protocol.observation_evidence import AcceptedView, AcceptedViewRecord
from stove0_protocol.observation_interfaces import ObservationInterface, RelationView
from stove0_protocol.observation_views import RelationViewResult
from stove0_recipe_config.compiled import CompiledRecipe

from stove0_core._checkpoint_sha256 import CheckpointSHA256
from stove0_core.accepted_view_queries import accepted_view_queries
from stove0_core.compiled_state_ports import CompiledStatePort
from stove0_core.work_state import ConcurrentWorkUpdate


def declare_compiled_groups(metadata: MetaData, planning: Table) -> dict[str, Table]:
    group = Table(
        "stove0_compiled_groups",
        metadata,
        Column(
            "work_id",
            String(129),
            ForeignKey(planning.c.work_id, ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("group_id", Text, primary_key=True),
        Column("binding_json", Text, nullable=False),
        Column("revision", BigInteger, nullable=False),
        Column("phase", String(16), nullable=False),
        Column("tier_ordinal", BigInteger, nullable=False),
        Column("after_subject_id", Text, nullable=False),
        Column("active_subject_id", Text),
        Column("after_primary_id", Text, nullable=False),
        Column("after_associated_id", Text, nullable=False),
        Column("global_blocked", Boolean, nullable=False),
        Column("hashed_count", BigInteger, nullable=False),
        Column("hashed_groups", BigInteger, nullable=False),
        Column("hash_state", Text, nullable=False),
        Column("authority_json", Text),
        CheckConstraint(
            "revision >= 1 AND tier_ordinal >= 0 AND hashed_count >= 0 AND hashed_groups >= 0",
            name="ck_stove0_compiled_group_counts",
        ),
        CheckConstraint(
            "phase IN ('coverage','associate','block','sealing','complete')",
            name="ck_stove0_compiled_group_phase",
        ),
    )

    # The work FK makes all projections share the compiled invocation's owner.
    def common() -> tuple[SchemaItem, ...]:
        return (
            Column(
                "work_id",
                String(129),
                ForeignKey(planning.c.work_id, ondelete="CASCADE"),
                primary_key=True,
            ),
            Column("group_id", Text, primary_key=True),
        )

    tiers = Table(
        "stove0_compiled_group_tiers",
        metadata,
        *common(),
        Column("tier_ordinal", BigInteger, primary_key=True),
        Column("primary_indeterminate", Boolean, nullable=False),
    )
    associations = Table(
        "stove0_compiled_group_associations",
        metadata,
        *common(),
        Column("subject_id", Text, primary_key=True),
        Column("primary_id", Text),
        Column("state", String(16), nullable=False),
        Column("tier_ordinal", BigInteger),
        CheckConstraint(
            "state IN ('attached','unmatched','blocked')",
            name="ck_stove0_compiled_association_state",
        ),
        Index("ix_stove0_compiled_group_attachments", "work_id", "group_id", "primary_id"),
    )
    blocked = Table(
        "stove0_compiled_group_blocked",
        metadata,
        *common(),
        Column("primary_id", Text, primary_key=True),
    )
    members = Table(
        "stove0_compiled_group_members",
        metadata,
        *common(),
        Column("primary_id", Text, primary_key=True),
        # Empty is only a private index for the typed primary declaration.
        Column("associated_id", Text, primary_key=True),
        Column("member_ordinal", BigInteger),
        CheckConstraint(
            "member_ordinal IS NULL OR member_ordinal >= 0",
            name="ck_stove0_compiled_group_member_ordinal",
        ),
        Index("ix_stove0_compiled_group_member_pages", "work_id", "group_id", "member_ordinal"),
    )
    return {
        "groups": group,
        "tiers": tiers,
        "associations": associations,
        "blocked": blocked,
        "members": members,
    }


class CompiledGroupPlanning:
    def __init__(self, state: CompiledStatePort, tables: Mapping[str, Table]) -> None:
        self.state, self.engine, self.tables = state, state.engine, tables

    def _insert(self, table: Table) -> PgInsert | SqliteInsert:
        return (
            pg_insert(table) if self.engine.dialect.name == "postgresql" else sqlite_insert(table)
        )

    def _key(self, table: Table, work_id: str, group_id: str) -> ColumnElement[bool]:
        return and_(
            table.c.work_id == self.state.planning_key(work_id), table.c.group_id == group_id
        )

    def _definition(
        self, work: WorkIdentity, group_id: str
    ) -> tuple[
        CompiledRecipe, ArtifactSelectionRef, list[tuple[AcceptedView, ObservationInterface]]
    ]:
        retained = self.state.recipe_definitions.load(work.recipe)
        if retained is None:
            raise ValueError("group planning has no retained exact compiled closure")
        recipe, closure = retained
        if group_id not in recipe.groups:
            raise ValueError("group planning selected an undeclared compiled group")
        inventory = self.state.compiled_planning.inventory_ref(work.work_id)
        if inventory is None:
            raise ValueError("group planning has no exact original inventory")
        views = []
        for name in recipe.groups[group_id].prefer:
            task_id, view_id = name.split(".")
            accepted = self.state.accepted_observations.accepted(work.work_id, task_id)
            if accepted is None:
                raise ValueError("group planning requires complete accepted relation tasks")
            authority = self.state.accepted_observations.retained_view(
                accepted, view_id=view_id, selected_scope=accepted.question.scope
            )
            if authority is None:
                raise ValueError("group planning requires an exact sealed relation view")
            interface = closure.interface(
                id=authority.interface.id, sha256=authority.interface.sha256
            ).interface
            if not isinstance(interface.views[view_id], RelationView):
                raise ValueError("group preference must select a typed relation view")
            views.append((authority, interface))
        return recipe, inventory, views

    def _ensure(
        self,
        work: WorkIdentity,
        group_id: str,
        recipe: CompiledRecipe,
        inventory: ArtifactSelectionRef,
        views: list[tuple[AcceptedView, ObservationInterface]],
    ) -> dict[str, Any]:
        table = self.tables["groups"]
        binding = canonical_json_bytes(
            {
                "work_id": work.work_id,
                "recipe": recipe.ref.model_dump(mode="json"),
                "group_id": group_id,
                "inventory": inventory.model_dump(mode="json"),
                "supports": [view.model_dump(mode="json") for view, _ in views],
            }
        ).decode("utf-8")
        with self.engine.begin() as connection:
            connection.execute(
                self._insert(table)
                .values(
                    work_id=self.state.planning_key(work.work_id),
                    group_id=group_id,
                    binding_json=binding,
                    revision=1,
                    phase="coverage",
                    tier_ordinal=0,
                    after_subject_id="",
                    active_subject_id=None,
                    after_primary_id="",
                    after_associated_id="",
                    global_blocked=False,
                    hashed_count=0,
                    hashed_groups=0,
                    hash_state=CheckpointSHA256().export_state(),
                )
                .on_conflict_do_nothing()
            )
            row = (
                connection.execute(select(table).where(self._key(table, work.work_id, group_id)))
                .mappings()
                .one()
            )
            if row["binding_json"] != binding:
                raise ValueError("group continuation changed its exact compiled inputs")
            return dict(row)

    def _change(
        self,
        row: Mapping[str, Any],
        *,
        writes: Callable[[Connection], None] | None = None,
        **changes: Any,
    ) -> None:
        table = self.tables["groups"]
        with self.engine.begin() as connection:
            current = (
                connection.execute(
                    select(table)
                    .where(self._key(table, row["work_id"], row["group_id"]))
                    .with_for_update()
                )
                .mappings()
                .one()
            )
            if current["revision"] != row["revision"]:
                raise ConcurrentWorkUpdate("compiled group advanced concurrently")
            if writes is not None:
                writes(connection)
            result = connection.execute(
                update(table)
                .where(
                    self._key(table, row["work_id"], row["group_id"]),
                    table.c.revision == row["revision"],
                )
                .values(revision=row["revision"] + 1, **changes)
            )
            if result.rowcount != 1:
                raise ConcurrentWorkUpdate("compiled group advanced concurrently")

    def step(self, work: WorkIdentity, group_id: str) -> InputGroupSet | None:
        recipe, inventory, views = self._definition(work, group_id)
        definition = recipe.groups[group_id]
        row = self._ensure(work, group_id, recipe, inventory, views)
        if row["phase"] == "complete":
            sealed_authority = InputGroupSet.model_validate_json(row["authority_json"])
            if (
                sealed_authority.work_id,
                sealed_authority.recipe,
                sealed_authority.group_id,
                sealed_authority.inventory,
                sealed_authority.supports,
            ) != (work.work_id, work.recipe, group_id, inventory, tuple(view for view, _ in views)):
                raise ValueError("retained group authority changed its exact support")
            return sealed_authority
        if row["phase"] == "coverage":
            if row["tier_ordinal"] == len(views):
                self._change(row, phase="associate", tier_ordinal=0, after_subject_id="")
                return None
            if row["tier_ordinal"] > len(views):
                raise ValueError("group coverage exceeded its exact preference tiers")
            page, cursor, complete = self.state.compiled_planning.role_page(
                work.work_id,
                roles=(definition.primary,),
                after_subject_id=row["after_subject_id"],
                limit=100,
            )
            authority, interface = views[row["tier_ordinal"]]
            relation = accepted_view_queries(
                self.state.accepted_observations, authority, interface=interface
            )
            if not isinstance(relation, RelationViewResult):
                raise ValueError("group planning selected another accepted view kind")
            statuses = relation.statuses
            bad = any(
                member.id not in statuses or statuses[member.id] != "complete" for member in page
            )
            tiers = self.tables["tiers"]

            def record_tier(connection: Connection) -> None:
                connection.execute(
                    self._insert(tiers)
                    .values(
                        work_id=self.state.planning_key(work.work_id),
                        group_id=group_id,
                        tier_ordinal=row["tier_ordinal"],
                        primary_indeterminate=bad,
                    )
                    .on_conflict_do_nothing()
                )
                if bad:
                    connection.execute(
                        update(tiers)
                        .where(
                            self._key(tiers, work.work_id, group_id),
                            tiers.c.tier_ordinal == row["tier_ordinal"],
                        )
                        .values(primary_indeterminate=True)
                    )

            self._change(
                row,
                writes=record_tier,
                tier_ordinal=row["tier_ordinal"] + int(complete),
                after_subject_id="" if complete else cursor,
            )
            return None
        if row["phase"] == "block":
            authority, _ = views[row["tier_ordinal"]]
            matches = self._matches(
                work,
                authority,
                definition.primary,
                row["active_subject_id"],
                after=row["after_primary_id"],
            )
            blocked = self.tables["blocked"]

            def record_block(connection: Connection) -> None:
                for primary in matches:
                    connection.execute(
                        self._insert(blocked)
                        .values(
                            work_id=self.state.planning_key(work.work_id),
                            group_id=group_id,
                            primary_id=primary,
                        )
                        .on_conflict_do_nothing()
                    )

            self._change(
                row,
                writes=record_block,
                phase="block" if len(matches) == 100 else "associate",
                after_primary_id=matches[-1] if len(matches) == 100 else "",
                active_subject_id=row["active_subject_id"] if len(matches) == 100 else None,
                after_subject_id=row["after_subject_id"]
                if len(matches) == 100
                else row["active_subject_id"],
                tier_ordinal=row["tier_ordinal"] if len(matches) == 100 else 0,
            )
            return None
        if row["phase"] == "associate":
            active = row["active_subject_id"]
            if active is None:
                page, _, _ = self.state.compiled_planning.role_page(
                    work.work_id,
                    roles=definition.attach,
                    after_subject_id=row["after_subject_id"],
                    limit=1,
                )
                if not page:
                    self._change(row, phase="sealing", after_primary_id="", after_associated_id="")
                    return None
                active = page[0].id
            tier = row["tier_ordinal"]
            if tier > len(views):
                raise ValueError("association continuation exceeded its exact preference tiers")
            primary, outcome = None, "unmatched"
            global_blocked, ambiguity = row["global_blocked"], False
            if tier < len(views) and not global_blocked:
                authority, interface = views[tier]
                t = self.tables["tiers"]
                with self.engine.connect() as connection:
                    primary_bad = connection.scalar(
                        select(t.c.primary_indeterminate).where(
                            self._key(t, work.work_id, group_id),
                            t.c.tier_ordinal == tier,
                        )
                    )
                relation = accepted_view_queries(
                    self.state.accepted_observations, authority, interface=interface
                )
                if not isinstance(relation, RelationViewResult):
                    raise ValueError("group planning selected another accepted view kind")
                statuses = relation.statuses
                if primary_bad is None:
                    raise ValueError("relation tier has no complete primary coverage proof")
                if primary_bad or active not in statuses or statuses[active] != "complete":
                    global_blocked, outcome = True, "blocked"
                else:
                    # A positive stronger relation cannot become a negative
                    # merely because classification excludes its primary.
                    matches = self._matches(work, authority, None, active, limit=2)
                    if not matches:
                        self._change(row, active_subject_id=active, tier_ordinal=tier + 1)
                        return None
                    if len(matches) > 1:
                        ambiguity, outcome = True, "blocked"
                    else:
                        classification = self.state.compiled_planning.tables["classification"]
                        with self.engine.connect() as connection:
                            role = connection.scalar(
                                select(classification.c.role).where(
                                    classification.c.work_id
                                    == self.state.planning_key(work.work_id),
                                    classification.c.subject_id == matches[0],
                                )
                            )
                        if role == definition.primary:
                            primary, outcome = matches[0], "attached"
            elif global_blocked:
                outcome = "blocked"
            a, m = self.tables["associations"], self.tables["members"]

            def record_association(connection: Connection) -> None:
                connection.execute(
                    self._insert(a)
                    .values(
                        work_id=self.state.planning_key(work.work_id),
                        group_id=group_id,
                        subject_id=active,
                        primary_id=primary,
                        state=outcome,
                        tier_ordinal=tier if tier < len(views) else None,
                    )
                    .on_conflict_do_nothing()
                )
                if primary is not None:
                    connection.execute(
                        self._insert(m)
                        .values(
                            work_id=self.state.planning_key(work.work_id),
                            group_id=group_id,
                            primary_id=primary,
                            associated_id=active,
                        )
                        .on_conflict_do_nothing()
                    )

            self._change(
                row,
                writes=record_association,
                global_blocked=global_blocked,
                phase="block" if ambiguity else "associate",
                active_subject_id=active if ambiguity else None,
                tier_ordinal=tier if ambiguity else 0,
                after_primary_id="",
                after_subject_id=row["after_subject_id"] if ambiguity else active,
            )
            return None
        if row["phase"] != "sealing":
            raise ValueError("unknown compiled group continuation")
        # Primaries are staged in bounded pages before any membership hash is
        # sealed. This preserves primaries with no attachments as real groups.
        if row["active_subject_id"] != "$primaries-complete":
            page, cursor, complete = self.state.compiled_planning.role_page(
                work.work_id,
                roles=(definition.primary,),
                after_subject_id=row["after_primary_id"],
                limit=100,
            )
            m = self.tables["members"]

            def record_primaries(connection: Connection) -> None:
                for member in page:
                    connection.execute(
                        self._insert(m)
                        .values(
                            work_id=self.state.planning_key(work.work_id),
                            group_id=group_id,
                            primary_id=member.id,
                            associated_id="",
                        )
                        .on_conflict_do_nothing()
                    )

            self._change(
                row,
                writes=record_primaries,
                active_subject_id="$primaries-complete" if complete else None,
                after_primary_id="" if complete else cursor,
            )
            return None
        m = self.tables["members"]
        statement = self._members(work.work_id, group_id, globally_blocked=row["global_blocked"])
        with self.engine.connect() as connection:
            member_page = (
                connection.execute(
                    statement.where(
                        or_(
                            m.c.primary_id > row["after_primary_id"],
                            and_(
                                m.c.primary_id == row["after_primary_id"],
                                m.c.associated_id > row["after_associated_id"],
                            ),
                        )
                    )
                    .order_by(m.c.primary_id, m.c.associated_id)
                    .limit(100)
                )
                .mappings()
                .all()
            )
        digest = CheckpointSHA256.from_state(row["hash_state"])
        count, groups = row["hashed_count"], row["hashed_groups"]
        for item in member_page:
            update_input_group_commitment(
                digest,
                ordinal=count,
                member=InputGroupMember(
                    primary_id=item["primary_id"],
                    associated_id=item["associated_id"] or None,
                ),
            )
            count += 1
            groups += int(not item["associated_id"])
        sealed_groups: InputGroupSet | None = None
        if len(member_page) < 100:
            sealed_groups = InputGroupSet.seal(
                InputGroupSetPayload.model_validate(
                    {
                        "work_id": work.work_id,
                        "recipe": work.recipe,
                        "group_id": group_id,
                        "inventory": inventory,
                        "supports": tuple(view for view, _ in views),
                        "group_count": str(groups),
                        "association_count": str(count - groups),
                        "members_sha256": digest.hexdigest(),
                    }
                )
            )

        def record_ordinals(connection: Connection) -> None:
            for ordinal, item in enumerate(member_page, row["hashed_count"]):
                connection.execute(
                    update(m)
                    .where(
                        self._key(m, work.work_id, group_id),
                        m.c.primary_id == item["primary_id"],
                        m.c.associated_id == item["associated_id"],
                    )
                    .values(member_ordinal=ordinal)
                )

        self._change(
            row,
            writes=record_ordinals,
            hashed_count=count,
            hashed_groups=groups,
            hash_state=digest.export_state(),
            after_primary_id=member_page[-1]["primary_id"]
            if member_page
            else row["after_primary_id"],
            after_associated_id=member_page[-1]["associated_id"]
            if member_page
            else row["after_associated_id"],
            phase="complete" if sealed_groups is not None else "sealing",
            authority_json=None
            if sealed_groups is None
            else canonical_json_bytes(sealed_groups.model_dump(mode="json")).decode("utf-8"),
        )
        return sealed_groups

    def _matches(
        self,
        work: WorkIdentity,
        authority: AcceptedView,
        primary_role: str | None,
        associated: str,
        *,
        after: str = "",
        limit: int = 100,
    ) -> Sequence[str]:
        if not 1 <= limit <= 100:
            raise ValueError("relation match page must be between 1 and 100")
        r = self.state.accepted_observations.tables["records"]
        c = self.state.compiled_planning.tables["classification"]
        statement = select(r.c.primary_id)
        if primary_role is not None:
            statement = statement.select_from(
                r.join(
                    c,
                    and_(
                        c.c.work_id == self.state.planning_key(work.work_id),
                        c.c.subject_id == r.c.primary_id,
                        c.c.role == primary_role,
                    ),
                )
            )
        statement = statement.where(
            r.c.question_sha256 == self.state.planning_key(authority.question_sha256),
            r.c.view_id == authority.view_id,
            r.c.kind == "relation",
            r.c.associated_id == associated,
            r.c.primary_id > after,
        )
        with self.engine.connect() as connection:
            matches = connection.scalars(
                statement.distinct().order_by(r.c.primary_id).limit(limit)
            ).all()
            # Recheck the bounded canonical records rather than treating mutable
            # index columns as a second evidence authority.
            if matches:
                for primary_id in matches:
                    indexed = (
                        connection.execute(
                            select(r)
                            .where(
                                r.c.question_sha256
                                == self.state.planning_key(authority.question_sha256),
                                r.c.view_id == authority.view_id,
                                r.c.kind == "relation",
                                r.c.associated_id == associated,
                                r.c.primary_id == primary_id,
                            )
                            .limit(1)
                        )
                        .mappings()
                        .one()
                    )
                    record = AcceptedViewRecord.model_validate_json(indexed["record_json"])
                    if record.kind != "relation" or record.value != {
                        "primary_id": indexed["primary_id"],
                        "associated_id": indexed["associated_id"],
                    }:
                        raise ValueError("relation indices differ from exact accepted records")
        return matches

    def _members(
        self, work_id: str, group_id: str, *, globally_blocked: bool = False
    ) -> Select[Any]:
        m, b = self.tables["members"], self.tables["blocked"]
        return select(m).where(
            self._key(m, work_id, group_id),
            literal(not globally_blocked),
            ~select(b.c.primary_id)
            .where(
                self._key(b, work_id, group_id),
                b.c.primary_id == m.c.primary_id,
            )
            .exists(),
        )

    def page(
        self, authority: InputGroupSet, *, start_ordinal: int, limit: int = 100
    ) -> InputGroupPage:
        if (
            type(start_ordinal) is not int
            or start_ordinal < 0
            or type(limit) is not int
            or not 1 <= limit <= 100
        ):
            raise ValueError("group page requires a nonnegative ordinal and bounded limit")
        g, m = self.tables["groups"], self.tables["members"]
        with self.engine.connect() as connection:
            original = connection.scalar(
                select(g.c.authority_json).where(
                    self._key(g, authority.work_id, authority.group_id),
                    g.c.phase == "complete",
                )
            )
            if original is None or InputGroupSet.model_validate_json(original) != authority:
                raise ValueError("group page has no exact sealed authority")
            count = authority.group_count + authority.association_count
            if start_ordinal > count:
                raise ValueError("group page exceeds its exact member extent")
            rows = (
                connection.execute(
                    select(m)
                    .where(
                        self._key(m, authority.work_id, authority.group_id),
                        m.c.member_ordinal >= start_ordinal,
                    )
                    .order_by(m.c.member_ordinal)
                    .limit(limit)
                )
                .mappings()
                .all()
            )
        if [item["member_ordinal"] for item in rows] != list(
            range(start_ordinal, start_ordinal + len(rows))
        ):
            raise ValueError("retained group membership has an ordinal gap")
        return InputGroupPage.model_validate(
            {
                "authority": authority,
                "start_ordinal": str(start_ordinal),
                "complete": start_ordinal + len(rows) == count,
                "members": tuple(
                    InputGroupMember(
                        primary_id=item["primary_id"], associated_id=item["associated_id"] or None
                    )
                    for item in rows
                ),
            }
        )

    def candidate(self, authority: InputGroupSet, primary_id: str) -> ArtifactSelectionRef | None:
        """Build one exact candidate with separately paged association membership."""
        from stove0_protocol import canonical_json_sha256

        # A sealed header is required before any candidate becomes visible.
        self.page(authority, start_ordinal=0, limit=1)
        gm = self.tables["members"]
        with self.engine.connect() as connection:
            exists = connection.scalar(
                select(gm.c.primary_id).where(
                    self._key(gm, authority.work_id, authority.group_id),
                    gm.c.primary_id == primary_id,
                    gm.c.associated_id == "",
                    gm.c.member_ordinal.is_not(None),
                )
            )
        if exists is None:
            raise ValueError("candidate does not name an unblocked sealed primary group")
        binding: dict[str, JsonValue] = {
            "format": "stove0-group-candidate-builder/v1",
            "groups": authority.model_dump(mode="json"),
            "primary_id": primary_id,
        }
        key = canonical_json_sha256(binding)
        builders = self.state.metadata_selections
        row = builders.ensure(key, binding=binding, source={"after_ordinal": -1})
        if row["state"] != "collecting":
            return builders.seal_step(key)
        source = json.loads(row["source_json"])
        c, m = (
            self.state.compiled_planning.tables["classification"],
            self.state.compiled_planning.members,
        )
        subject_id = case((gm.c.associated_id == "", gm.c.primary_id), else_=gm.c.associated_id)
        with self.engine.connect() as connection:
            rows = (
                connection.execute(
                    select(m.c.document_json, c.c.role, gm.c.member_ordinal)
                    .select_from(
                        gm.join(
                            c,
                            and_(
                                c.c.work_id == self.state.planning_key(authority.work_id),
                                c.c.subject_id == subject_id,
                            ),
                        ).join(
                            m,
                            and_(
                                m.c.selection_sha256 == authority.inventory.selection_sha256,
                                m.c.artifact_id == c.c.subject_id,
                            ),
                        )
                    )
                    .where(
                        self._key(gm, authority.work_id, authority.group_id),
                        gm.c.primary_id == primary_id,
                        gm.c.member_ordinal > source["after_ordinal"],
                    )
                    .order_by(gm.c.member_ordinal)
                    .limit(101)
                )
                .mappings()
                .all()
            )
        page = rows[:100]
        if any(item["role"] is None for item in page):
            raise ValueError("group candidate contains an unclassified member")
        subjects = tuple(
            WorkArtifactSubject.model_validate_json(item["document_json"]).model_copy(
                update={"role": item["role"]}
            )
            for item in page
        )
        builders.append(
            key,
            expected_revision=row["revision"],
            subjects=subjects,
            source={
                "after_ordinal": page[-1]["member_ordinal"] if page else source["after_ordinal"]
            },
            complete=len(rows) <= 100,
        )
        return None
