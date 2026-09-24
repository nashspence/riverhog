"""Executable catalog/authority/job test double, NOT Riverhog's production DB.

Real SQL transactions and subprocess crashes exercise handoff.py; this small
schema does not stand in for end-to-end Riverhog or PostgreSQL qualification.
"""
from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from sqlalchemy import BigInteger, Column, ForeignKey, MetaData, String, Table, Text, create_engine, event, insert, inspect, select
from sqlalchemy.orm import Session, sessionmaker

from handoff import (
    AccessDenied, Actor, Binding, Handoff, InvalidRequest, JobReceipt,
    TerminalHandoffError, canonical, metadata as handoff_metadata,
)

NOW = "2026-09-24T00:00:00.000000000Z"
ACTOR = Actor("test-uploader", "0000000000000001")
BASE_IDENTITY = "a" * 64
md = MetaData()
keys = Table("app_keys", md,
    Column("id", Text, primary_key=True), Column("app", Text, nullable=False),
    Column("revoked_at", Text), Column("expires_at", Text))
grants = Table("app_key_access_grants", md,
    Column("key_id", Text, ForeignKey("app_keys.id"), primary_key=True),
    Column("permission", Text, primary_key=True), Column("resource", Text, primary_key=True))
collections = Table("collections", md,
    Column("id", BigInteger, primary_key=True, autoincrement=False),
    Column("owner", Text, nullable=False), Column("source", Text, nullable=False),
    Column("tags_json", Text, nullable=False), Column("published", String(1), nullable=False))
jobs = Table("archive_copy_jobs", md,
    Column("collection_id", BigInteger, ForeignKey("collections.id"), primary_key=True),
    Column("destination_store", Text, primary_key=True), Column("source_store", Text, nullable=False),
    Column("state", Text, nullable=False), Column("initiated_by_app", Text, nullable=False),
    Column("initiated_by_key_id", Text, nullable=False), Column("event_context_json", Text, nullable=False))
events = Table("lifecycle_events", md,
    Column("collection_id", BigInteger, primary_key=True, autoincrement=False),
    Column("destination_store", Text, primary_key=True), Column("kind", Text, primary_key=True),
    Column("app", Text, nullable=False), Column("key_id", Text, nullable=False), Column("context_json", Text, nullable=False))


class Registry:
    def __init__(self) -> None:
        self.stores = {name: "test-binding:" + name for name in ("archive", "copy-a", "copy-b", "new-default")}
        self.default = "archive"
        self.cache_default = True

    def default_archive(self) -> str:
        return self.default

    def default_use_cache(self, archive_store: str) -> bool:
        return self.cache_default

    def binding(self, name: str) -> Binding:
        if name not in self.stores:
            raise InvalidRequest("store is not configured")
        return Binding(name, self.stores[name])


class CurrentAuthority:
    def __init__(self, clock: Callable[[], str]) -> None:
        self.clock = clock

    def _grants(self, session: Session, actor: Actor) -> set[tuple[str, str]]:
        actor.validate()
        key = session.execute(select(keys).where(keys.c.id == actor.key_id).with_for_update()).mappings().first()
        if key is None or key["app"] != actor.app or key["revoked_at"] is not None or (key["expires_at"] is not None and key["expires_at"] <= self.clock()):
            raise AccessDenied("key is not active")
        return set(session.execute(select(grants.c.permission, grants.c.resource).where(grants.c.key_id == actor.key_id).with_for_update()).all())

    @staticmethod
    def _covers(access: set[tuple[str, str]], permission: str, resource: str) -> bool:
        return any(p in ("*", permission) and r in ("*", resource) for p, r in access)

    def require_accept(self, session: Session, actor: Actor, tags: tuple[str, ...], *, copies: bool) -> None:
        access = self._grants(session, actor)
        for permission in (("collections:create", "archives:manage") if copies else ("collections:create",)):
            if self._covers(access, permission, "*"):
                continue
            if tags and all(self._covers(access, permission, "tag:" + t) for t in tags):
                continue
            raise AccessDenied("creation scope denied")

    def require_handoff(self, session: Session, actor: Actor, collection_id: int) -> None:
        access = self._grants(session, actor)
        collection = session.execute(select(collections).where(collections.c.id == collection_id)).mappings().first()
        if collection is None or collection["published"] != "1":
            raise TerminalHandoffError("publication_missing")
        resources = (f"collection:{collection_id}", *("tag:" + t for t in json.loads(collection["tags_json"])))
        if not any(self._covers(access, "archives:manage", r) for r in resources):
            raise AccessDenied("archive-management scope denied")


class PublishedCatalog:
    def require_publication(self, session: Session, collection_id: int, actor: Actor, archive_store: str) -> None:
        row = session.execute(select(collections).where(collections.c.id == collection_id).with_for_update()).mappings().first()
        if row is None or row["published"] != "1" or row["owner"] != actor.app or row["source"] != archive_store:
            raise TerminalHandoffError("publication_missing")


class OrdinaryJobs:
    def __init__(self, fault: Callable[[str], None] = lambda phase: None) -> None:
        self.fault = fault

    def ensure(self, session: Session, collection_id: int, destination_store: str, actor: Actor, event_context: dict[str, Any], now: str) -> JobReceipt:
        existing = session.execute(select(jobs).where(jobs.c.collection_id == collection_id, jobs.c.destination_store == destination_store).with_for_update()).mappings().first()
        if existing is not None:
            return JobReceipt(collection_id, destination_store, False)
        source = session.execute(select(collections.c.source).where(collections.c.id == collection_id)).scalar_one()
        session.execute(insert(jobs).values(collection_id=collection_id, destination_store=destination_store, source_store=source,
            state="requested", initiated_by_app=actor.app, initiated_by_key_id=actor.key_id, event_context_json=canonical(event_context)))
        self.fault("after_job")
        session.execute(insert(events).values(collection_id=collection_id, destination_store=destination_store, kind="archive_copy_job.requested",
            app=actor.app, key_id=actor.key_id, context_json=canonical(event_context)))
        self.fault("after_event")
        return JobReceipt(collection_id, destination_store, True)

    def observe(self, session: Session, receipt: JobReceipt) -> dict[str, Any] | None:
        row = session.execute(select(jobs).where(jobs.c.collection_id == receipt.collection_id, jobs.c.destination_store == receipt.destination_store)).mappings().first()
        return dict(row) if row else None


class Harness:
    def __init__(self, path: str | Path, *, create: bool = True, fault: Callable[[str], None] = lambda phase: None,
                 database_url: str | None = None) -> None:
        self.path = str(path)
        self.now = NOW
        self.engine = create_engine(database_url or "sqlite:///" + self.path, connect_args={"timeout": 20} if not database_url else {})
        if self.engine.dialect.name == "sqlite":
            @event.listens_for(self.engine, "connect")
            def connect(dbapi: Any, _: Any) -> None:
                dbapi.isolation_level = None
                dbapi.execute("PRAGMA foreign_keys=ON")
                dbapi.execute("PRAGMA synchronous=FULL")

            @event.listens_for(self.engine, "begin")
            def begin(connection: Any) -> None:
                # SQLAlchemy ignores FOR UPDATE on SQLite. Make test writer
                # serialization explicit rather than claiming PG row-lock tests.
                connection.exec_driver_sql("BEGIN IMMEDIATE")
        self.sessions = sessionmaker(self.engine, expire_on_commit=False)
        self.registry = Registry()
        self.gateway = OrdinaryJobs(fault)
        self.authority = CurrentAuthority(lambda: self.now)
        self.service = Handoff(stores=self.registry, authority=self.authority, catalog=PublishedCatalog(), jobs=self.gateway, clock=lambda: self.now)
        if create:
            if database_url and inspect(self.engine).get_table_names():
                raise RuntimeError("Reference tests require an EMPTY disposable PostgreSQL database")
            md.create_all(self.engine)
            handoff_metadata.create_all(self.engine)
            with self.sessions.begin() as session:
                session.execute(insert(keys).values(id=ACTOR.key_id, app=ACTOR.app))
                session.execute(insert(grants), [{"key_id": ACTOR.key_id, "permission": p, "resource": "*"} for p in ("collections:create", "archives:manage")])

    def accept(self, session: Session, **kwargs: Any) -> dict[str, Any]:
        args: dict[str, Any] = dict(collection_id=1, actor=ACTOR, base_identity=BASE_IDENTITY, copy_to=["copy-a"], tags=["test-tag"], event_context={"trace": "test-trace"})
        args.update(kwargs)
        return self.service.accept(session, **args)

    def publish(self, session: Session, collection_id: int = 1) -> None:
        if session.execute(select(collections.c.id).where(collections.c.id == collection_id)).first() is None:
            session.execute(insert(collections).values(id=collection_id, owner=ACTOR.app, source="archive", tags_json='["test-tag"]', published="1"))
        self.service.publish(session, collection_id)

    def ready(self, **kwargs: Any) -> None:
        with self.sessions.begin() as session:
            self.accept(session, **kwargs)
        with self.sessions.begin() as session:
            self.publish(session, kwargs.get("collection_id", 1))

    def snapshot(self) -> dict[str, Any]:
        with self.sessions.begin() as session:
            return self.service.get(session, 1, ACTOR)

    def close(self) -> None:
        self.engine.dispose()
