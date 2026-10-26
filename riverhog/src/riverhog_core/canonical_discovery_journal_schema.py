"""Private normalized framing and child-registration schema, without runtime owners."""

from __future__ import annotations

import sqlite3


def initialize_archive_framing(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("archive framing initialization requires no active transaction")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_archive_framing (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            worker_id TEXT NOT NULL, algorithm TEXT NOT NULL,
            source_bytes TEXT NOT NULL, source_sha256 TEXT NOT NULL,
            version TEXT NOT NULL, byte_offset TEXT NOT NULL, frame_start TEXT,
            frame_count TEXT NOT NULL, phase TEXT NOT NULL, failure TEXT,
            PRIMARY KEY(build_id,admission_sha256,journal_id),
            CHECK(phase IN ('scanning','complete','deferred')),
            CHECK((phase='deferred') = (failure IS NOT NULL))
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_archive_frames (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            ordinal TEXT NOT NULL, byte_offset TEXT NOT NULL, json_bytes TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,journal_id,ordinal)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_archive_pages (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            ordinal TEXT NOT NULL, content BLOB NOT NULL, sha256 TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,journal_id,ordinal),
            CHECK(length(content) > 0 AND length(content) <= 65536)
        ) WITHOUT ROWID;
    """)


def initialize_journal_registration(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("journal registration initialization requires no active transaction")
    db.execute("""
        CREATE TABLE IF NOT EXISTS discovery_journal_children (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            worker_id TEXT NOT NULL, directory_id TEXT NOT NULL, binding_json TEXT NOT NULL,
            confirmed INTEGER NOT NULL CHECK(confirmed IN (0,1)),
            PRIMARY KEY(build_id,admission_sha256,journal_id),
            UNIQUE(build_id,admission_sha256,directory_id)
        ) WITHOUT ROWID
    """).close()


def initialize_journal_epochs(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("journal epoch initialization requires no active transaction")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_journal_epochs (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            worker_id TEXT NOT NULL, directory_id TEXT NOT NULL, algorithm TEXT NOT NULL,
            binding_json TEXT NOT NULL, version TEXT NOT NULL, next_frame TEXT NOT NULL,
            byte_offset TEXT NOT NULL, epoch TEXT NOT NULL, phase TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,journal_id),
            CHECK(phase IN ('scanning','complete','deferred'))
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_journal_epoch_frames (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            ordinal TEXT NOT NULL, byte_offset TEXT NOT NULL, json_bytes TEXT NOT NULL,
            epoch TEXT NOT NULL, entry_kind TEXT NOT NULL, json_sha256 TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,journal_id,ordinal)
        ) WITHOUT ROWID;
    """)


def initialize_journal_topology(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("journal topology initialization requires no active transaction")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_journal_topology (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            worker_id TEXT NOT NULL, directory_id TEXT NOT NULL, algorithm TEXT NOT NULL,
            binding_json TEXT NOT NULL, epoch_version TEXT NOT NULL,
            version TEXT NOT NULL, next_frame TEXT NOT NULL, phase TEXT NOT NULL,
            cursor_json TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,journal_id),
            CHECK(phase IN ('scanning','complete','deferred'))
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_journal_topology_facts (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            ordinal TEXT NOT NULL, frame_ordinal TEXT NOT NULL, epoch TEXT NOT NULL,
            array_kind TEXT NOT NULL, item_ordinal TEXT NOT NULL, byte_offset TEXT NOT NULL,
            json_bytes TEXT NOT NULL, json_sha256 TEXT NOT NULL,
            assertion_id TEXT NOT NULL, kind TEXT NOT NULL, source_id TEXT, target_id TEXT,
            PRIMARY KEY(build_id,admission_sha256,journal_id,ordinal),
            UNIQUE(build_id,admission_sha256,journal_id,frame_ordinal,array_kind,item_ordinal),
            CHECK(array_kind IN ('retracts','relations')),
            CHECK(kind IN ('retraction','derivation','specialization'))
        ) WITHOUT ROWID;
    """)


def initialize_journal_union(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("journal union initialization requires no active transaction")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_journal_union (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            worker_id TEXT NOT NULL, directory_id TEXT NOT NULL, algorithm TEXT NOT NULL,
            binding_json TEXT NOT NULL, topology_version TEXT NOT NULL,
            version TEXT NOT NULL, frame TEXT NOT NULL, state TEXT NOT NULL,
            epoch TEXT NOT NULL, phase TEXT NOT NULL, consumed TEXT NOT NULL,
            seed_after TEXT, active_count TEXT NOT NULL, seeded_count TEXT NOT NULL,
            last_source_sha256 TEXT, last_arc_epoch TEXT, last_arc_kind TEXT,
            last_arc_source TEXT, last_arc_target TEXT,
            PRIMARY KEY(build_id,admission_sha256,journal_id),
            CHECK(state IN ('building','complete','deferred')),
            CHECK(phase IN ('frame','retracts','seed','add')),
            CHECK((last_arc_epoch IS NULL)=(last_arc_kind IS NULL)
              AND (last_arc_epoch IS NULL)=(last_arc_source IS NULL)
              AND (last_arc_epoch IS NULL)=(last_arc_target IS NULL))
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_journal_active_edges (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            assertion_id TEXT NOT NULL, kind TEXT NOT NULL, source_id TEXT NOT NULL,
            target_id TEXT NOT NULL, frame TEXT NOT NULL, item TEXT NOT NULL,
            active INTEGER NOT NULL CHECK(active IN (0,1)),
            PRIMARY KEY(build_id,admission_sha256,journal_id,assertion_id)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS discovery_journal_active_seed
            ON discovery_journal_active_edges
            (build_id,admission_sha256,journal_id,active,assertion_id);
        CREATE TABLE IF NOT EXISTS discovery_journal_union_graphs (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            epoch TEXT NOT NULL, kind TEXT NOT NULL, vertices TEXT NOT NULL, arcs TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('building','complete')),
            PRIMARY KEY(build_id,admission_sha256,journal_id,epoch,kind)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_journal_union_arcs (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            epoch TEXT NOT NULL, kind TEXT NOT NULL,
            source_id TEXT NOT NULL, target_id TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,journal_id,epoch,kind,source_id,target_id)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_journal_union_vertices (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            epoch TEXT NOT NULL, kind TEXT NOT NULL, vertex_id TEXT NOT NULL, degree TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,journal_id,epoch,kind,vertex_id)
        ) WITHOUT ROWID;
    """)


def initialize_journal_rank(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("journal rank initialization requires no active transaction")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_journal_rank (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            worker_id TEXT NOT NULL, directory_id TEXT NOT NULL, algorithm TEXT NOT NULL,
            binding_json TEXT NOT NULL, union_version TEXT NOT NULL,
            version TEXT NOT NULL, state TEXT NOT NULL, epoch TEXT NOT NULL, kind TEXT NOT NULL,
            phase TEXT NOT NULL, seed_after TEXT, active TEXT, edge_after TEXT,
            vertices TEXT NOT NULL, arcs TEXT NOT NULL, seeded TEXT NOT NULL,
            settled TEXT NOT NULL, visited TEXT NOT NULL, ready TEXT NOT NULL,
            last_vertex_epoch TEXT, last_vertex_kind TEXT, last_vertex_id TEXT,
            last_vertex_degree TEXT, last_vertex_rank TEXT, last_vertex_ready INTEGER,
            last_arc_epoch TEXT, last_arc_kind TEXT, last_arc_source TEXT, last_arc_target TEXT,
            last_graph_epoch TEXT, last_graph_kind TEXT, last_graph_state TEXT,
            last_graph_vertices TEXT, last_graph_arcs TEXT, last_graph_settled TEXT,
            last_graph_visited TEXT,
            PRIMARY KEY(build_id,admission_sha256,journal_id),
            CHECK(state IN ('building','complete','deferred')),
            CHECK(kind IN ('derivation','specialization')),
            CHECK(phase IN ('graph','seed','pop','edges'))
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_journal_rank_vertices (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            epoch TEXT NOT NULL, kind TEXT NOT NULL, vertex_id TEXT NOT NULL,
            degree TEXT NOT NULL, rank TEXT, ready INTEGER NOT NULL CHECK(ready IN (0,1)),
            PRIMARY KEY(build_id,admission_sha256,journal_id,epoch,kind,vertex_id)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS discovery_journal_rank_ready
            ON discovery_journal_rank_vertices
            (build_id,admission_sha256,journal_id,epoch,kind,ready,vertex_id);
        CREATE TABLE IF NOT EXISTS discovery_journal_rank_graphs (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            epoch TEXT NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL,
            vertices TEXT NOT NULL, arcs TEXT NOT NULL,
            settled TEXT NOT NULL, visited TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,journal_id,epoch,kind),
            CHECK(state IN ('acyclic','cyclic'))
        ) WITHOUT ROWID;
    """)
