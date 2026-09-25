"""Immutable Opentimestamps witness SQLite v1 baseline."""

# ruff: noqa: E501

SQLITE_DDL: tuple[str, ...] = (
    "CREATE TABLE observations (\n\tgeneration INTEGER NOT NULL, \n\tcollection_id INTEGER NOT NULL, \n\trevision TEXT NOT NULL, \n\tdocument BLOB NOT NULL, \n\tstatement_digest TEXT, \n\tdeparted INTEGER NOT NULL, \n\tdeparture_cause TEXT, \n\tPRIMARY KEY (generation, collection_id)\n)",
    "CREATE TABLE progress (\n\tid INTEGER NOT NULL, \n\tgeneration INTEGER NOT NULL, \n\tserial INTEGER NOT NULL, \n\tphase TEXT NOT NULL, \n\tsource_identity TEXT, \n\tauthorization_view_identity TEXT, \n\tcursor TEXT, \n\tthrough_revision TEXT NOT NULL, \n\tlast_collection_id INTEGER, \n\treset_reason TEXT, \n\tPRIMARY KEY (id)\n)",
    "CREATE TABLE proof_history (\n\tdigest TEXT NOT NULL, \n\trevision INTEGER NOT NULL, \n\tproof BLOB NOT NULL, \n\tproof_digest TEXT NOT NULL, \n\trecorded_at INTEGER NOT NULL, \n\tPRIMARY KEY (digest, revision)\n)",
    "CREATE TABLE statements (\n\tdigest TEXT NOT NULL, \n\tpayload BLOB NOT NULL, \n\tjob_state BLOB NOT NULL, \n\tjob_version INTEGER NOT NULL, \n\tdue INTEGER, \n\tPRIMARY KEY (digest)\n)",
    "CREATE INDEX ix_statements_due ON statements (due, digest)",
)
