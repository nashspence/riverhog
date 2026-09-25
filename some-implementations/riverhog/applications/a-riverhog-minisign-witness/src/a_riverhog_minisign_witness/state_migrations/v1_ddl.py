"""Immutable Minisign witness SQLite v1 baseline."""

# ruff: noqa: E501

SQLITE_DDL: tuple[str, ...] = (
    "CREATE TABLE observations (\n\tgeneration INTEGER NOT NULL, \n\tcollection_id INTEGER NOT NULL, \n\trevision TEXT NOT NULL, \n\tdocument BLOB NOT NULL, \n\tstatement_digest TEXT, \n\tdeparted INTEGER NOT NULL, \n\tdeparture_cause TEXT, \n\tPRIMARY KEY (generation, collection_id)\n)",
    "CREATE TABLE progress (\n\tid INTEGER NOT NULL, \n\tgeneration INTEGER NOT NULL, \n\tserial INTEGER NOT NULL, \n\tphase TEXT NOT NULL, \n\tsource_identity TEXT, \n\tauthorization_view_identity TEXT, \n\tcursor TEXT, \n\tthrough_revision TEXT NOT NULL, \n\tlast_collection_id INTEGER, \n\treset_reason TEXT, \n\tPRIMARY KEY (id)\n)",
    "CREATE TABLE statements (\n\tdigest TEXT NOT NULL, \n\tpayload BLOB NOT NULL, \n\tstate TEXT NOT NULL, \n\tattempts INTEGER NOT NULL, \n\tdue INTEGER, \n\terror TEXT, \n\tsignature BLOB, \n\tkey_identity TEXT, \n\tPRIMARY KEY (digest)\n)",
    "CREATE INDEX ix_statements_due ON statements (state, due, digest)",
)
