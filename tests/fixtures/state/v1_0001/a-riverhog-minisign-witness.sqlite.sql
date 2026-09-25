-- Exact Minisign witness v1 baseline conformance fixture.
BEGIN TRANSACTION;
CREATE TABLE observations (
	generation INTEGER NOT NULL,
	collection_id INTEGER NOT NULL,
	revision TEXT NOT NULL,
	document BLOB NOT NULL,
	statement_digest TEXT,
	departed INTEGER NOT NULL,
	departure_cause TEXT,
	PRIMARY KEY (generation, collection_id)
);
CREATE TABLE progress (
	id INTEGER NOT NULL,
	generation INTEGER NOT NULL,
	serial INTEGER NOT NULL,
	phase TEXT NOT NULL,
	source_identity TEXT,
	authorization_view_identity TEXT,
	cursor TEXT,
	through_revision TEXT NOT NULL,
	last_collection_id INTEGER,
	reset_reason TEXT,
	PRIMARY KEY (id)
);
CREATE TABLE statements (
	digest TEXT NOT NULL,
	payload BLOB NOT NULL,
	state TEXT NOT NULL,
	attempts INTEGER NOT NULL,
	due INTEGER,
	error TEXT,
	signature BLOB,
	key_identity TEXT,
	PRIMARY KEY (digest)
);
CREATE INDEX ix_statements_due ON statements (state, due, digest);
CREATE TABLE state_schema_revision (version_num VARCHAR(32) NOT NULL, CONSTRAINT state_schema_revision_pkc PRIMARY KEY (version_num));
INSERT INTO state_schema_revision VALUES('v1_0001');
COMMIT;
