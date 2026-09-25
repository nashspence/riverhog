-- Exact Opentimestamps witness v1 baseline conformance fixture.
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
CREATE TABLE proof_history (
	digest TEXT NOT NULL,
	revision INTEGER NOT NULL,
	proof BLOB NOT NULL,
	proof_digest TEXT NOT NULL,
	recorded_at INTEGER NOT NULL,
	PRIMARY KEY (digest, revision)
);
CREATE TABLE statements (
	digest TEXT NOT NULL,
	payload BLOB NOT NULL,
	job_state BLOB NOT NULL,
	job_version INTEGER NOT NULL,
	due INTEGER,
	PRIMARY KEY (digest)
);
CREATE INDEX ix_statements_due ON statements (due, digest);
CREATE TABLE state_schema_revision (version_num VARCHAR(32) NOT NULL, CONSTRAINT state_schema_revision_pkc PRIMARY KEY (version_num));
INSERT INTO state_schema_revision VALUES('v1_0001');
COMMIT;
