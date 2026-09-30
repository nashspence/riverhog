-- Exact current Riverhog CLI local-state v1 baseline conformance fixture.
BEGIN TRANSACTION;
CREATE TABLE desired_collections (
	collection_id INTEGER NOT NULL, 
	archive_root_sha256 TEXT NOT NULL, 
	inventory_identity TEXT NOT NULL, 
	artifact_set_identity TEXT NOT NULL, 
	provenance_identity TEXT NOT NULL, 
	created_at TEXT NOT NULL, 
	layout_mode TEXT NOT NULL, 
	rules_json TEXT NOT NULL, 
	remote_unavailable INTEGER DEFAULT 0 NOT NULL, 
	PRIMARY KEY (collection_id), 
	CONSTRAINT ck_desired_collections_id CHECK (collection_id > 0), 
	CONSTRAINT ck_desired_collections_layout CHECK (layout_mode IN ('declared-hints', 'id-layout')), 
	CONSTRAINT ck_desired_collections_unavailable CHECK (remote_unavailable IN (0, 1))
);
CREATE TABLE retrieval_jobs (
	id TEXT NOT NULL, 
	state TEXT NOT NULL, 
	updated_at TEXT DEFAULT CURRENT_TIMESTAMP NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_retrieval_jobs_state CHECK (state IN ('requested', 'ready', 'completed', 'expired', 'failed', 'canceled'))
);
CREATE TABLE settings (
	"key" TEXT NOT NULL, 
	value TEXT NOT NULL, 
	PRIMARY KEY ("key")
);
CREATE TABLE desired_artifacts (
	collection_id INTEGER NOT NULL, 
	artifact_id TEXT NOT NULL, 
	bytes INTEGER NOT NULL, 
	sha256 TEXT NOT NULL, 
	destination_json TEXT NOT NULL, 
	reason TEXT NOT NULL, 
	hint_json TEXT, 
	binding_json TEXT NOT NULL, 
	primary_bytes INTEGER NOT NULL, 
	primary_sha256 TEXT NOT NULL, 
	PRIMARY KEY (collection_id, artifact_id), 
	CONSTRAINT ck_desired_artifacts_bytes CHECK (bytes >= 0), 
	CONSTRAINT ck_desired_artifacts_primary_bytes CHECK (primary_bytes > 0), 
	CONSTRAINT uq_desired_artifact_destination UNIQUE (collection_id, destination_json), 
	FOREIGN KEY(collection_id) REFERENCES desired_collections (collection_id) ON DELETE CASCADE
);
CREATE TABLE desired_collection_tags (
	collection_id INTEGER NOT NULL, 
	tag TEXT NOT NULL, 
	PRIMARY KEY (collection_id, tag), 
	FOREIGN KEY(collection_id) REFERENCES desired_collections (collection_id) ON DELETE CASCADE
);
CREATE TABLE desired_journals (
	collection_id INTEGER NOT NULL, 
	journal_id TEXT NOT NULL, 
	bytes INTEGER NOT NULL, 
	sha256 TEXT NOT NULL, 
	PRIMARY KEY (collection_id, journal_id), 
	CONSTRAINT ck_desired_journals_bytes CHECK (bytes > 0), 
	FOREIGN KEY(collection_id) REFERENCES desired_collections (collection_id) ON DELETE CASCADE
);
CREATE TABLE retrieval_job_artifacts (
	retrieval_job_id TEXT NOT NULL, 
	ordinal INTEGER NOT NULL, 
	collection_id INTEGER NOT NULL, 
	artifact_id TEXT NOT NULL, 
	bytes INTEGER NOT NULL, 
	sha256 TEXT NOT NULL, 
	PRIMARY KEY (retrieval_job_id, ordinal), 
	CONSTRAINT ck_retrieval_job_artifacts_ordinal CHECK (ordinal >= 0), 
	CONSTRAINT ck_retrieval_job_artifacts_collection CHECK (collection_id > 0), 
	CONSTRAINT ck_retrieval_job_artifacts_bytes CHECK (bytes >= 0), 
	CONSTRAINT uq_retrieval_job_artifacts_member UNIQUE (retrieval_job_id, collection_id, artifact_id), 
	FOREIGN KEY(retrieval_job_id) REFERENCES retrieval_jobs (id) ON DELETE CASCADE
);
INSERT INTO settings VALUES('catalog_reconcile_after', '1');
INSERT INTO desired_collections VALUES(1, 'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc', '9999999999999999999999999999999999999999999999999999999999999999', '2026-01-01T00:00:00.000000000Z', 'declared-hints', '{"case_sensitive": true, "component_bytes": 255, "relative_path_bytes": 4096, "unicode_equivalence": "exact", "windows_names": false}', 0);
INSERT INTO desired_collection_tags VALUES(1, 'fixture');
INSERT INTO desired_journals VALUES(1, 'urn:uuid:11111111-1111-4111-8111-111111111111', 128, 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff');
INSERT INTO desired_artifacts VALUES(1, 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 12, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '["notes", "fixture.txt"]', 'declared-hint', '["notes", "fixture.txt"]', '{"artifact_id": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "delivery_association_id": "urn:uuid:22222222-2222-4222-8222-222222222222", "journal": {"journal_id": "urn:uuid:11111111-1111-4111-8111-111111111111", "prefix_bytes": "128", "prefix_sha256": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}}', 128, 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff');
INSERT INTO retrieval_jobs VALUES('fixture-retrieval', 'ready', '2026-01-01T00:00:00.000000000Z');
INSERT INTO retrieval_job_artifacts VALUES('fixture-retrieval', 0, 1, 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 12, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
CREATE TABLE state_schema_revision (version_num VARCHAR(32) NOT NULL, CONSTRAINT state_schema_revision_pkc PRIMARY KEY (version_num));
INSERT INTO state_schema_revision VALUES('v1_0001');
COMMIT;
