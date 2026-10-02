-- Exact current Riverhog CLI local-state v1 baseline conformance fixture.
BEGIN TRANSACTION;
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
	history_binding_json TEXT NOT NULL,
	history_extent TEXT NOT NULL,
	history_proof_json TEXT NOT NULL,
	PRIMARY KEY (collection_id, artifact_id),
	CONSTRAINT ck_desired_artifacts_bytes CHECK (bytes >= 0),
	CONSTRAINT ck_desired_artifacts_primary_bytes CHECK (primary_bytes > 0),
	CONSTRAINT ck_desired_artifacts_history_extent CHECK (history_extent = 'complete-retained-history'),
	CONSTRAINT uq_desired_artifact_destination UNIQUE (collection_id, destination_json),
	FOREIGN KEY(collection_id) REFERENCES desired_collections (collection_id) ON DELETE CASCADE
);
INSERT INTO "desired_artifacts" VALUES(1,'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',12,'5cb72f90e968922d30557d0af8f719d21f61792becaa87eb32477767d739dc0b','["notes", "fixture.txt"]','declared-hint','["notes", "fixture.txt"]','{"artifact_id":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","delivery_association_id":"urn:uuid:a310fbe1-da5d-43dd-9e4b-730d1fd2695d","journal":{"journal_id":"urn:uuid:ee862567-ad52-429e-aa69-dfe03a5b9a84","prefix_bytes":"11326","prefix_sha256":"b73570de255d654265e5969135bac7c225db02f289f5280cfd5d4770ba8eaa6e","through":{"entry_id":"urn:uuid:930b39ce-013d-4ae8-abd9-e906c0dca821","json_sha256":"8ddeea07e52ec77e64a952268749684ea0213264cb1013d47b92a9057819083b","sequence":"1"}}}',11326,'b73570de255d654265e5969135bac7c225db02f289f5280cfd5d4770ba8eaa6e','{"artifact_id":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","bytes":"12","history_bytes":"1049","history_sha256":"c1ba2b61e174330e0158275e3903fe00f2a12b4d9d0b8729f07b264041a5947c","sha256":"5cb72f90e968922d30557d0af8f719d21f61792becaa87eb32477767d739dc0b"}','complete-retained-history','{"archive_root_base64":"eyJhcmNoaXZlX2dlbmVyYXRpb24iOiJhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhIiwiYXJ0aWZhY3Rfc2V0Ijp7ImJ5dGVzIjoiMTIiLCJjb3VudCI6IjEiLCJzaGEyNTYiOiJlNzExYWM2ZjE3ZTYzYjQyZjI4NWQxZmFjMzllNzQ0MmRhNzI3NDc4MjZmZDZhNjBmZTFmOGE1MTM4NjZjNjRhIn0sImZvcm1hdCI6ImNvbGxlY3Rpb24tYXJjaGl2ZS1tYW5pZmVzdC92MSIsInByb3ZlbmFuY2UiOnsiaWRlbnRpdHkiOiIwYmJlZDQ4NmU3ZWMzNTIzYjQxMzExNDFjOTk5MzU3ODBmMzIyM2NjOGM0Y2FkYmU4MWJjOTBiNzBiMTgyMWU4Iiwicm9vdCI6eyJpZCI6InByb3ZlbmFuY2Utcm9vdCIsImtpbmQiOiJwcm92ZW5hbmNlLXJvb3QiLCJwYXRoIjoicHJvdmVuYW5jZS9yb290Lmpzb24uYWdlIiwicGxhaW50ZXh0X2J5dGVzIjoiNTIwIiwic2hhMjU2IjoiMGJiZWQ0ODZlN2VjMzUyM2I0MTMxMTQxYzk5OTM1NzgwZjMyMjNjYzhjNGNhZGJlODFiYzkwYjcwYjE4MjFlOCIsInN0b3JlZF9ieXRlcyI6IjcwMSIsInN0b3JlZF9zaGEyNTYiOiIzOGRkMDIwNTg3NWEwZDgwMmZjOGIwNzg0OTcwZGU5NTJmZjk0NGQzOTI5ZDVjODRlMjc1YjhkZjIzOGVlOTFmIn19LCJzdG9yYWdlX3Byb2ZpbGUiOnsiZW5jcnlwdGlvbiI6ImFnZS12MS1zY3J5cHQiLCJwYWNrX2luZGV4Ijoicml2ZXJob2ctcGFjay1pbmRleC92MSIsInBhcnRfZGlnZXN0Ijoic2hhMjU2Iiwic2VsZWN0aXZlX3JlYWQiOiJhZ2UtY2h1bmstcmFuZ2UvdjEifSwidm9sdW1lX3NlcXVlbmNlIjp7InNoYTI1NiI6ImI1N2ZiMWMyNjYzZDQ5MzZmNzNmOGVjMGViM2YzYjBjZTUwNDc0OTEzMjk5MTkyOWJiYzFkMmY5YWUzMjkyZjMifX0=","binding":{"artifact_id":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","bytes":"12","history_bytes":"1049","history_sha256":"c1ba2b61e174330e0158275e3903fe00f2a12b4d9d0b8729f07b264041a5947c","sha256":"5cb72f90e968922d30557d0af8f719d21f61792becaa87eb32477767d739dc0b"},"collection_id":"1","format":"riverhog-source-member-history-proof/v1","index":"0","provenance_root_base64":"eyJhcmNoaXZlX2dlbmVyYXRpb24iOiJhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhIiwiYXJ0aWZhY3Rfc2V0X3NoYTI1NiI6ImU3MTFhYzZmMTdlNjNiNDJmMjg1ZDFmYWMzOWU3NDQyZGE3Mjc0NzgyNmZkNmE2MGZlMWY4YTUxMzg2NmM2NGEiLCJiaW5kaW5nX2NvdW50IjoiMSIsImJpbmRpbmdfdHJlZV9zaGEyNTYiOiJkNGVkNmQwN2JiZTNlOTNhNmViYTFiY2U2YWRlMTQyZDc5ZmZlYjU4Mzg3MzkyZTRlODA1NGQ4YzQyZTUwMjJlIiwiZGVsaXZlcnlfY29udGV4dF9pZCI6InVybjp1dWlkOjVhOGJhNThjLTA4ZGYtNGVlOC04YjRhLTUwNzVmNWU0MGUxNCIsImZvcm1hdCI6InJpdmVyaG9nLWFyY2hpdmUtcHJvdmVuYW5jZS1yb290L3YxIiwiam91cm5hbF9jb3VudCI6IjEiLCJ2b2x1bWVfc2VxdWVuY2UiOnsic2hhMjU2IjoiZGI1MWNjOTA1YmY5MDU2MzQxYjNjMzgwZWZiMmE2ZWE3MWUyOThlNWI0NmUwMjIxMzU2MDI4ODlkOTY1YmZhYyJ9fQ==","siblings":[],"source_identity":"1010101010101010101010101010101010101010101010101010101010101010"}');
CREATE TABLE desired_collection_tags (
	collection_id INTEGER NOT NULL,
	tag TEXT NOT NULL,
	PRIMARY KEY (collection_id, tag),
	FOREIGN KEY(collection_id) REFERENCES desired_collections (collection_id) ON DELETE CASCADE
);
INSERT INTO "desired_collection_tags" VALUES(1,'fixture');
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
INSERT INTO "desired_collections" VALUES(1,'c7384671d6f867039e08b60e6e360edb9b4c0ce5ed4f06bc72b496017f546f81','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','e711ac6f17e63b42f285d1fac39e7442da72747826fd6a60fe1f8a513866c64a','0bbed486e7ec3523b4131141c99935780f3223cc8c4cadbe81bc90b70b1821e8','2026-01-01T00:00:00.000000000Z','declared-hints','{"case_sensitive":true,"component_bytes":255,"relative_path_bytes":4096,"unicode_equivalence":"exact","windows_names":false}',0);
CREATE TABLE desired_history_objects (
	collection_id INTEGER NOT NULL,
	object_id TEXT NOT NULL,
	bytes INTEGER NOT NULL,
	sha256 TEXT NOT NULL,
	PRIMARY KEY (collection_id, object_id),
	CONSTRAINT ck_desired_history_objects_bytes CHECK (bytes > 0),
	FOREIGN KEY(collection_id) REFERENCES desired_collections (collection_id) ON DELETE CASCADE
);
INSERT INTO "desired_history_objects" VALUES(1,'provenance-history-c1ba2b61e174330e0158275e3903fe00f2a12b4d9d0b8729f07b264041a5947c',1049,'c1ba2b61e174330e0158275e3903fe00f2a12b4d9d0b8729f07b264041a5947c');
INSERT INTO "desired_history_objects" VALUES(1,'provenance-record-page-60ce097411059356579da85646db4ccc18ed88202ace4adea84a54b09b181607-0000000000000000000000000000000000000000000000000000000000000000',744,'58d43c19a7f8e081a8d8820045cd95d568f2df12fdd1d7cd321608865f26dd0c');
INSERT INTO "desired_history_objects" VALUES(1,'provenance-record-page-60ce097411059356579da85646db4ccc18ed88202ace4adea84a54b09b181607-0000000000000000000000000000000000000000000000000000000000000001',293,'5df47973fea41310c06e9e22604455a1d29c6c39a4ecaf0882d9787dc974eb9c');
INSERT INTO "desired_history_objects" VALUES(1,'provenance-record-page-cf0a9bc593ce411974f90bb3f15a03bfb6c03916e2b710745d754c9463c8d882-0000000000000000000000000000000000000000000000000000000000000000',295,'0c81fd788ea56c46100945d9d262b324f6d19096403a020a8f5012745a40f74c');
CREATE TABLE desired_journals (
	collection_id INTEGER NOT NULL,
	journal_id TEXT NOT NULL,
	bytes INTEGER NOT NULL,
	sha256 TEXT NOT NULL,
	PRIMARY KEY (collection_id, journal_id),
	CONSTRAINT ck_desired_journals_bytes CHECK (bytes > 0),
	FOREIGN KEY(collection_id) REFERENCES desired_collections (collection_id) ON DELETE CASCADE
);
INSERT INTO "desired_journals" VALUES(1,'urn:uuid:ee862567-ad52-429e-aa69-dfe03a5b9a84',11326,'b73570de255d654265e5969135bac7c225db02f289f5280cfd5d4770ba8eaa6e');
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
INSERT INTO "retrieval_job_artifacts" VALUES('fixture-retrieval',0,1,'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',12,'5cb72f90e968922d30557d0af8f719d21f61792becaa87eb32477767d739dc0b');
CREATE TABLE retrieval_jobs (
	id TEXT NOT NULL,
	state TEXT NOT NULL,
	updated_at TEXT DEFAULT CURRENT_TIMESTAMP NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT ck_retrieval_jobs_state CHECK (state IN ('requested', 'ready', 'completed', 'expired', 'failed', 'canceled'))
);
INSERT INTO "retrieval_jobs" VALUES('fixture-retrieval','ready','2026-01-01T00:00:00.000000000Z');
CREATE TABLE settings (
	"key" TEXT NOT NULL,
	value TEXT NOT NULL,
	PRIMARY KEY ("key")
);
INSERT INTO "settings" VALUES('catalog_reconcile_after','1');
CREATE TABLE state_schema_revision (version_num VARCHAR(32) NOT NULL, CONSTRAINT state_schema_revision_pkc PRIMARY KEY (version_num));
INSERT INTO "state_schema_revision" VALUES('v1_0001');
COMMIT;
