-- NON-AUTHORITATIVE #869 reference. Not an applied migration.
-- Reconcile with the Riverhog catalog baseline; never run at startup.

CREATE TABLE upload_copy_operations (
	collection_id BIGINT NOT NULL,
	initiated_by_app TEXT NOT NULL,
	initiated_by_key_id TEXT NOT NULL,
	base_identity VARCHAR(64) NOT NULL,
	creation_identity VARCHAR(64) NOT NULL,
	choices_json TEXT NOT NULL,
	tags_json TEXT NOT NULL,
	event_context_json TEXT NOT NULL,
	state VARCHAR(16) NOT NULL,
	accepted_at TEXT NOT NULL,
	published_at TEXT,
	canceled_at TEXT,
	PRIMARY KEY (collection_id),
	CONSTRAINT ck_upload_copy_operation_state CHECK (state IN ('accepted','published','canceled')),
	CONSTRAINT ck_upload_copy_operation_times CHECK ((state = 'accepted' AND published_at IS NULL AND canceled_at IS NULL) OR (state = 'published' AND published_at IS NOT NULL AND canceled_at IS NULL) OR (state = 'canceled' AND published_at IS NULL AND canceled_at IS NOT NULL))
);

CREATE TABLE upload_copy_intents (
	collection_id BIGINT NOT NULL,
	destination_store TEXT NOT NULL,
	state VARCHAR(16) NOT NULL,
	attempts INTEGER NOT NULL,
	next_attempt_at TEXT,
	failure_code TEXT,
	finished_at TEXT,
	job_receipt_json TEXT,
	PRIMARY KEY (collection_id, destination_store),
	CONSTRAINT ck_upload_copy_intent_state CHECK (state IN ('accepted','pending','handed_off','failed','canceled')),
	CONSTRAINT ck_upload_copy_intent_attempts CHECK (attempts >= 0),
	CONSTRAINT ck_upload_copy_intent_receipt CHECK ((state = 'handed_off' AND job_receipt_json IS NOT NULL) OR (state <> 'handed_off' AND job_receipt_json IS NULL)),
	CONSTRAINT ck_upload_copy_intent_times CHECK ((state IN ('handed_off','failed','canceled') AND finished_at IS NOT NULL AND next_attempt_at IS NULL) OR (state = 'accepted' AND finished_at IS NULL AND next_attempt_at IS NULL) OR (state = 'pending' AND finished_at IS NULL AND next_attempt_at IS NOT NULL)),
	CONSTRAINT ck_upload_copy_intent_failure CHECK (state <> 'failed' OR failure_code IS NOT NULL),
	FOREIGN KEY(collection_id) REFERENCES upload_copy_operations (collection_id) ON DELETE RESTRICT
);

CREATE INDEX ix_upload_copy_intents_due ON upload_copy_intents (state, next_attempt_at, collection_id);
