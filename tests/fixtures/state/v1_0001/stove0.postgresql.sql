-- Exact current Stove0 PostgreSQL v1 baseline conformance fixture.

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;

CREATE TABLE stove0_state_schema_revision (
    version_num VARCHAR(32) NOT NULL,
    CONSTRAINT stove0_state_schema_revision_pkc PRIMARY KEY (version_num)
);

CREATE TABLE stove0_departure_policies (
	policy_id VARCHAR(160) NOT NULL,
	policy_revision INTEGER NOT NULL,
	policy_sha256 VARCHAR(64) NOT NULL,
	phase VARCHAR(32) NOT NULL,
	generation VARCHAR(64) NOT NULL,
	source_identity VARCHAR(64),
	authorization_view_identity VARCHAR(64),
	cursor VARCHAR(4096),
	through_revision VARCHAR(19) NOT NULL,
	updated_at VARCHAR(40) NOT NULL,
	PRIMARY KEY (policy_id),
	CONSTRAINT ck_stove0_departure_policy_revision CHECK (policy_revision >= 1),
	CONSTRAINT ck_stove0_departure_policy_phase CHECK (phase IN ('new','baseline','following','reset_required')),
	CONSTRAINT ck_stove0_departure_policies_policy_sha256_hex CHECK (length(policy_sha256) = 64 AND lower(policy_sha256) = policy_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(policy_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_departure_policies_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_departure_policies_source_identity_hex CHECK (source_identity IS NULL OR length(source_identity) = 64 AND lower(source_identity) = source_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_departure_policies_authorization_view_identity_hex CHECK (authorization_view_identity IS NULL OR length(authorization_view_identity) = 64 AND lower(authorization_view_identity) = authorization_view_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authorization_view_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_departure_policies_phase ON stove0_departure_policies (phase, policy_id);

CREATE TABLE stove0_departure_seen (
	policy_id VARCHAR(160) NOT NULL,
	generation VARCHAR(64) NOT NULL,
	collection_id BIGINT NOT NULL,
	revision VARCHAR(19) NOT NULL,
	operation VARCHAR(9) NOT NULL,
	authority_sha256 VARCHAR(64) NOT NULL,
	matched BOOLEAN NOT NULL,
	document_bytes BIGINT,
	document_json TEXT,
	PRIMARY KEY (policy_id, generation, collection_id),
	CONSTRAINT ck_stove0_departure_seen_collection CHECK (collection_id >= 1),
	CONSTRAINT ck_stove0_departure_seen_operation CHECK (operation IN ('upsert','departure')),
	CONSTRAINT ck_stove0_departure_seen_document CHECK (document_bytes IS NULL AND document_json IS NULL OR document_bytes >= 0 AND document_json IS NOT NULL),
	CONSTRAINT ck_stove0_departure_seen_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_departure_seen_authority_sha256_hex CHECK (length(authority_sha256) = 64 AND lower(authority_sha256) = authority_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authority_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_departure_seen_collection ON stove0_departure_seen (collection_id, policy_id);

CREATE TABLE stove0_departure_effects (
	departure_id VARCHAR(64) NOT NULL,
	policy_id VARCHAR(160) NOT NULL,
	state VARCHAR(16) NOT NULL,
	document_bytes BIGINT NOT NULL,
	document_json TEXT NOT NULL,
	receipt_sha256 VARCHAR(64),
	receipt_bytes BIGINT,
	receipt_json TEXT,
	attempt_count INTEGER NOT NULL,
	next_attempt_at VARCHAR(40),
	failure TEXT,
	created_at VARCHAR(40) NOT NULL,
	updated_at VARCHAR(40) NOT NULL,
	PRIMARY KEY (departure_id),
	CONSTRAINT ck_stove0_departure_effect_state CHECK (state IN ('pending','complete')),
	CONSTRAINT ck_stove0_departure_effect_bytes CHECK (document_bytes >= 0),
	CONSTRAINT ck_stove0_departure_effect_receipt_bytes CHECK (receipt_bytes IS NULL OR receipt_bytes >= 0),
	CONSTRAINT ck_stove0_departure_effect_attempts CHECK (attempt_count >= 0),
	CONSTRAINT ck_stove0_departure_effect_stage CHECK (state = 'complete' AND receipt_sha256 IS NOT NULL AND receipt_bytes IS NOT NULL AND receipt_json IS NOT NULL AND next_attempt_at IS NULL OR state = 'pending' AND receipt_sha256 IS NULL AND receipt_bytes IS NULL AND receipt_json IS NULL AND next_attempt_at IS NOT NULL),
	CONSTRAINT ck_stove0_departure_effects_departure_id_hex CHECK (length(departure_id) = 64 AND lower(departure_id) = departure_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(departure_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_departure_effects_receipt_sha256_hex CHECK (receipt_sha256 IS NULL OR length(receipt_sha256) = 64 AND lower(receipt_sha256) = receipt_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(receipt_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_departure_effects_policy ON stove0_departure_effects (policy_id, departure_id);

CREATE INDEX ix_stove0_departure_effects_retry ON stove0_departure_effects (state, next_attempt_at, departure_id);

CREATE TABLE stove0_admission_policies (
	policy_id VARCHAR(160) NOT NULL,
	policy_revision INTEGER NOT NULL,
	policy_sha256 VARCHAR(64) NOT NULL,
	phase VARCHAR(32) NOT NULL,
	generation VARCHAR(64) NOT NULL,
	source_identity VARCHAR(64),
	authorization_view_identity VARCHAR(64),
	cursor VARCHAR(4096),
	baseline_mode VARCHAR(16) NOT NULL,
	through_revision VARCHAR(19) NOT NULL,
	updated_at VARCHAR(40) NOT NULL,
	PRIMARY KEY (policy_id),
	CONSTRAINT ck_stove0_admission_policy_revision CHECK (policy_revision >= 1),
	CONSTRAINT ck_stove0_admission_policy_id CHECK (length(policy_id) >= 1),
	CONSTRAINT ck_stove0_admission_policy_phase CHECK (phase IN ('new','baseline','following','reset_required')),
	CONSTRAINT ck_stove0_admission_policy_baseline_mode CHECK (baseline_mode IN ('observe','backfill')),
	CONSTRAINT ck_stove0_admission_policies_policy_sha256_hex CHECK (length(policy_sha256) = 64 AND lower(policy_sha256) = policy_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(policy_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_admission_policies_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_admission_policies_source_identity_hex CHECK (source_identity IS NULL OR length(source_identity) = 64 AND lower(source_identity) = source_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_admission_policies_authorization_view_identity_hex CHECK (authorization_view_identity IS NULL OR length(authorization_view_identity) = 64 AND lower(authorization_view_identity) = authorization_view_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authorization_view_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_admission_policies_phase ON stove0_admission_policies (phase, policy_id);

CREATE TABLE stove0_admission_matches (
	policy_id VARCHAR(160) NOT NULL,
	generation VARCHAR(64) NOT NULL,
	collection_id BIGINT NOT NULL,
	matched BOOLEAN NOT NULL,
	descriptor_revision VARCHAR(19) NOT NULL,
	tag_revision BIGINT NOT NULL,
	tag_set_identity VARCHAR(64) NOT NULL,
	document_bytes BIGINT NOT NULL,
	document_json TEXT NOT NULL,
	PRIMARY KEY (policy_id, generation, collection_id),
	CONSTRAINT ck_stove0_admission_match_collection CHECK (collection_id >= 1),
	CONSTRAINT ck_stove0_admission_match_tag_revision CHECK (tag_revision >= 1),
	CONSTRAINT ck_stove0_admission_match_bytes CHECK (document_bytes >= 0),
	CONSTRAINT ck_stove0_admission_matches_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_admission_matches_tag_set_identity_hex CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_admission_matches_collection ON stove0_admission_matches (collection_id, policy_id, generation);

CREATE TABLE stove0_admission_observed_revisions (
	policy_id VARCHAR(160) NOT NULL,
	generation VARCHAR(64) NOT NULL,
	collection_id BIGINT NOT NULL,
	descriptor_revision VARCHAR(19) NOT NULL,
	operation VARCHAR(9) NOT NULL,
	authority_sha256 VARCHAR(64) NOT NULL,
	PRIMARY KEY (policy_id, generation, collection_id),
	CONSTRAINT ck_stove0_admission_observed_revision_collection CHECK (collection_id >= 1),
	CONSTRAINT ck_stove0_admission_observed_revision_operation CHECK (operation IN ('upsert','departure')),
	CONSTRAINT ck_stove0_admission_observed_revisions_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_admission_observed_revisions_authority_sha256_hex CHECK (length(authority_sha256) = 64 AND lower(authority_sha256) = authority_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authority_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_admission_observed_revisions_collection ON stove0_admission_observed_revisions (collection_id, policy_id, generation);

CREATE TABLE stove0_admission_candidates (
	admission_id VARCHAR(64) NOT NULL,
	policy_id VARCHAR(160) NOT NULL,
	state VARCHAR(32) NOT NULL,
	preview_sha256 VARCHAR(64),
	work_id VARCHAR(64),
	document_bytes BIGINT NOT NULL,
	document_json TEXT NOT NULL,
	preview_bytes BIGINT,
	preview_json TEXT,
	attempt_count INTEGER NOT NULL,
	next_attempt_at VARCHAR(40),
	failure TEXT,
	created_at VARCHAR(40) NOT NULL,
	updated_at VARCHAR(40) NOT NULL,
	PRIMARY KEY (admission_id),
	CONSTRAINT ck_stove0_admission_candidate_state CHECK (state IN ('intent','previewed','work_bound','resolved_inapplicable','resolved_failed','resolved_canceled')),
	CONSTRAINT ck_stove0_admission_candidate_bytes CHECK (document_bytes >= 0),
	CONSTRAINT ck_stove0_admission_candidate_preview_bytes CHECK (preview_bytes IS NULL OR preview_bytes >= 0),
	CONSTRAINT ck_stove0_admission_candidate_attempt_count CHECK (attempt_count >= 0),
	CONSTRAINT ck_stove0_admission_candidate_next_attempt CHECK (state IN ('work_bound','resolved_inapplicable','resolved_failed','resolved_canceled') AND next_attempt_at IS NULL OR state IN ('intent','previewed') AND next_attempt_at IS NOT NULL),
	CONSTRAINT ck_stove0_admission_candidates_admission_id_hex CHECK (length(admission_id) = 64 AND lower(admission_id) = admission_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(admission_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_admission_candidates_preview_sha256_hex CHECK (preview_sha256 IS NULL OR length(preview_sha256) = 64 AND lower(preview_sha256) = preview_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(preview_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_admission_candidates_work_id_hex CHECK (work_id IS NULL OR length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_admission_candidates_policy ON stove0_admission_candidates (policy_id, admission_id);

CREATE INDEX ix_stove0_admission_candidates_state ON stove0_admission_candidates (state, admission_id);

CREATE INDEX ix_stove0_admission_candidates_retry ON stove0_admission_candidates (state, next_attempt_at, admission_id);

CREATE INDEX ix_stove0_admission_candidates_work ON stove0_admission_candidates (work_id, admission_id);

CREATE INDEX ix_stove0_admission_candidates_created ON stove0_admission_candidates (created_at, admission_id);

CREATE INDEX ix_stove0_admission_candidates_updated ON stove0_admission_candidates (updated_at, admission_id);

CREATE INDEX ix_stove0_admission_candidates_id_trgm ON stove0_admission_candidates USING gin (admission_id gin_trgm_ops);

CREATE INDEX ix_stove0_admission_candidates_policy_trgm ON stove0_admission_candidates USING gin (policy_id gin_trgm_ops);

CREATE INDEX ix_stove0_admission_candidates_work_trgm ON stove0_admission_candidates USING gin (work_id gin_trgm_ops);

CREATE TABLE stove0_artifact_selections (
	selection_sha256 VARCHAR(64) NOT NULL, 
	artifact_count INTEGER NOT NULL, 
	total_bytes BIGINT NOT NULL, 
	state VARCHAR(16) NOT NULL, 
	PRIMARY KEY (selection_sha256), 
	CONSTRAINT ck_stove0_selection_publication_state CHECK (state IN ('building','sealed')), 
	CONSTRAINT ck_stove0_selections_id CHECK (length(selection_sha256) = 64), 
	CONSTRAINT ck_stove0_selections_count CHECK (artifact_count >= 0), 
	CONSTRAINT ck_stove0_selections_bytes CHECK (total_bytes >= 0), 
	CONSTRAINT ck_stove0_artifact_selections_selection_sha256_hex CHECK (length(selection_sha256) = 64 AND lower(selection_sha256) = selection_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(selection_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE TABLE stove0_evaluation_records (
	evaluation_id VARCHAR(64) NOT NULL,
	revision INTEGER NOT NULL,
	phase VARCHAR(32) NOT NULL,
	updated_at VARCHAR(40) NOT NULL,
	document_bytes BIGINT NOT NULL,
	document_json TEXT NOT NULL,
	PRIMARY KEY (evaluation_id),
	CONSTRAINT ck_stove0_evaluation_records_revision CHECK (revision >= 1),
	CONSTRAINT ck_stove0_evaluation_records_phase CHECK (phase IN ('planning','running','partially_complete','complete','failed','canceled')),
	CONSTRAINT ck_stove0_evaluation_records_id CHECK (length(evaluation_id) = 64),
	CONSTRAINT ck_stove0_evaluation_records_document_bytes CHECK (document_bytes >= 0),
	CONSTRAINT ck_stove0_evaluation_records_evaluation_id_hex CHECK (length(evaluation_id) = 64 AND lower(evaluation_id) = evaluation_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(evaluation_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_evaluation_records_id_trgm ON stove0_evaluation_records USING gin (evaluation_id gin_trgm_ops);

CREATE INDEX ix_stove0_evaluation_records_phase_id ON stove0_evaluation_records (phase, evaluation_id);

CREATE INDEX ix_stove0_evaluation_records_updated_id ON stove0_evaluation_records (updated_at, evaluation_id);

CREATE TABLE stove0_event_cursors (
	stream VARCHAR(160) NOT NULL,
	cursor VARCHAR(500) NOT NULL,
	revision INTEGER NOT NULL,
	updated_at VARCHAR(40) NOT NULL,
	PRIMARY KEY (stream),
	CONSTRAINT ck_stove0_event_cursors_revision CHECK (revision >= 1)
);

CREATE TABLE stove0_lifecycle_events (
	sequence SERIAL NOT NULL,
	created_at VARCHAR(40) NOT NULL,
	event_bytes BIGINT NOT NULL,
	event_json TEXT NOT NULL,
	PRIMARY KEY (sequence),
	CONSTRAINT ck_stove0_lifecycle_events_event_bytes CHECK (event_bytes >= 0)
);

CREATE INDEX ix_stove0_lifecycle_events_created_at ON stove0_lifecycle_events (created_at);

CREATE TABLE stove0_artifact_selection_members (
	selection_sha256 VARCHAR(64) NOT NULL, 
	artifact_id VARCHAR(160) NOT NULL, 
	member_identity_sha256 VARCHAR(64) NOT NULL, 
	artifact_identity_sha256 VARCHAR(64) NOT NULL, 
	source_collection_id BIGINT NOT NULL, 
	source_artifact_id VARCHAR(64) NOT NULL, 
	artifact_order INTEGER NOT NULL, 
	continuation_sha256 VARCHAR(64) NOT NULL, 
	document_bytes BIGINT NOT NULL, 
	document_json TEXT NOT NULL, 
	PRIMARY KEY (selection_sha256, artifact_id), 
	CONSTRAINT ck_stove0_selection_members_artifact_id CHECK (length(artifact_id) >= 1), 
	CONSTRAINT ck_stove0_selection_members_identity CHECK (length(member_identity_sha256) = 64), 
	CONSTRAINT ck_stove0_selection_artifact_identity CHECK (length(artifact_identity_sha256) = 64), 
	CONSTRAINT ck_stove0_selection_members_order CHECK (artifact_order >= 0), 
	CONSTRAINT ck_stove0_selection_members_continuation CHECK (length(continuation_sha256) = 64), 
	CONSTRAINT ck_stove0_selection_members_document_bytes CHECK (document_bytes >= 0), 
	FOREIGN KEY(selection_sha256) REFERENCES stove0_artifact_selections (selection_sha256) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_artifact_selection_members_selection_sha256_hex CHECK (length(selection_sha256) = 64 AND lower(selection_sha256) = selection_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(selection_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_sha256_38c276233c60174f CHECK (length(member_identity_sha256) = 64 AND lower(member_identity_sha256) = member_identity_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(member_identity_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_sha256_e3ca5cb0afec48f3 CHECK (length(artifact_identity_sha256) = 64 AND lower(artifact_identity_sha256) = artifact_identity_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(artifact_identity_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_artifact_selection_members_source_artifact_id_hex CHECK (length(source_artifact_id) = 64 AND lower(source_artifact_id) = source_artifact_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_artifact_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_artifact_selection_members_continuation_sha256_hex CHECK (length(continuation_sha256) = 64 AND lower(continuation_sha256) = continuation_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(continuation_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE UNIQUE INDEX ix_stove0_selection_members_continuation ON stove0_artifact_selection_members (selection_sha256, continuation_sha256);

CREATE UNIQUE INDEX ix_stove0_selection_members_order ON stove0_artifact_selection_members (selection_sha256, artifact_order);

CREATE TABLE stove0_evaluation_children (
	evaluation_id VARCHAR(64) NOT NULL,
	work_id VARCHAR(64) NOT NULL,
	PRIMARY KEY (evaluation_id, work_id),
	FOREIGN KEY(evaluation_id) REFERENCES stove0_evaluation_records (evaluation_id) ON DELETE CASCADE,
	CONSTRAINT ck_stove0_evaluation_children_evaluation_id_hex CHECK (length(evaluation_id) = 64 AND lower(evaluation_id) = evaluation_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(evaluation_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_evaluation_children_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_evaluation_children_work ON stove0_evaluation_children (work_id, evaluation_id);

CREATE TABLE stove0_work_records (
	work_id VARCHAR(64) NOT NULL, 
	recipe_sha256 VARCHAR(64) NOT NULL, 
	revision INTEGER NOT NULL, 
	phase VARCHAR(48) NOT NULL, 
	updated_at VARCHAR(40) NOT NULL, 
	document_bytes BIGINT NOT NULL, 
	document_json TEXT NOT NULL, 
	contact_at VARCHAR(40) NOT NULL, 
	contact_failures INTEGER NOT NULL, 
	claim_renew_at VARCHAR(40), 
	PRIMARY KEY (work_id), 
	CONSTRAINT ck_stove0_work_records_revision CHECK (revision >= 1), 
	CONSTRAINT ck_stove0_work_records_phase CHECK (phase IN ('eligible','claimed','observing','planning','target_preflight','queued','executing','output_finalizing','verifying','settled','source_collection_retirement_pending','coordinating','no_output_pending','abandon_pending','complete','no_action','inapplicable','failed','canceled')), 
	CONSTRAINT ck_stove0_work_records_id CHECK (length(work_id) = 64), 
	CONSTRAINT ck_stove0_work_records_document_bytes CHECK (document_bytes >= 0), 
	CONSTRAINT ck_stove0_work_contact_failures CHECK (contact_failures >= 0), 
	CONSTRAINT ck_stove0_work_records_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_work_records_recipe_sha256_hex CHECK (length(recipe_sha256) = 64 AND lower(recipe_sha256) = recipe_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(recipe_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE TABLE stove0_target_input_dispositions (
	work_id VARCHAR(64) NOT NULL,
	job_id VARCHAR(64) NOT NULL,
	input_id VARCHAR(160) NOT NULL,
	status VARCHAR(32) NOT NULL,
	code VARCHAR(160),
	message VARCHAR(1000),
	PRIMARY KEY (work_id, job_id, input_id),
	CONSTRAINT ck_stove0_target_dispositions_id CHECK (length(input_id) >= 1),
	CONSTRAINT ck_stove0_target_dispositions_status CHECK (status IN ('not-carried-forward','omitted','preserved','rejected','transformed')),
	FOREIGN KEY(work_id) REFERENCES stove0_work_records (work_id) ON DELETE CASCADE,
	CONSTRAINT ck_stove0_target_input_dispositions_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_target_input_dispositions_job_id_hex CHECK (length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE TABLE stove0_target_outputs (
	work_id VARCHAR(64) NOT NULL,
	job_id VARCHAR(64) NOT NULL,
	output_id VARCHAR(160) NOT NULL,
	artifact_id VARCHAR(64) NOT NULL,
	document_bytes BIGINT NOT NULL,
	document_json TEXT NOT NULL,
	PRIMARY KEY (work_id, job_id, output_id),
	CONSTRAINT ck_stove0_target_outputs_id CHECK (length(output_id) >= 1),
	CONSTRAINT ck_stove0_target_outputs_artifact_id CHECK (length(artifact_id) = 64),
	CONSTRAINT ck_stove0_target_outputs_document_bytes CHECK (document_bytes >= 0),
	FOREIGN KEY(work_id) REFERENCES stove0_work_records (work_id) ON DELETE CASCADE,
	CONSTRAINT ck_stove0_target_outputs_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_target_outputs_job_id_hex CHECK (length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_target_outputs_artifact_id_hex CHECK (length(artifact_id) = 64 AND lower(artifact_id) = artifact_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(artifact_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE UNIQUE INDEX uq_stove0_target_outputs_artifact_id ON stove0_target_outputs (work_id, job_id, artifact_id);

CREATE TABLE stove0_target_source_edges (
	work_id VARCHAR(64) NOT NULL,
	job_id VARCHAR(64) NOT NULL,
	output_id VARCHAR(160) NOT NULL,
	input_id VARCHAR(160) NOT NULL,
	PRIMARY KEY (work_id, job_id, output_id, input_id),
	CONSTRAINT ck_stove0_target_source_edges_output CHECK (length(output_id) >= 1),
	CONSTRAINT ck_stove0_target_source_edges_input CHECK (length(input_id) >= 1),
	FOREIGN KEY(work_id) REFERENCES stove0_work_records (work_id) ON DELETE CASCADE,
	CONSTRAINT ck_stove0_target_source_edges_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_target_source_edges_job_id_hex CHECK (length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_target_source_edges_input ON stove0_target_source_edges (work_id, job_id, input_id, output_id);

CREATE TABLE stove0_target_production_seals (
	work_id VARCHAR(64) NOT NULL,
	job_id VARCHAR(64) NOT NULL,
	revision INTEGER NOT NULL,
	state VARCHAR(32) NOT NULL,
	updated_at VARCHAR(40) NOT NULL,
	document_bytes BIGINT NOT NULL,
	document_json TEXT NOT NULL,
	PRIMARY KEY (work_id, job_id),
	CONSTRAINT ck_stove0_target_production_seals_revision CHECK (revision >= 1),
	CONSTRAINT ck_stove0_target_production_seals_state CHECK (state IN ('receiving','sealing','sealed','failed')),
	CONSTRAINT ck_stove0_target_production_seals_document_bytes CHECK (document_bytes >= 0),
	FOREIGN KEY(work_id) REFERENCES stove0_work_records (work_id) ON DELETE CASCADE,
	CONSTRAINT ck_stove0_target_production_seals_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_target_production_seals_job_id_hex CHECK (length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_target_production_seals_state_updated ON stove0_target_production_seals (state, updated_at, work_id, job_id);

CREATE TABLE stove0_target_settlement_seals (
	work_id VARCHAR(64) NOT NULL,
	job_id VARCHAR(64) NOT NULL,
	revision INTEGER NOT NULL,
	state VARCHAR(32) NOT NULL,
	updated_at VARCHAR(40) NOT NULL,
	document_bytes BIGINT NOT NULL,
	document_json TEXT NOT NULL,
	PRIMARY KEY (work_id, job_id),
	CONSTRAINT ck_stove0_target_settlement_seals_revision CHECK (revision >= 1),
	CONSTRAINT ck_stove0_target_settlement_seals_state CHECK (state IN ('binding','sealed','failed')),
	CONSTRAINT ck_stove0_target_settlement_seals_document_bytes CHECK (document_bytes >= 0),
	FOREIGN KEY(work_id) REFERENCES stove0_work_records (work_id) ON DELETE CASCADE,
	CONSTRAINT ck_stove0_target_settlement_seals_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_target_settlement_seals_job_id_hex CHECK (length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_target_settlement_seals_state_updated ON stove0_target_settlement_seals (state, updated_at, work_id, job_id);

CREATE TABLE stove0_work_evaluations (
	work_id VARCHAR(64) NOT NULL,
	evaluation_id VARCHAR(64) NOT NULL,
	PRIMARY KEY (work_id, evaluation_id),
	FOREIGN KEY(work_id) REFERENCES stove0_work_records (work_id) ON DELETE CASCADE,
	CONSTRAINT ck_stove0_work_evaluations_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_work_evaluations_evaluation_id_hex CHECK (length(evaluation_id) = 64 AND lower(evaluation_id) = evaluation_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(evaluation_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_work_evaluations_evaluation ON stove0_work_evaluations (evaluation_id, work_id);

CREATE TABLE stove0_work_relations (
	work_id VARCHAR(64) NOT NULL,
	related_work_id VARCHAR(64) NOT NULL,
	PRIMARY KEY (work_id, related_work_id),
	CONSTRAINT ck_stove0_work_relations_distinct CHECK (work_id <> related_work_id),
	FOREIGN KEY(work_id) REFERENCES stove0_work_records (work_id) ON DELETE CASCADE,
	CONSTRAINT ck_stove0_work_relations_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_work_relations_related_work_id_hex CHECK (length(related_work_id) = 64 AND lower(related_work_id) = related_work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(related_work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_work_relations_related ON stove0_work_relations (related_work_id, work_id);

CREATE TABLE stove0_work_selection_references (
	work_id VARCHAR(64) NOT NULL,
	selection_sha256 VARCHAR(64) NOT NULL,
	PRIMARY KEY (work_id, selection_sha256),
	FOREIGN KEY(work_id) REFERENCES stove0_work_records (work_id) ON DELETE CASCADE,
	CONSTRAINT ck_stove0_work_selection_references_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''),
	CONSTRAINT ck_stove0_work_selection_references_selection_sha256_hex CHECK (length(selection_sha256) = 64 AND lower(selection_sha256) = selection_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(selection_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_work_selection_references_selection ON stove0_work_selection_references (selection_sha256, work_id);

CREATE INDEX ix_stove0_work_contacts ON stove0_work_records (phase, contact_at, work_id);

CREATE INDEX ix_stove0_work_maintenance ON stove0_work_records (claim_renew_at, work_id);

CREATE INDEX ix_stove0_work_records_id_trgm ON stove0_work_records USING gin (work_id gin_trgm_ops);

CREATE INDEX ix_stove0_work_records_phase_work_id ON stove0_work_records (phase, work_id);

CREATE INDEX ix_stove0_work_records_updated_work_id ON stove0_work_records (updated_at, work_id);

CREATE TABLE stove0_observation_deliveries (
	owner_kind VARCHAR(16) NOT NULL, 
	owner_id VARCHAR(64) NOT NULL, 
	job_id VARCHAR(64) NOT NULL, 
	claim_id VARCHAR(160) NOT NULL, 
	fence BIGINT NOT NULL, 
	state VARCHAR(16) NOT NULL, 
	document_bytes BIGINT NOT NULL, 
	document_json TEXT NOT NULL, 
	updated_at VARCHAR(40) NOT NULL, 
	PRIMARY KEY (owner_kind, owner_id, job_id), 
	CONSTRAINT ck_stove0_observation_owner_kind CHECK (owner_kind IN ('work','preview')), 
	CONSTRAINT ck_stove0_observation_delivery_state CHECK (state IN ('unsubmitted','queued','running','interrupted','canceling','completed')), 
	CONSTRAINT ck_stove0_observation_delivery_bytes CHECK (document_bytes >= 0), 
	CONSTRAINT ck_stove0_observation_delivery_fence CHECK (fence >= 1), 
	CONSTRAINT ck_stove0_observation_deliveries_owner_id_hex CHECK (length(owner_id) = 64 AND lower(owner_id) = owner_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(owner_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_observation_deliveries_job_id_hex CHECK (length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_observation_deliveries_pending ON stove0_observation_deliveries (owner_kind, owner_id, state, job_id);

CREATE TABLE stove0_planning_jobs (
	job_id VARCHAR(64) NOT NULL, 
	work_id VARCHAR(64) NOT NULL, 
	recipe_sha256 VARCHAR(64) NOT NULL, 
	revision BIGINT NOT NULL, 
	phase VARCHAR(16) NOT NULL, 
	claim_renew_at VARCHAR(40), 
	contact_at VARCHAR(40) NOT NULL, 
	document_bytes BIGINT NOT NULL, 
	document_json TEXT NOT NULL, 
	updated_at VARCHAR(40) NOT NULL, 
	PRIMARY KEY (job_id), 
	CONSTRAINT ck_stove0_planning_revision CHECK (revision >= 1), 
	CONSTRAINT ck_stove0_planning_document_bytes CHECK (document_bytes >= 0), 
	CONSTRAINT ck_stove0_planning_phase CHECK (phase IN ('queued','observing','planning','preflight','canceling','abandoning','admitting','completed')), 
	CONSTRAINT ck_stove0_planning_jobs_job_id_hex CHECK (length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_planning_jobs_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_planning_jobs_recipe_sha256_hex CHECK (length(recipe_sha256) = 64 AND lower(recipe_sha256) = recipe_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(recipe_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_planning_contact ON stove0_planning_jobs (phase, contact_at, job_id);

CREATE INDEX ix_stove0_planning_maintenance ON stove0_planning_jobs (claim_renew_at, job_id);

CREATE TABLE stove0_planning_contexts (
	context_id VARCHAR(64) NOT NULL, 
	owner_kind VARCHAR(16) NOT NULL, 
	owner_id VARCHAR(64) NOT NULL, 
	updated_at TEXT NOT NULL, 
	PRIMARY KEY (context_id), 
	CONSTRAINT ck_stove0_planning_context_owner CHECK (owner_kind IN ('work','preview')), 
	CONSTRAINT ck_stove0_planning_contexts_context_id_hex CHECK (length(context_id) = 64 AND lower(context_id) = context_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(context_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_planning_contexts_owner_id_hex CHECK (length(owner_id) = 64 AND lower(owner_id) = owner_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(owner_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE TABLE stove0_observation_questions (
	question_sha256 VARCHAR(129) NOT NULL, 
	context_id VARCHAR(64), 
	work_id VARCHAR(129) NOT NULL, 
	task_id TEXT NOT NULL, 
	scope_sha256 VARCHAR(64) NOT NULL, 
	question_json TEXT NOT NULL, 
	state VARCHAR(16) NOT NULL, 
	result_ordinal BIGINT NOT NULL, 
	hash_state TEXT NOT NULL, 
	evidence_set_sha256 VARCHAR(64), 
	evidence_set_json TEXT, 
	PRIMARY KEY (question_sha256), 
	CONSTRAINT uq_stove0_observation_task UNIQUE (work_id, task_id), 
	CONSTRAINT ck_stove0_observation_question_state CHECK (state IN ('collecting','sealing','complete')), 
	CONSTRAINT ck_stove0_observation_result_ordinal CHECK (result_ordinal >= 0), 
	FOREIGN KEY(context_id) REFERENCES stove0_planning_contexts (context_id) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_observation_questions_context_id_hex CHECK (context_id IS NULL OR length(context_id) = 64 AND lower(context_id) = context_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(context_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_observation_questions_scope_sha256_hex CHECK (length(scope_sha256) = 64 AND lower(scope_sha256) = scope_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(scope_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_observation_questions_evidence_set_sha256_hex CHECK (evidence_set_sha256 IS NULL OR length(evidence_set_sha256) = 64 AND lower(evidence_set_sha256) = evidence_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(evidence_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_observation_questions_pending ON stove0_observation_questions (state, work_id, task_id);

CREATE TABLE stove0_accepted_views (
	view_key VARCHAR(129) NOT NULL, 
	question_sha256 VARCHAR(129) NOT NULL, 
	view_id TEXT NOT NULL, 
	scope_sha256 VARCHAR(64) NOT NULL, 
	state VARCHAR(16) NOT NULL, 
	record_count BIGINT NOT NULL, 
	after_request_id TEXT NOT NULL, 
	after_record_ordinal BIGINT NOT NULL, 
	hash_state TEXT NOT NULL, 
	view_sha256 VARCHAR(129), 
	view_json TEXT, 
	PRIMARY KEY (view_key), 
	CONSTRAINT ck_stove0_accepted_view_state CHECK (state IN ('sealing','complete')), 
	CONSTRAINT ck_stove0_accepted_view_count CHECK (record_count >= 0), 
	FOREIGN KEY(question_sha256) REFERENCES stove0_observation_questions (question_sha256) ON DELETE CASCADE, 
	UNIQUE (view_sha256), 
	CONSTRAINT ck_stove0_accepted_views_scope_sha256_hex CHECK (length(scope_sha256) = 64 AND lower(scope_sha256) = scope_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(scope_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE TABLE stove0_observation_results (
	request_id VARCHAR(129) NOT NULL, 
	question_sha256 VARCHAR(129) NOT NULL, 
	request_json TEXT NOT NULL, 
	descriptor_json TEXT NOT NULL, 
	evidence_json TEXT, 
	ref_json TEXT, 
	result_ordinal BIGINT, 
	PRIMARY KEY (request_id), 
	CONSTRAINT uq_stove0_observation_result_ordinal UNIQUE (question_sha256, result_ordinal), 
	CONSTRAINT ck_stove0_observation_physical_ordinal CHECK (result_ordinal IS NULL OR result_ordinal >= 0), 
	FOREIGN KEY(question_sha256) REFERENCES stove0_observation_questions (question_sha256) ON DELETE CASCADE
);

CREATE INDEX ix_stove0_observation_results_task ON stove0_observation_results (question_sha256, request_id);

CREATE TABLE stove0_accepted_view_members (
	view_key VARCHAR(129) NOT NULL, 
	record_ordinal BIGINT NOT NULL, 
	record_json TEXT NOT NULL, 
	PRIMARY KEY (view_key, record_ordinal), 
	CONSTRAINT ck_stove0_accepted_view_member_ordinal CHECK (record_ordinal >= 0), 
	FOREIGN KEY(view_key) REFERENCES stove0_accepted_views (view_key) ON DELETE CASCADE
);

CREATE TABLE stove0_observation_coverage (
	question_sha256 VARCHAR(129) NOT NULL, 
	subject_id TEXT NOT NULL, 
	request_id VARCHAR(129) NOT NULL, 
	PRIMARY KEY (question_sha256, subject_id), 
	FOREIGN KEY(question_sha256) REFERENCES stove0_observation_questions (question_sha256) ON DELETE CASCADE, 
	FOREIGN KEY(request_id) REFERENCES stove0_observation_results (request_id) ON DELETE CASCADE
);

CREATE TABLE stove0_observation_view_records (
	question_sha256 VARCHAR(129) NOT NULL, 
	view_id TEXT NOT NULL, 
	request_id VARCHAR(129) NOT NULL, 
	record_ordinal BIGINT NOT NULL, 
	subject_id TEXT, 
	kind VARCHAR(16) NOT NULL, 
	primary_id TEXT, 
	associated_id TEXT, 
	record_json TEXT NOT NULL, 
	PRIMARY KEY (question_sha256, view_id, request_id, record_ordinal), 
	CONSTRAINT ck_stove0_observation_view_record_ordinal CHECK (record_ordinal >= 0), 
	FOREIGN KEY(question_sha256) REFERENCES stove0_observation_questions (question_sha256) ON DELETE CASCADE, 
	FOREIGN KEY(request_id) REFERENCES stove0_observation_results (request_id) ON DELETE CASCADE
);

CREATE INDEX ix_stove0_observation_view_relation ON stove0_observation_view_records (question_sha256, view_id, associated_id, primary_id);

CREATE INDEX ix_stove0_observation_view_subject ON stove0_observation_view_records (question_sha256, view_id, subject_id, kind);

CREATE TABLE stove0_selection_builders (
	builder_id VARCHAR(129) NOT NULL, 
	context_id VARCHAR(64), 
	revision BIGINT NOT NULL, 
	binding_json TEXT NOT NULL, 
	source_json TEXT NOT NULL, 
	state VARCHAR(16) NOT NULL, 
	member_count BIGINT NOT NULL, 
	total_bytes BIGINT NOT NULL, 
	hashed_count BIGINT NOT NULL, 
	published_count BIGINT NOT NULL, 
	after_collection_id BIGINT NOT NULL, 
	after_artifact_id TEXT NOT NULL, 
	hash_state TEXT NOT NULL, 
	selection_sha256 VARCHAR(64), 
	PRIMARY KEY (builder_id), 
	CONSTRAINT ck_stove0_selection_builder_revision CHECK (revision >= 1), 
	CONSTRAINT ck_stove0_selection_builder_state CHECK (state IN ('collecting','sealing','publishing','complete')), 
	CONSTRAINT ck_stove0_selection_builder_counts CHECK (member_count >= 0 AND total_bytes >= 0 AND hashed_count >= 0 AND published_count >= 0), 
	FOREIGN KEY(context_id) REFERENCES stove0_planning_contexts (context_id) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_selection_builders_context_id_hex CHECK (context_id IS NULL OR length(context_id) = 64 AND lower(context_id) = context_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(context_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_selection_builders_selection_sha256_hex CHECK (selection_sha256 IS NULL OR length(selection_sha256) = 64 AND lower(selection_sha256) = selection_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(selection_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_selection_builders_pending ON stove0_selection_builders (state, builder_id);

CREATE TABLE stove0_selection_builder_members (
	builder_id VARCHAR(129) NOT NULL, 
	subject_id TEXT NOT NULL, 
	collection_id BIGINT NOT NULL, 
	artifact_id VARCHAR(64) NOT NULL, 
	document_json TEXT NOT NULL, 
	artifact_order BIGINT, 
	PRIMARY KEY (builder_id, subject_id), 
	CONSTRAINT uq_stove0_selection_builder_instance UNIQUE (builder_id, collection_id, artifact_id), 
	FOREIGN KEY(builder_id) REFERENCES stove0_selection_builders (builder_id) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_selection_builder_members_artifact_id_hex CHECK (length(artifact_id) = 64 AND lower(artifact_id) = artifact_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(artifact_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_selection_builder_member_order ON stove0_selection_builder_members (builder_id, collection_id, artifact_id);

CREATE INDEX ix_stove0_selection_builder_publish ON stove0_selection_builder_members (builder_id, artifact_order);

CREATE TABLE stove0_recipe_definitions (
	recipe_sha256 VARCHAR(64) NOT NULL, 
	recipe_json TEXT NOT NULL, 
	closure_json TEXT NOT NULL, 
	updated_at TEXT NOT NULL, 
	PRIMARY KEY (recipe_sha256), 
	CONSTRAINT ck_stove0_recipe_definitions_recipe_sha256_hex CHECK (length(recipe_sha256) = 64 AND lower(recipe_sha256) = recipe_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(recipe_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE TABLE stove0_compiled_planning (
	work_id VARCHAR(129) NOT NULL, 
	context_id VARCHAR(64), 
	recipe_sha256 VARCHAR(64) NOT NULL, 
	revision BIGINT NOT NULL, 
	phase VARCHAR(16) NOT NULL, 
	scope_sha256 VARCHAR(64), 
	task_ordinal BIGINT NOT NULL, 
	dispatch_turn BIGINT NOT NULL, 
	input_ordinal BIGINT NOT NULL, 
	decision_ordinal BIGINT NOT NULL, 
	decision_json TEXT, 
	classified_count BIGINT NOT NULL, 
	PRIMARY KEY (work_id), 
	CONSTRAINT ck_stove0_compiled_planning_counters CHECK (revision >= 1 AND task_ordinal >= 0 AND dispatch_turn >= 0 AND input_ordinal >= 0 AND decision_ordinal >= 0 AND classified_count >= 0), 
	CONSTRAINT ck_stove0_compiled_planning_phase CHECK (phase IN ('inventory','observations','classified','decisions','complete')), 
	FOREIGN KEY(context_id) REFERENCES stove0_planning_contexts (context_id) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_compiled_planning_context_id_hex CHECK (context_id IS NULL OR length(context_id) = 64 AND lower(context_id) = context_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(context_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_compiled_planning_recipe_sha256_hex CHECK (length(recipe_sha256) = 64 AND lower(recipe_sha256) = recipe_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(recipe_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_compiled_planning_scope_sha256_hex CHECK (scope_sha256 IS NULL OR length(scope_sha256) = 64 AND lower(scope_sha256) = scope_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(scope_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_compiled_planning_pending ON stove0_compiled_planning (phase, work_id);

CREATE TABLE stove0_compiled_classifications (
	work_id VARCHAR(129) NOT NULL, 
	subject_id TEXT NOT NULL, 
	role TEXT, 
	PRIMARY KEY (work_id, subject_id), 
	FOREIGN KEY(work_id) REFERENCES stove0_compiled_planning (work_id) ON DELETE CASCADE
);

CREATE INDEX ix_stove0_compiled_classification_roles ON stove0_compiled_classifications (work_id, role, subject_id);

CREATE TABLE stove0_compiled_task_ports (
	work_id VARCHAR(129) NOT NULL, 
	task_id TEXT NOT NULL, 
	port_id TEXT NOT NULL, 
	selection_sha256 VARCHAR(64) NOT NULL, 
	reference_json TEXT NOT NULL, 
	PRIMARY KEY (work_id, task_id, port_id), 
	FOREIGN KEY(work_id) REFERENCES stove0_compiled_planning (work_id) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_compiled_task_ports_selection_sha256_hex CHECK (length(selection_sha256) = 64 AND lower(selection_sha256) = selection_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(selection_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE TABLE stove0_compiled_fact_evaluations (
	evaluation_key VARCHAR(129) NOT NULL, 
	work_id VARCHAR(129) NOT NULL, 
	binding_json TEXT NOT NULL, 
	scope_sha256 VARCHAR(64) NOT NULL, 
	revision BIGINT NOT NULL, 
	state VARCHAR(16) NOT NULL, 
	continuation VARCHAR(64), 
	record_ordinal BIGINT NOT NULL, 
	positive BOOLEAN NOT NULL, 
	negative BOOLEAN NOT NULL, 
	indeterminate BOOLEAN NOT NULL, 
	truth VARCHAR(16), 
	result_json TEXT, 
	PRIMARY KEY (evaluation_key), 
	CONSTRAINT ck_stove0_fact_evaluation_counters CHECK (revision >= 1 AND record_ordinal >= 0), 
	CONSTRAINT ck_stove0_fact_evaluation_state CHECK (state IN ('collecting','complete')), 
	CONSTRAINT ck_stove0_fact_evaluation_truth CHECK (truth IS NULL OR truth IN ('true','false','indeterminate')), 
	FOREIGN KEY(work_id) REFERENCES stove0_compiled_planning (work_id) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_compiled_fact_evaluations_scope_sha256_hex CHECK (length(scope_sha256) = 64 AND lower(scope_sha256) = scope_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(scope_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_compiled_fact_evaluations_continuation_hex CHECK (continuation IS NULL OR length(continuation) = 64 AND lower(continuation) = continuation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(continuation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_fact_evaluations_scope ON stove0_compiled_fact_evaluations (scope_sha256);

CREATE INDEX ix_stove0_fact_evaluations_work ON stove0_compiled_fact_evaluations (work_id, evaluation_key);

CREATE TABLE stove0_compiled_decision_conditions (
	work_id VARCHAR(129) NOT NULL, 
	decision_ordinal BIGINT NOT NULL, 
	proof_sha256 VARCHAR(64) NOT NULL, 
	proof_json TEXT NOT NULL, 
	PRIMARY KEY (work_id, decision_ordinal), 
	CONSTRAINT ck_stove0_decision_condition_ordinal CHECK (decision_ordinal >= 0), 
	FOREIGN KEY(work_id) REFERENCES stove0_compiled_planning (work_id) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_compiled_decision_conditions_proof_sha256_hex CHECK (length(proof_sha256) = 64 AND lower(proof_sha256) = proof_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(proof_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE TABLE stove0_compiled_group_associations (
	work_id VARCHAR(129) NOT NULL, 
	group_id TEXT NOT NULL, 
	subject_id TEXT NOT NULL, 
	primary_id TEXT, 
	state VARCHAR(16) NOT NULL, 
	tier_ordinal BIGINT, 
	PRIMARY KEY (work_id, group_id, subject_id), 
	CONSTRAINT ck_stove0_compiled_association_state CHECK (state IN ('attached','unmatched','blocked')), 
	FOREIGN KEY(work_id) REFERENCES stove0_compiled_planning (work_id) ON DELETE CASCADE
);

CREATE INDEX ix_stove0_compiled_group_attachments ON stove0_compiled_group_associations (work_id, group_id, primary_id);

CREATE TABLE stove0_compiled_group_blocked (
	work_id VARCHAR(129) NOT NULL, 
	group_id TEXT NOT NULL, 
	primary_id TEXT NOT NULL, 
	PRIMARY KEY (work_id, group_id, primary_id), 
	FOREIGN KEY(work_id) REFERENCES stove0_compiled_planning (work_id) ON DELETE CASCADE
);

CREATE TABLE stove0_compiled_group_members (
	work_id VARCHAR(129) NOT NULL, 
	group_id TEXT NOT NULL, 
	primary_id TEXT NOT NULL, 
	associated_id TEXT NOT NULL, 
	member_ordinal BIGINT, 
	PRIMARY KEY (work_id, group_id, primary_id, associated_id), 
	CONSTRAINT ck_stove0_compiled_group_member_ordinal CHECK (member_ordinal IS NULL OR member_ordinal >= 0), 
	FOREIGN KEY(work_id) REFERENCES stove0_compiled_planning (work_id) ON DELETE CASCADE
);

CREATE INDEX ix_stove0_compiled_group_member_pages ON stove0_compiled_group_members (work_id, group_id, member_ordinal);

CREATE TABLE stove0_compiled_group_tiers (
	work_id VARCHAR(129) NOT NULL, 
	group_id TEXT NOT NULL, 
	tier_ordinal BIGINT NOT NULL, 
	primary_indeterminate BOOLEAN NOT NULL, 
	PRIMARY KEY (work_id, group_id, tier_ordinal), 
	FOREIGN KEY(work_id) REFERENCES stove0_compiled_planning (work_id) ON DELETE CASCADE
);

CREATE TABLE stove0_compiled_groups (
	work_id VARCHAR(129) NOT NULL, 
	group_id TEXT NOT NULL, 
	binding_json TEXT NOT NULL, 
	revision BIGINT NOT NULL, 
	phase VARCHAR(16) NOT NULL, 
	tier_ordinal BIGINT NOT NULL, 
	after_subject_id TEXT NOT NULL, 
	active_subject_id TEXT, 
	after_primary_id TEXT NOT NULL, 
	after_associated_id TEXT NOT NULL, 
	global_blocked BOOLEAN NOT NULL, 
	hashed_count BIGINT NOT NULL, 
	hashed_groups BIGINT NOT NULL, 
	hash_state TEXT NOT NULL, 
	authority_json TEXT, 
	PRIMARY KEY (work_id, group_id), 
	CONSTRAINT ck_stove0_compiled_group_counts CHECK (revision >= 1 AND tier_ordinal >= 0 AND hashed_count >= 0 AND hashed_groups >= 0), 
	CONSTRAINT ck_stove0_compiled_group_phase CHECK (phase IN ('coverage','associate','block','sealing','complete')), 
	FOREIGN KEY(work_id) REFERENCES stove0_compiled_planning (work_id) ON DELETE CASCADE
);

CREATE TABLE stove0_compiled_branch_choices (
	work_id VARCHAR(129) NOT NULL, 
	branch_id TEXT NOT NULL, 
	choice_ordinal BIGINT NOT NULL, 
	primary_id TEXT, 
	scope_sha256 VARCHAR(64) NOT NULL, 
	condition_json TEXT NOT NULL, 
	condition_sha256 VARCHAR(64) NOT NULL, 
	truth VARCHAR(16) NOT NULL, 
	PRIMARY KEY (work_id, branch_id, choice_ordinal), 
	CONSTRAINT uq_stove0_branch_primary_choice UNIQUE (work_id, branch_id, primary_id), 
	CONSTRAINT ck_stove0_branch_choice_domain CHECK (choice_ordinal >= 0 AND truth IN ('true','false')), 
	FOREIGN KEY(work_id) REFERENCES stove0_compiled_planning (work_id) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_compiled_branch_choices_scope_sha256_hex CHECK (length(scope_sha256) = 64 AND lower(scope_sha256) = scope_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(scope_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_compiled_branch_choices_condition_sha256_hex CHECK (length(condition_sha256) = 64 AND lower(condition_sha256) = condition_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(condition_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_branch_choice_scopes ON stove0_compiled_branch_choices (scope_sha256);

CREATE TABLE stove0_compiled_branch_selections (
	work_id VARCHAR(129) NOT NULL, 
	branch_id TEXT NOT NULL, 
	binding_json TEXT NOT NULL, 
	revision BIGINT NOT NULL, 
	phase VARCHAR(16) NOT NULL, 
	source_json TEXT NOT NULL, 
	scope_sha256 VARCHAR(64), 
	selection_sha256 VARCHAR(64), 
	choice_count BIGINT NOT NULL, 
	selected_count BIGINT NOT NULL, 
	hash_state TEXT NOT NULL, 
	authority_json TEXT, 
	PRIMARY KEY (work_id, branch_id), 
	CONSTRAINT ck_stove0_branch_selection_counts CHECK (revision >= 1 AND choice_count >= 0 AND selected_count >= 0 AND selected_count <= choice_count), 
	CONSTRAINT ck_stove0_branch_selection_phase CHECK (phase IN ('init','candidate','facts','copy','seal','complete')), 
	FOREIGN KEY(work_id) REFERENCES stove0_compiled_planning (work_id) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_compiled_branch_selections_scope_sha256_hex CHECK (scope_sha256 IS NULL OR length(scope_sha256) = 64 AND lower(scope_sha256) = scope_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(scope_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_compiled_branch_selections_selection_sha256_hex CHECK (selection_sha256 IS NULL OR length(selection_sha256) = 64 AND lower(selection_sha256) = selection_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(selection_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_branch_selection_results ON stove0_compiled_branch_selections (selection_sha256);

CREATE INDEX ix_stove0_branch_selection_scopes ON stove0_compiled_branch_selections (scope_sha256);

CREATE TABLE stove0_compiled_recipe_frames (
	work_id VARCHAR(129) NOT NULL, 
	owner_work_id VARCHAR(129) NOT NULL, 
	work_json TEXT NOT NULL, 
	depth BIGINT NOT NULL, 
	revision BIGINT NOT NULL, 
	checked_turn BIGINT NOT NULL, 
	phase VARCHAR(16) NOT NULL, 
	branch_ordinal BIGINT NOT NULL, 
	decision_sha256 VARCHAR(64), 
	outcome_json TEXT, 
	decision_json TEXT, 
	PRIMARY KEY (work_id), 
	CONSTRAINT ck_stove0_compiled_recipe_frame_counters CHECK (depth >= 0 AND revision >= 1 AND branch_ordinal >= 0 AND checked_turn >= 0), 
	CONSTRAINT ck_stove0_compiled_recipe_frame_phase CHECK (phase IN ('observations','decisions','selections','coverage','calls','seal','ready','no-output','inapplicable')), 
	FOREIGN KEY(work_id) REFERENCES stove0_compiled_planning (work_id) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_compiled_recipe_frames_decision_sha256_hex CHECK (decision_sha256 IS NULL OR length(decision_sha256) = 64 AND lower(decision_sha256) = decision_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(decision_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_recipe_frame_due ON stove0_compiled_recipe_frames (owner_work_id, phase, checked_turn, depth, work_id);

CREATE TABLE stove0_compiled_recipe_frame_branches (
	work_id VARCHAR(129) NOT NULL, 
	branch_id TEXT NOT NULL, 
	scope_json TEXT NOT NULL, 
	selection_sha256 VARCHAR(64) NOT NULL, 
	artifact_count BIGINT NOT NULL, 
	declared BOOLEAN NOT NULL, 
	declaration_json TEXT, 
	child_work_id VARCHAR(64), 
	PRIMARY KEY (work_id, branch_id), 
	CONSTRAINT ck_stove0_recipe_frame_branch_count CHECK (artifact_count >= 0), 
	FOREIGN KEY(work_id) REFERENCES stove0_compiled_recipe_frames (work_id) ON DELETE CASCADE, 
	CONSTRAINT ck_sha256_7c05575fc3df0ec4 CHECK (length(selection_sha256) = 64 AND lower(selection_sha256) = selection_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(selection_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_compiled_recipe_frame_branches_child_work_id_hex CHECK (child_work_id IS NULL OR length(child_work_id) = 64 AND lower(child_work_id) = child_work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(child_work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_recipe_frame_branch_selection ON stove0_compiled_recipe_frame_branches (selection_sha256);

CREATE TABLE stove0_retained_recipe_operations (
	recipe_sha256 VARCHAR(64) NOT NULL, 
	operation_sha256 VARCHAR(64) NOT NULL, 
	operation_id TEXT NOT NULL, 
	document_json TEXT NOT NULL, 
	PRIMARY KEY (recipe_sha256, operation_sha256), 
	FOREIGN KEY(recipe_sha256) REFERENCES stove0_recipe_definitions (recipe_sha256) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_retained_recipe_operations_recipe_sha256_hex CHECK (length(recipe_sha256) = 64 AND lower(recipe_sha256) = recipe_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(recipe_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_retained_recipe_operations_operation_sha256_hex CHECK (length(operation_sha256) = 64 AND lower(operation_sha256) = operation_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(operation_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_retained_operation_identity ON stove0_retained_recipe_operations (operation_id, operation_sha256, recipe_sha256);

CREATE TABLE stove0_compiled_provider_bindings (
	work_id VARCHAR(129) NOT NULL, 
	binding_id TEXT NOT NULL, 
	binding_json TEXT NOT NULL, 
	revision BIGINT NOT NULL, 
	position BIGINT NOT NULL, 
	provider_id TEXT, 
	descriptor_json TEXT, 
	phase VARCHAR(16) NOT NULL, 
	PRIMARY KEY (work_id, binding_id), 
	CONSTRAINT ck_stove0_provider_binding_counters CHECK (revision >= 1 AND position >= 0), 
	CONSTRAINT ck_stove0_provider_binding_phase CHECK (phase IN ('search','chosen','ambiguous','unavailable')), 
	FOREIGN KEY(work_id) REFERENCES stove0_compiled_recipe_frames (work_id) ON DELETE CASCADE
);

CREATE TABLE stove0_accepted_observation_inputs (
	input_sha256 VARCHAR(129) NOT NULL, 
	question_sha256 VARCHAR(129) NOT NULL, 
	scope_sha256 VARCHAR(64) NOT NULL, 
	input_json TEXT NOT NULL, 
	PRIMARY KEY (input_sha256), 
	FOREIGN KEY(question_sha256) REFERENCES stove0_observation_questions (question_sha256) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_accepted_observation_inputs_scope_sha256_hex CHECK (length(scope_sha256) = 64 AND lower(scope_sha256) = scope_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(scope_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_selection_artifact_identity ON stove0_artifact_selection_members (selection_sha256, artifact_identity_sha256);

CREATE INDEX ix_stove0_planning_context_owner ON stove0_planning_contexts (owner_kind, owner_id);

CREATE INDEX ix_stove0_planning_context_updated ON stove0_planning_contexts (updated_at, context_id);

CREATE UNIQUE INDEX ix_stove0_selection_native_order ON stove0_artifact_selection_members (selection_sha256, source_collection_id, source_artifact_id);

CREATE TABLE stove0_riverhog_transfers (
	transfer_id VARCHAR(129) NOT NULL, 
	context_id VARCHAR(64) NOT NULL, 
	revision BIGINT NOT NULL, 
	binding_json TEXT NOT NULL, 
	checkpoint_json TEXT NOT NULL, 
	PRIMARY KEY (transfer_id), 
	FOREIGN KEY(context_id) REFERENCES stove0_planning_contexts (context_id) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_riverhog_transfers_context_id_hex CHECK (length(context_id) = 64 AND lower(context_id) = context_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(context_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_riverhog_transfer_owner ON stove0_riverhog_transfers (context_id, transfer_id);

CREATE INDEX ix_stove0_preview_recipe ON stove0_planning_jobs (recipe_sha256, job_id);

CREATE INDEX ix_stove0_recipe_definition_updated ON stove0_recipe_definitions (updated_at, recipe_sha256);

CREATE INDEX ix_stove0_work_recipe ON stove0_work_records (recipe_sha256, work_id);

CREATE TABLE stove0_retained_recipe_dependencies (
	recipe_sha256 VARCHAR(64) NOT NULL, 
	dependency_sha256 VARCHAR(64) NOT NULL, 
	PRIMARY KEY (recipe_sha256, dependency_sha256), 
	FOREIGN KEY(recipe_sha256) REFERENCES stove0_recipe_definitions (recipe_sha256) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_retained_recipe_dependencies_recipe_sha256_hex CHECK (length(recipe_sha256) = 64 AND lower(recipe_sha256) = recipe_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(recipe_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''), 
	CONSTRAINT ck_stove0_retained_recipe_dependencies_dependency_sha256_hex CHECK (length(dependency_sha256) = 64 AND lower(dependency_sha256) = dependency_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(dependency_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_retained_recipe_dependency ON stove0_retained_recipe_dependencies (dependency_sha256, recipe_sha256);

CREATE TABLE stove0_compiled_tasks (
	work_id VARCHAR(129) NOT NULL, 
	task_id TEXT NOT NULL, 
	revision BIGINT NOT NULL, 
	state VARCHAR(16) NOT NULL, 
	dependencies_sha256 VARCHAR(64) NOT NULL, 
	dependency_ordinal BIGINT NOT NULL, 
	port_ordinal BIGINT NOT NULL, 
	view_ordinal BIGINT NOT NULL, 
	checked_turn BIGINT NOT NULL, 
	PRIMARY KEY (work_id, task_id), 
	CONSTRAINT ck_stove0_compiled_task_counters CHECK (revision >= 1 AND dependency_ordinal >= 0 AND port_ordinal >= 0 AND view_ordinal >= 0 AND checked_turn >= 0), 
	CONSTRAINT ck_stove0_compiled_task_state CHECK (state IN ('initializing','collecting','complete')), 
	FOREIGN KEY(work_id) REFERENCES stove0_compiled_planning (work_id) ON DELETE CASCADE, 
	CONSTRAINT ck_stove0_compiled_tasks_dependencies_sha256_hex CHECK (length(dependencies_sha256) = 64 AND lower(dependencies_sha256) = dependencies_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(dependencies_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')
);

CREATE INDEX ix_stove0_compiled_tasks_due ON stove0_compiled_tasks (work_id, state, checked_turn, task_id);

CREATE TABLE stove0_compiled_task_dependencies (
	work_id VARCHAR(129) NOT NULL, 
	task_id TEXT NOT NULL, 
	predecessor_id TEXT NOT NULL, 
	PRIMARY KEY (work_id, task_id, predecessor_id), 
	FOREIGN KEY(work_id, task_id) REFERENCES stove0_compiled_tasks (work_id, task_id) ON DELETE CASCADE
);

CREATE INDEX ix_stove0_compiled_task_predecessors ON stove0_compiled_task_dependencies (work_id, predecessor_id, task_id);

CREATE INDEX ix_stove0_recipe_frame_turn ON stove0_compiled_recipe_frames (owner_work_id, checked_turn);

CREATE INDEX ix_stove0_recipe_frame_pending_call ON stove0_compiled_recipe_frame_branches (work_id, declared, artifact_count);

CREATE INDEX ix_stove0_recipe_frame_owner ON stove0_compiled_recipe_frames (owner_work_id, work_id);

INSERT INTO stove0_state_schema_revision (version_num) VALUES ('v1_0001');
