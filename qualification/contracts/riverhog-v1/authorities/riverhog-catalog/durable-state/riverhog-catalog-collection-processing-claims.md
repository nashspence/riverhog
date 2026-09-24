# riverhog-catalog: collection_processing_claims

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-processing-claims:fc7a7686d3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-3811e19ab2"></a>

### Table: `collection_processing_claims`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-603e1ec3dc"></a>`id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-27b968f672"></a>`work_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-cc8df404f2"></a>`consumer_app` | `VARCHAR` | no | `—` | — |
| <a id="s-3d3eecca03"></a>`consumer_key_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-015771d2cc"></a>`purpose` | `VARCHAR` | no | `—` | — |
| <a id="s-1db36a3f8f"></a>`work_document_json` | `TEXT` | no | `—` | — |
| <a id="s-571a8a09e3"></a>`work_document_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-c80f98a8ee"></a>`execution_id` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-1da64bc09c"></a>`controller_evidence_json` | `TEXT` | yes | `—` | — |
| <a id="s-34a1c08489"></a>`controller_evidence_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-a8de762bf9"></a>`operation_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-12aff18420"></a>`operation_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-a386f46988"></a>`input_count` | `BIGINT` | no | `—` | — |
| <a id="s-d5074a8025"></a>`input_hash_state` | `TEXT` | yes | `—` | — |
| <a id="s-faa7d394c6"></a>`input_set_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-d10c04a483"></a>`inputs_sealed_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-090f3c5f02"></a>`artifact_count` | `BIGINT` | no | `—` | — |
| <a id="s-b26ec1d100"></a>`artifact_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-19c4dcff44"></a>`artifact_hash_state` | `TEXT` | yes | `—` | — |
| <a id="s-e1e81ea7be"></a>`artifact_set_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-e733febda4"></a>`artifacts_sealed_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-9b8542265f"></a>`outcome_count` | `BIGINT` | no | `—` | — |
| <a id="s-c7746647b5"></a>`outcome_state` | `VARCHAR` | no | `—` | — |
| <a id="s-3732c0ac26"></a>`outcome_hash_state` | `TEXT` | yes | `—` | — |
| <a id="s-ef127f8e69"></a>`outcome_validation_cursor` | `VARCHAR` | yes | `—` | — |
| <a id="s-8685b55b8d"></a>`outcome_validation_count` | `BIGINT` | no | `—` | — |
| <a id="s-4207494166"></a>`outcome_set_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-082b84b55f"></a>`outcome_failure` | `TEXT` | yes | `—` | — |
| <a id="s-1a41087a1f"></a>`outcomes_sealed_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-cf04e7c3e1"></a>`source_collection_retirement_policy` | `VARCHAR` | yes | `—` | — |
| <a id="s-7e1a3f0704"></a>`source_collection_retirement_grace_seconds` | `BIGINT` | no | `—` | — |
| <a id="s-5cbe75c5a7"></a>`plan_sealed_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-87de9877ba"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-fef47034bb"></a>`fence` | `BIGINT` | no | `—` | — |
| <a id="s-002cd6ee37"></a>`expires_at` | `VARCHAR` | no | `—` | — |
| <a id="s-0d3e0e0f4b"></a>`output_collection_id` | `BIGINT` | yes | `—` | — |
| <a id="s-4b4287da81"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-02e3489acc"></a>`updated_at` | `VARCHAR` | no | `—` | — |
| <a id="s-c3298dfbd9"></a>`settled_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-e58e18258b"></a>`abandoned_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-5a3f264075"></a>`abandonment_reason` | `TEXT` | yes | `—` | — |
| <a id="s-954b4e81ba"></a>`released_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-cd362c9a0c"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |
| <a id="s-06b97722b5"></a>`unique` | `uq_collection_processing_claims_owner_work` | `CONSTRAINT uq_collection_processing_claims_owner_work UNIQUE (consumer_app, purpose, work_id)` |
| <a id="s-df5e0b99f0"></a>`check` | `ck_collection_processing_claims_state` | `CONSTRAINT ck_collection_processing_claims_state CHECK (state IN ('active','settled','retiring','abandoned','released'))` |
| <a id="s-5511f8445a"></a>`check` | `ck_collection_processing_claims_outcome_state` | `CONSTRAINT ck_collection_processing_claims_outcome_state CHECK (outcome_state IN ('receiving','sealing','sealed','failed'))` |
| <a id="s-77366b2e95"></a>`check` | `ck_collection_processing_claims_fence` | `CONSTRAINT ck_collection_processing_claims_fence CHECK (fence >= 1)` |
| <a id="s-a6b03442de"></a>`check` | `ck_collection_processing_claims_grace` | `CONSTRAINT ck_collection_processing_claims_grace CHECK (source_collection_retirement_grace_seconds >= 0)` |
| <a id="s-13ac8be7e6"></a>`check` | `ck_collection_processing_claims_artifact_count` | `CONSTRAINT ck_collection_processing_claims_artifact_count CHECK (input_count >= 0 AND artifact_count >= 0 AND artifact_bytes >= 0 AND outcome_count >= 0 AND outcome_validation_count >= 0)` |
| <a id="s-4da4891634"></a>`check` | `ck_collection_processing_claims_id` | `CONSTRAINT ck_collection_processing_claims_id CHECK (length(id) = 64)` |
| <a id="s-ebb811eed6"></a>`check` | `ck_collection_processing_claims_work_id` | `CONSTRAINT ck_collection_processing_claims_work_id CHECK (length(work_id) = 64)` |
| <a id="s-262b01a414"></a>`check` | `ck_collection_processing_claims_document_sha256` | `CONSTRAINT ck_collection_processing_claims_document_sha256 CHECK (length(work_document_sha256) = 64)` |
| <a id="s-0523679da0"></a>`unique` | `—` | `UNIQUE (execution_id)` |
| <a id="s-87bb301d81"></a>`foreign-key` | `—` | `FOREIGN KEY(output_collection_id) REFERENCES collections (id) ON DELETE SET NULL` |
| <a id="s-c51d243459"></a>`check` | `ck_collection_processing_claims_id_hex` | `CONSTRAINT ck_collection_processing_claims_id_hex CHECK (length(id) = 64 AND lower(id) = id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-6584b84656"></a>`check` | `ck_collection_processing_claims_work_id_hex` | `CONSTRAINT ck_collection_processing_claims_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-b4b4c736c7"></a>`check` | `ck_collection_processing_claims_work_document_sha256_hex` | `CONSTRAINT ck_collection_processing_claims_work_document_sha256_hex CHECK (length(work_document_sha256) = 64 AND lower(work_document_sha256) = work_document_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_document_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-6997b15df2"></a>`check` | `ck_collection_processing_claims_execution_id_hex` | `CONSTRAINT ck_collection_processing_claims_execution_id_hex CHECK (execution_id IS NULL OR length(execution_id) = 64 AND lower(execution_id) = execution_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(execution_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-728bb3c7b4"></a>`check` | `ck_sha256_c09acb3cbfceaefd` | `CONSTRAINT ck_sha256_c09acb3cbfceaefd CHECK (controller_evidence_sha256 IS NULL OR length(controller_evidence_sha256) = 64 AND lower(controller_evidence_sha256) = controller_evidence_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(controller_evidence_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-57250e2d6d"></a>`check` | `ck_collection_processing_claims_operation_sha256_hex` | `CONSTRAINT ck_collection_processing_claims_operation_sha256_hex CHECK (operation_sha256 IS NULL OR length(operation_sha256) = 64 AND lower(operation_sha256) = operation_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(operation_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-49665841bc"></a>`check` | `ck_collection_processing_claims_input_set_sha256_hex` | `CONSTRAINT ck_collection_processing_claims_input_set_sha256_hex CHECK (input_set_sha256 IS NULL OR length(input_set_sha256) = 64 AND lower(input_set_sha256) = input_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(input_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-aa51d58e70"></a>`check` | `ck_collection_processing_claims_artifact_set_sha256_hex` | `CONSTRAINT ck_collection_processing_claims_artifact_set_sha256_hex CHECK (artifact_set_sha256 IS NULL OR length(artifact_set_sha256) = 64 AND lower(artifact_set_sha256) = artifact_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(artifact_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-3657bde370"></a>`check` | `ck_collection_processing_claims_outcome_set_sha256_hex` | `CONSTRAINT ck_collection_processing_claims_outcome_set_sha256_hex CHECK (outcome_set_sha256 IS NULL OR length(outcome_set_sha256) = 64 AND lower(outcome_set_sha256) = outcome_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(outcome_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-75bc1660b4"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/22`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6c698d881d5c7e140bfa353d5feee0a58e39dd9ed6c0d9bc7cce4cdb1c902ec6 -->

```json
{
  "columns": [
    {
      "definition": "id VARCHAR(64) NOT NULL",
      "name": "id",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "work_id VARCHAR(64) NOT NULL",
      "name": "work_id",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "consumer_app VARCHAR NOT NULL",
      "name": "consumer_app",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "consumer_key_id VARCHAR",
      "name": "consumer_key_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "purpose VARCHAR NOT NULL",
      "name": "purpose",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "work_document_json TEXT NOT NULL",
      "name": "work_document_json",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "work_document_sha256 VARCHAR(64) NOT NULL",
      "name": "work_document_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "execution_id VARCHAR(64)",
      "name": "execution_id",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "controller_evidence_json TEXT",
      "name": "controller_evidence_json",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "controller_evidence_sha256 VARCHAR(64)",
      "name": "controller_evidence_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "operation_id VARCHAR",
      "name": "operation_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "operation_sha256 VARCHAR(64)",
      "name": "operation_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "input_count BIGINT NOT NULL",
      "name": "input_count",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "input_hash_state TEXT",
      "name": "input_hash_state",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "input_set_sha256 VARCHAR(64)",
      "name": "input_set_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "inputs_sealed_at VARCHAR",
      "name": "inputs_sealed_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "artifact_count BIGINT NOT NULL",
      "name": "artifact_count",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "artifact_bytes BIGINT NOT NULL",
      "name": "artifact_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "artifact_hash_state TEXT",
      "name": "artifact_hash_state",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "artifact_set_sha256 VARCHAR(64)",
      "name": "artifact_set_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "artifacts_sealed_at VARCHAR",
      "name": "artifacts_sealed_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "outcome_count BIGINT NOT NULL",
      "name": "outcome_count",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "outcome_state VARCHAR NOT NULL",
      "name": "outcome_state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "outcome_hash_state TEXT",
      "name": "outcome_hash_state",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "outcome_validation_cursor VARCHAR",
      "name": "outcome_validation_cursor",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "outcome_validation_count BIGINT NOT NULL",
      "name": "outcome_validation_count",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "outcome_set_sha256 VARCHAR(64)",
      "name": "outcome_set_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "outcome_failure TEXT",
      "name": "outcome_failure",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "outcomes_sealed_at VARCHAR",
      "name": "outcomes_sealed_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "source_collection_retirement_policy VARCHAR",
      "name": "source_collection_retirement_policy",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "source_collection_retirement_grace_seconds BIGINT NOT NULL",
      "name": "source_collection_retirement_grace_seconds",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "plan_sealed_at VARCHAR",
      "name": "plan_sealed_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "fence BIGINT NOT NULL",
      "name": "fence",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "expires_at VARCHAR NOT NULL",
      "name": "expires_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "output_collection_id BIGINT",
      "name": "output_collection_id",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "created_at VARCHAR NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "updated_at VARCHAR NOT NULL",
      "name": "updated_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "settled_at VARCHAR",
      "name": "settled_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "abandoned_at VARCHAR",
      "name": "abandoned_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "abandonment_reason TEXT",
      "name": "abandonment_reason",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "released_at VARCHAR",
      "name": "released_at",
      "nullable": true,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "id"
      ],
      "definition": "PRIMARY KEY (id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "consumer_app",
        "purpose",
        "work_id"
      ],
      "definition": "CONSTRAINT uq_collection_processing_claims_owner_work UNIQUE (consumer_app, purpose, work_id)",
      "kind": "unique",
      "name": "uq_collection_processing_claims_owner_work"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_state CHECK (state IN ('active','settled','retiring','abandoned','released'))",
      "expression": "(state IN ('active','settled','retiring','abandoned','released'))",
      "kind": "check",
      "name": "ck_collection_processing_claims_state"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_outcome_state CHECK (outcome_state IN ('receiving','sealing','sealed','failed'))",
      "expression": "(outcome_state IN ('receiving','sealing','sealed','failed'))",
      "kind": "check",
      "name": "ck_collection_processing_claims_outcome_state"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_fence CHECK (fence >= 1)",
      "expression": "(fence >= 1)",
      "kind": "check",
      "name": "ck_collection_processing_claims_fence"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_grace CHECK (source_collection_retirement_grace_seconds >= 0)",
      "expression": "(source_collection_retirement_grace_seconds >= 0)",
      "kind": "check",
      "name": "ck_collection_processing_claims_grace"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_artifact_count CHECK (input_count >= 0 AND artifact_count >= 0 AND artifact_bytes >= 0 AND outcome_count >= 0 AND outcome_validation_count >= 0)",
      "expression": "(input_count >= 0 AND artifact_count >= 0 AND artifact_bytes >= 0 AND outcome_count >= 0 AND outcome_validation_count >= 0)",
      "kind": "check",
      "name": "ck_collection_processing_claims_artifact_count"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_id CHECK (length(id) = 64)",
      "expression": "(length(id) = 64)",
      "kind": "check",
      "name": "ck_collection_processing_claims_id"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_work_id CHECK (length(work_id) = 64)",
      "expression": "(length(work_id) = 64)",
      "kind": "check",
      "name": "ck_collection_processing_claims_work_id"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_document_sha256 CHECK (length(work_document_sha256) = 64)",
      "expression": "(length(work_document_sha256) = 64)",
      "kind": "check",
      "name": "ck_collection_processing_claims_document_sha256"
    },
    {
      "columns": [
        "execution_id"
      ],
      "definition": "UNIQUE (execution_id)",
      "kind": "unique"
    },
    {
      "columns": [
        "output_collection_id"
      ],
      "definition": "FOREIGN KEY(output_collection_id) REFERENCES collections (id) ON DELETE SET NULL",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE SET NULL",
        "columns": [
          "id"
        ],
        "table": "collections"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_id_hex CHECK (length(id) = 64 AND lower(id) = id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(id) = 64 AND lower(id) = id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_claims_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_claims_work_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_work_document_sha256_hex CHECK (length(work_document_sha256) = 64 AND lower(work_document_sha256) = work_document_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_document_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(work_document_sha256) = 64 AND lower(work_document_sha256) = work_document_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_document_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_claims_work_document_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_execution_id_hex CHECK (execution_id IS NULL OR length(execution_id) = 64 AND lower(execution_id) = execution_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(execution_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(execution_id IS NULL OR length(execution_id) = 64 AND lower(execution_id) = execution_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(execution_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_claims_execution_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_sha256_c09acb3cbfceaefd CHECK (controller_evidence_sha256 IS NULL OR length(controller_evidence_sha256) = 64 AND lower(controller_evidence_sha256) = controller_evidence_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(controller_evidence_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(controller_evidence_sha256 IS NULL OR length(controller_evidence_sha256) = 64 AND lower(controller_evidence_sha256) = controller_evidence_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(controller_evidence_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_c09acb3cbfceaefd"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_operation_sha256_hex CHECK (operation_sha256 IS NULL OR length(operation_sha256) = 64 AND lower(operation_sha256) = operation_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(operation_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(operation_sha256 IS NULL OR length(operation_sha256) = 64 AND lower(operation_sha256) = operation_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(operation_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_claims_operation_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_input_set_sha256_hex CHECK (input_set_sha256 IS NULL OR length(input_set_sha256) = 64 AND lower(input_set_sha256) = input_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(input_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(input_set_sha256 IS NULL OR length(input_set_sha256) = 64 AND lower(input_set_sha256) = input_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(input_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_claims_input_set_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_artifact_set_sha256_hex CHECK (artifact_set_sha256 IS NULL OR length(artifact_set_sha256) = 64 AND lower(artifact_set_sha256) = artifact_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(artifact_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(artifact_set_sha256 IS NULL OR length(artifact_set_sha256) = 64 AND lower(artifact_set_sha256) = artifact_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(artifact_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_claims_artifact_set_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claims_outcome_set_sha256_hex CHECK (outcome_set_sha256 IS NULL OR length(outcome_set_sha256) = 64 AND lower(outcome_set_sha256) = outcome_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(outcome_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(outcome_set_sha256 IS NULL OR length(outcome_set_sha256) = 64 AND lower(outcome_set_sha256) = outcome_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(outcome_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_claims_outcome_set_sha256_hex"
    }
  ],
  "name": "collection_processing_claims"
}
```

</details>
