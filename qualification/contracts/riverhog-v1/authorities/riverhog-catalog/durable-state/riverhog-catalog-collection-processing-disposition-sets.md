# riverhog-catalog: collection_processing_disposition_sets

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-processing-di-c1b37f0046:1446612c9c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-cf59823348"></a>
- Table: `collection_processing_disposition_sets`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-4fe8090c32"></a>`claim_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-0f291cd4e2"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-0f961b500a"></a>`disposition_count` | `BIGINT` | no | `—` | — |
| <a id="s-23d2a36754"></a>`output_edge_count` | `BIGINT` | no | `—` | — |
| <a id="s-c787b2f974"></a>`output_artifact_count` | `BIGINT` | no | `—` | — |
| <a id="s-53c3fd3afc"></a>`transformed_count` | `BIGINT` | no | `—` | — |
| <a id="s-0e699987d6"></a>`transformed_with_outputs_count` | `BIGINT` | no | `—` | — |
| <a id="s-65c67f8d99"></a>`validation_phase` | `VARCHAR` | yes | `—` | — |
| <a id="s-1f6f24b761"></a>`validation_collection_id` | `BIGINT` | yes | `—` | — |
| <a id="s-159fdff678"></a>`validation_input_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-2a0942f60e"></a>`validation_output_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-7d860b1b62"></a>`validation_output_collection_id` | `BIGINT` | yes | `—` | — |
| <a id="s-5c4f76a891"></a>`validation_output_input_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-c7fcf52790"></a>`disposition_hash_state` | `TEXT` | yes | `—` | — |
| <a id="s-710fba49f9"></a>`output_hash_state` | `TEXT` | yes | `—` | — |
| <a id="s-80e9dac22f"></a>`disposition_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-cf8aeee505"></a>`output_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-ab800383b2"></a>`identity_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-cf60392f04"></a>`failure` | `TEXT` | yes | `—` | — |
| <a id="s-2013f58a1e"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-2a6edd3948"></a>`updated_at` | `VARCHAR` | no | `—` | — |
| <a id="s-30cc61f34d"></a>`sealed_at` | `VARCHAR` | yes | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-9927dc56fe"></a>`primary-key` | `—` | `PRIMARY KEY (claim_id)` |
| <a id="s-850043dd24"></a>`check` | `ck_processing_disposition_sets_state` | `CONSTRAINT ck_processing_disposition_sets_state CHECK (state IN ('receiving','sealing','sealed','failed'))` |
| <a id="s-68e5038c1f"></a>`check` | `ck_processing_disposition_sets_phase` | `CONSTRAINT ck_processing_disposition_sets_phase CHECK (validation_phase IS NULL OR validation_phase IN ('dispositions','outputs'))` |
| <a id="s-d610feb878"></a>`check` | `ck_processing_disposition_sets_counts` | `CONSTRAINT ck_processing_disposition_sets_counts CHECK (disposition_count >= 0 AND output_edge_count >= 0 AND output_artifact_count >= 0 AND transformed_count >= 0 AND transformed_with_outputs_count >= 0)` |
| <a id="s-612721833c"></a>`check` | `ck_processing_disposition_sets_output_counts` | `CONSTRAINT ck_processing_disposition_sets_output_counts CHECK (output_artifact_count <= output_edge_count)` |
| <a id="s-2c6a5f9de3"></a>`foreign-key` | `—` | `FOREIGN KEY(claim_id) REFERENCES collection_processing_claims (id) ON DELETE CASCADE` |
| <a id="s-82d4157845"></a>`check` | `ck_collection_processing_disposition_sets_claim_id_hex` | `CONSTRAINT ck_collection_processing_disposition_sets_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-68fb3472d8"></a>`check` | `ck_sha256_66cafd8d5821f368` | `CONSTRAINT ck_sha256_66cafd8d5821f368 CHECK (disposition_sha256 IS NULL OR length(disposition_sha256) = 64 AND lower(disposition_sha256) = disposition_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(disposition_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-ea982db0dd"></a>`check` | `ck_collection_processing_disposition_sets_output_sha256_hex` | `CONSTRAINT ck_collection_processing_disposition_sets_output_sha256_hex CHECK (output_sha256 IS NULL OR length(output_sha256) = 64 AND lower(output_sha256) = output_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(output_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-79cb736dfe"></a>`check` | `ck_sha256_11dda87dcbdbf203` | `CONSTRAINT ck_sha256_11dda87dcbdbf203 CHECK (identity_sha256 IS NULL OR length(identity_sha256) = 64 AND lower(identity_sha256) = identity_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(identity_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-ec6ddb1363"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/46`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e404dc6836cc9739ae5f6c6dbe38e8ec83752eaef4620e981f9940c62b6495a9 -->

```json
{
  "columns": [
    {
      "definition": "claim_id VARCHAR(64) NOT NULL",
      "name": "claim_id",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "disposition_count BIGINT NOT NULL",
      "name": "disposition_count",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "output_edge_count BIGINT NOT NULL",
      "name": "output_edge_count",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "output_artifact_count BIGINT NOT NULL",
      "name": "output_artifact_count",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "transformed_count BIGINT NOT NULL",
      "name": "transformed_count",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "transformed_with_outputs_count BIGINT NOT NULL",
      "name": "transformed_with_outputs_count",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "validation_phase VARCHAR",
      "name": "validation_phase",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "validation_collection_id BIGINT",
      "name": "validation_collection_id",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "validation_input_path VARCHAR",
      "name": "validation_input_path",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "validation_output_path VARCHAR",
      "name": "validation_output_path",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "validation_output_collection_id BIGINT",
      "name": "validation_output_collection_id",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "validation_output_input_path VARCHAR",
      "name": "validation_output_input_path",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "disposition_hash_state TEXT",
      "name": "disposition_hash_state",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "output_hash_state TEXT",
      "name": "output_hash_state",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "disposition_sha256 VARCHAR(64)",
      "name": "disposition_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "output_sha256 VARCHAR(64)",
      "name": "output_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "identity_sha256 VARCHAR(64)",
      "name": "identity_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "failure TEXT",
      "name": "failure",
      "nullable": true,
      "type": "TEXT"
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
      "definition": "sealed_at VARCHAR",
      "name": "sealed_at",
      "nullable": true,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "claim_id"
      ],
      "definition": "PRIMARY KEY (claim_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_processing_disposition_sets_state CHECK (state IN ('receiving','sealing','sealed','failed'))",
      "expression": "(state IN ('receiving','sealing','sealed','failed'))",
      "kind": "check",
      "name": "ck_processing_disposition_sets_state"
    },
    {
      "definition": "CONSTRAINT ck_processing_disposition_sets_phase CHECK (validation_phase IS NULL OR validation_phase IN ('dispositions','outputs'))",
      "expression": "(validation_phase IS NULL OR validation_phase IN ('dispositions','outputs'))",
      "kind": "check",
      "name": "ck_processing_disposition_sets_phase"
    },
    {
      "definition": "CONSTRAINT ck_processing_disposition_sets_counts CHECK (disposition_count >= 0 AND output_edge_count >= 0 AND output_artifact_count >= 0 AND transformed_count >= 0 AND transformed_with_outputs_count >= 0)",
      "expression": "(disposition_count >= 0 AND output_edge_count >= 0 AND output_artifact_count >= 0 AND transformed_count >= 0 AND transformed_with_outputs_count >= 0)",
      "kind": "check",
      "name": "ck_processing_disposition_sets_counts"
    },
    {
      "definition": "CONSTRAINT ck_processing_disposition_sets_output_counts CHECK (output_artifact_count <= output_edge_count)",
      "expression": "(output_artifact_count <= output_edge_count)",
      "kind": "check",
      "name": "ck_processing_disposition_sets_output_counts"
    },
    {
      "columns": [
        "claim_id"
      ],
      "definition": "FOREIGN KEY(claim_id) REFERENCES collection_processing_claims (id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "id"
        ],
        "table": "collection_processing_claims"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_disposition_sets_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_disposition_sets_claim_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_sha256_66cafd8d5821f368 CHECK (disposition_sha256 IS NULL OR length(disposition_sha256) = 64 AND lower(disposition_sha256) = disposition_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(disposition_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(disposition_sha256 IS NULL OR length(disposition_sha256) = 64 AND lower(disposition_sha256) = disposition_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(disposition_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_66cafd8d5821f368"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_disposition_sets_output_sha256_hex CHECK (output_sha256 IS NULL OR length(output_sha256) = 64 AND lower(output_sha256) = output_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(output_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(output_sha256 IS NULL OR length(output_sha256) = 64 AND lower(output_sha256) = output_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(output_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_disposition_sets_output_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_sha256_11dda87dcbdbf203 CHECK (identity_sha256 IS NULL OR length(identity_sha256) = 64 AND lower(identity_sha256) = identity_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(identity_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(identity_sha256 IS NULL OR length(identity_sha256) = 64 AND lower(identity_sha256) = identity_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(identity_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_11dda87dcbdbf203"
    }
  ],
  "name": "collection_processing_disposition_sets"
}
```
