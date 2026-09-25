# riverhog-catalog: collection_processing_disposition_sets

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-processing-di-c1b37f0046:42f6f129aa -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-752449f3f7"></a>

### Table: `collection_processing_disposition_sets`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1844fea9ca"></a>`claim_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-a25c903146"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-7deb583524"></a>`disposition_count` | `BIGINT` | no | `—` | — |
| <a id="s-d9e917b22f"></a>`output_edge_count` | `BIGINT` | no | `—` | — |
| <a id="s-6163934f46"></a>`output_artifact_count` | `BIGINT` | no | `—` | — |
| <a id="s-0cfbc54f10"></a>`transformed_count` | `BIGINT` | no | `—` | — |
| <a id="s-bc4326af82"></a>`transformed_with_outputs_count` | `BIGINT` | no | `—` | — |
| <a id="s-b520f1dc18"></a>`validation_phase` | `VARCHAR` | yes | `—` | — |
| <a id="s-849032c210"></a>`validation_collection_id` | `BIGINT` | yes | `—` | — |
| <a id="s-e533001274"></a>`validation_input_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-016e2e0228"></a>`validation_output_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-0d88308909"></a>`validation_output_collection_id` | `BIGINT` | yes | `—` | — |
| <a id="s-e62a7e6287"></a>`validation_output_input_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-6e5a297743"></a>`disposition_hash_state` | `TEXT` | yes | `—` | — |
| <a id="s-67ab50ecb0"></a>`output_hash_state` | `TEXT` | yes | `—` | — |
| <a id="s-96df75cc4f"></a>`disposition_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-a6500d1a8b"></a>`output_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-809963b485"></a>`identity_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-3116588402"></a>`failure` | `TEXT` | yes | `—` | — |
| <a id="s-6c21c3897b"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-58fda7d3d2"></a>`updated_at` | `VARCHAR` | no | `—` | — |
| <a id="s-e54a308e39"></a>`sealed_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-b5b1e0ce88"></a>`primary-key` | `—` | `PRIMARY KEY (claim_id)` |
| <a id="s-1b54efaa44"></a>`check` | `ck_processing_disposition_sets_state` | `CONSTRAINT ck_processing_disposition_sets_state CHECK (state IN ('receiving','sealing','sealed','failed'))` |
| <a id="s-e4f433b235"></a>`check` | `ck_processing_disposition_sets_phase` | `CONSTRAINT ck_processing_disposition_sets_phase CHECK (validation_phase IS NULL OR validation_phase IN ('dispositions','outputs'))` |
| <a id="s-898f22405f"></a>`check` | `ck_processing_disposition_sets_counts` | `CONSTRAINT ck_processing_disposition_sets_counts CHECK (disposition_count >= 0 AND output_edge_count >= 0 AND output_artifact_count >= 0 AND transformed_count >= 0 AND transformed_with_outputs_count >= 0)` |
| <a id="s-ffc342a574"></a>`check` | `ck_processing_disposition_sets_output_counts` | `CONSTRAINT ck_processing_disposition_sets_output_counts CHECK (output_artifact_count <= output_edge_count)` |
| <a id="s-92f05227ce"></a>`foreign-key` | `—` | `FOREIGN KEY(claim_id) REFERENCES collection_processing_claims (id) ON DELETE CASCADE` |
| <a id="s-058438dd05"></a>`check` | `ck_collection_processing_disposition_sets_claim_id_hex` | `CONSTRAINT ck_collection_processing_disposition_sets_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-99e9c3861e"></a>`check` | `ck_sha256_66cafd8d5821f368` | `CONSTRAINT ck_sha256_66cafd8d5821f368 CHECK (disposition_sha256 IS NULL OR length(disposition_sha256) = 64 AND lower(disposition_sha256) = disposition_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(disposition_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-05ef151826"></a>`check` | `ck_collection_processing_disposition_sets_output_sha256_hex` | `CONSTRAINT ck_collection_processing_disposition_sets_output_sha256_hex CHECK (output_sha256 IS NULL OR length(output_sha256) = 64 AND lower(output_sha256) = output_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(output_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-d707c30d4b"></a>`check` | `ck_sha256_11dda87dcbdbf203` | `CONSTRAINT ck_sha256_11dda87dcbdbf203 CHECK (identity_sha256 IS NULL OR length(identity_sha256) = 64 AND lower(identity_sha256) = identity_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(identity_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-0bd83f3abb"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/48`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
