# riverhog-catalog: collection_processing_capabilities

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-processing-capabilities:a3e60641c7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-e6abdf96d1"></a>

### Table: `collection_processing_capabilities`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-20c7e3e7df"></a>`id` | `VARCHAR(32)` | no | `—` | — |
| <a id="s-d3d91fea84"></a>`claim_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-97155ab8ee"></a>`fence` | `BIGINT` | no | `—` | — |
| <a id="s-0c3c5db4f5"></a>`audience` | `VARCHAR(300)` | no | `—` | — |
| <a id="s-6d02810359"></a>`token_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-ed0c757167"></a>`actions_json` | `TEXT` | no | `—` | — |
| <a id="s-377ffe2db5"></a>`artifact_count` | `BIGINT` | no | `—` | — |
| <a id="s-a898258ac6"></a>`artifact_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-4351dfa192"></a>`artifact_hash_state` | `TEXT` | yes | `—` | — |
| <a id="s-47e9d07f4a"></a>`artifact_set_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-4125f2eca9"></a>`artifacts_sealed_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-391ef847fb"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-0b8f6e07b2"></a>`expires_at` | `VARCHAR` | no | `—` | — |
| <a id="s-d0b1b9a6af"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-e22474c0b2"></a>`revoked_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-8bfc344ae4"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |
| <a id="s-b589cf0895"></a>`check` | `ck_collection_processing_capabilities_state` | `CONSTRAINT ck_collection_processing_capabilities_state CHECK (state IN ('receiving','active','revoked'))` |
| <a id="s-e38219ad8c"></a>`check` | `ck_collection_processing_capabilities_fence` | `CONSTRAINT ck_collection_processing_capabilities_fence CHECK (fence >= 1)` |
| <a id="s-0b0f9dd8f7"></a>`check` | `ck_collection_processing_capabilities_artifact_totals` | `CONSTRAINT ck_collection_processing_capabilities_artifact_totals CHECK (artifact_count >= 0 AND artifact_bytes >= 0)` |
| <a id="s-f3c765618f"></a>`foreign-key` | `—` | `FOREIGN KEY(claim_id) REFERENCES collection_processing_claims (id) ON DELETE CASCADE` |
| <a id="s-f4c4b6398f"></a>`unique` | `—` | `UNIQUE (token_sha256)` |
| <a id="s-ab45166100"></a>`check` | `ck_collection_processing_capabilities_claim_id_hex` | `CONSTRAINT ck_collection_processing_capabilities_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-21a3062f73"></a>`check` | `ck_collection_processing_capabilities_token_sha256_hex` | `CONSTRAINT ck_collection_processing_capabilities_token_sha256_hex CHECK (length(token_sha256) = 64 AND lower(token_sha256) = token_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(token_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-ef0db0772e"></a>`check` | `ck_sha256_f421ad4e24e487cb` | `CONSTRAINT ck_sha256_f421ad4e24e487cb CHECK (artifact_set_sha256 IS NULL OR length(artifact_set_sha256) = 64 AND lower(artifact_set_sha256) = artifact_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(artifact_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-a85d29d2c3"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/63`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d4bc4801bd425278c02a98f1577e8cb0d77ad2e2e114988874c41a01a6a21849 -->

```json
{
  "columns": [
    {
      "definition": "id VARCHAR(32) NOT NULL",
      "name": "id",
      "nullable": false,
      "type": "VARCHAR(32)"
    },
    {
      "definition": "claim_id VARCHAR(64) NOT NULL",
      "name": "claim_id",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "fence BIGINT NOT NULL",
      "name": "fence",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "audience VARCHAR(300) NOT NULL",
      "name": "audience",
      "nullable": false,
      "type": "VARCHAR(300)"
    },
    {
      "definition": "token_sha256 VARCHAR(64) NOT NULL",
      "name": "token_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "actions_json TEXT NOT NULL",
      "name": "actions_json",
      "nullable": false,
      "type": "TEXT"
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
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "expires_at VARCHAR NOT NULL",
      "name": "expires_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "created_at VARCHAR NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "revoked_at VARCHAR",
      "name": "revoked_at",
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
      "definition": "CONSTRAINT ck_collection_processing_capabilities_state CHECK (state IN ('receiving','active','revoked'))",
      "expression": "(state IN ('receiving','active','revoked'))",
      "kind": "check",
      "name": "ck_collection_processing_capabilities_state"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_capabilities_fence CHECK (fence >= 1)",
      "expression": "(fence >= 1)",
      "kind": "check",
      "name": "ck_collection_processing_capabilities_fence"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_capabilities_artifact_totals CHECK (artifact_count >= 0 AND artifact_bytes >= 0)",
      "expression": "(artifact_count >= 0 AND artifact_bytes >= 0)",
      "kind": "check",
      "name": "ck_collection_processing_capabilities_artifact_totals"
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
      "columns": [
        "token_sha256"
      ],
      "definition": "UNIQUE (token_sha256)",
      "kind": "unique"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_capabilities_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_capabilities_claim_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_capabilities_token_sha256_hex CHECK (length(token_sha256) = 64 AND lower(token_sha256) = token_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(token_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(token_sha256) = 64 AND lower(token_sha256) = token_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(token_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_capabilities_token_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_sha256_f421ad4e24e487cb CHECK (artifact_set_sha256 IS NULL OR length(artifact_set_sha256) = 64 AND lower(artifact_set_sha256) = artifact_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(artifact_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(artifact_set_sha256 IS NULL OR length(artifact_set_sha256) = 64 AND lower(artifact_set_sha256) = artifact_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(artifact_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_f421ad4e24e487cb"
    }
  ],
  "name": "collection_processing_capabilities"
}
```

</details>
