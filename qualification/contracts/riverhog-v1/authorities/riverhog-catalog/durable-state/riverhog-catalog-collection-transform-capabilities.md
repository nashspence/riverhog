# riverhog-catalog: collection_transform_capabilities

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-transform-capabilities:20b32b58b4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-a8ec8d29ff"></a>

### Table: `collection_transform_capabilities`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-8c1bf328fd"></a>`id` | `VARCHAR(32)` | no | `—` | — |
| <a id="s-06cbc17628"></a>`claim_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-6ba8b99570"></a>`fence` | `BIGINT` | no | `—` | — |
| <a id="s-9e388b244c"></a>`audience` | `VARCHAR(300)` | no | `—` | — |
| <a id="s-b466d4d19a"></a>`token_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-cddb57bd11"></a>`actions_json` | `TEXT` | no | `—` | — |
| <a id="s-c5110e8d96"></a>`artifact_count` | `BIGINT` | no | `—` | — |
| <a id="s-dbe20b015d"></a>`artifact_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-345219cc83"></a>`artifact_hash_state` | `TEXT` | yes | `—` | — |
| <a id="s-0eabc8b416"></a>`artifact_set_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-18fd97a8c5"></a>`artifacts_sealed_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-a9e608e13f"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-da93c17d62"></a>`expires_at` | `VARCHAR` | no | `—` | — |
| <a id="s-e7d8c22706"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-1610cbe515"></a>`revoked_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-8351099719"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |
| <a id="s-472b4b1b39"></a>`check` | `ck_collection_transform_capabilities_state` | `CONSTRAINT ck_collection_transform_capabilities_state CHECK (state IN ('receiving','active','revoked'))` |
| <a id="s-9cd8e488b1"></a>`check` | `ck_collection_transform_capabilities_fence` | `CONSTRAINT ck_collection_transform_capabilities_fence CHECK (fence >= 1)` |
| <a id="s-32ea844fce"></a>`check` | `ck_collection_transform_capabilities_artifact_totals` | `CONSTRAINT ck_collection_transform_capabilities_artifact_totals CHECK (artifact_count >= 0 AND artifact_bytes >= 0)` |
| <a id="s-bf92aa18cd"></a>`foreign-key` | `—` | `FOREIGN KEY(claim_id) REFERENCES collection_processing_claims (id) ON DELETE CASCADE` |
| <a id="s-b6876674b4"></a>`unique` | `—` | `UNIQUE (token_sha256)` |
| <a id="s-01b1ec7635"></a>`check` | `ck_collection_transform_capabilities_claim_id_hex` | `CONSTRAINT ck_collection_transform_capabilities_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-287c3a5774"></a>`check` | `ck_collection_transform_capabilities_token_sha256_hex` | `CONSTRAINT ck_collection_transform_capabilities_token_sha256_hex CHECK (length(token_sha256) = 64 AND lower(token_sha256) = token_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(token_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-2825cb0291"></a>`check` | `ck_collection_transform_capabilities_artifact_set_sha256_hex` | `CONSTRAINT ck_collection_transform_capabilities_artifact_set_sha256_hex CHECK (artifact_set_sha256 IS NULL OR length(artifact_set_sha256) = 64 AND lower(artifact_set_sha256) = artifact_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(artifact_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-809faecf8f"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/61`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6d5fdb91242bb0ff0c0feff8024e483ead4560589d01bb9433b146416fc43ecd -->

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
      "definition": "CONSTRAINT ck_collection_transform_capabilities_state CHECK (state IN ('receiving','active','revoked'))",
      "expression": "(state IN ('receiving','active','revoked'))",
      "kind": "check",
      "name": "ck_collection_transform_capabilities_state"
    },
    {
      "definition": "CONSTRAINT ck_collection_transform_capabilities_fence CHECK (fence >= 1)",
      "expression": "(fence >= 1)",
      "kind": "check",
      "name": "ck_collection_transform_capabilities_fence"
    },
    {
      "definition": "CONSTRAINT ck_collection_transform_capabilities_artifact_totals CHECK (artifact_count >= 0 AND artifact_bytes >= 0)",
      "expression": "(artifact_count >= 0 AND artifact_bytes >= 0)",
      "kind": "check",
      "name": "ck_collection_transform_capabilities_artifact_totals"
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
      "definition": "CONSTRAINT ck_collection_transform_capabilities_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_transform_capabilities_claim_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_transform_capabilities_token_sha256_hex CHECK (length(token_sha256) = 64 AND lower(token_sha256) = token_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(token_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(token_sha256) = 64 AND lower(token_sha256) = token_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(token_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_transform_capabilities_token_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_transform_capabilities_artifact_set_sha256_hex CHECK (artifact_set_sha256 IS NULL OR length(artifact_set_sha256) = 64 AND lower(artifact_set_sha256) = artifact_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(artifact_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(artifact_set_sha256 IS NULL OR length(artifact_set_sha256) = 64 AND lower(artifact_set_sha256) = artifact_set_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(artifact_set_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_transform_capabilities_artifact_set_sha256_hex"
    }
  ],
  "name": "collection_transform_capabilities"
}
```

</details>
