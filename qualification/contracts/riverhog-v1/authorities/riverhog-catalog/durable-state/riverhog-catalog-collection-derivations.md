# riverhog-catalog: collection_derivations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-derivations:d0e1e378d5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-534a86f2be"></a>

### Table: `collection_derivations`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-6917b0905f"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-2fc0f38afc"></a>`execution_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-53bc220b21"></a>`claim_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-922a4eccea"></a>`fence` | `BIGINT` | no | `—` | — |
| <a id="s-652e226f59"></a>`document_json` | `TEXT` | no | `—` | — |
| <a id="s-774413c21c"></a>`document_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-e5af529259"></a>`created_at` | `VARCHAR` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-cbe8c47225"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id)` |
| <a id="s-2f7025a01f"></a>`foreign-key` | `—` | `FOREIGN KEY(claim_id) REFERENCES collection_processing_claims (id) ON DELETE RESTRICT` |
| <a id="s-162ff679de"></a>`check` | `ck_collection_derivations_fence` | `CONSTRAINT ck_collection_derivations_fence CHECK (fence >= 1)` |
| <a id="s-54986f64d2"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE` |
| <a id="s-d9b0a86b7b"></a>`unique` | `—` | `UNIQUE (execution_id)` |
| <a id="s-2ad719db32"></a>`check` | `ck_collection_derivations_execution_id_hex` | `CONSTRAINT ck_collection_derivations_execution_id_hex CHECK (length(execution_id) = 64 AND lower(execution_id) = execution_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(execution_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-07d688fe63"></a>`check` | `ck_collection_derivations_claim_id_hex` | `CONSTRAINT ck_collection_derivations_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-77b405af46"></a>`check` | `ck_collection_derivations_document_sha256_hex` | `CONSTRAINT ck_collection_derivations_document_sha256_hex CHECK (length(document_sha256) = 64 AND lower(document_sha256) = document_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(document_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-1ca48c3b82"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/43`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 961ee2cc817b7ea1f00820ea5a16fbd32f8d40971c5a0196ad4184661f2ae5cc -->

```json
{
  "columns": [
    {
      "definition": "collection_id BIGINT NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "execution_id VARCHAR(64) NOT NULL",
      "name": "execution_id",
      "nullable": false,
      "type": "VARCHAR(64)"
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
      "definition": "document_json TEXT NOT NULL",
      "name": "document_json",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "document_sha256 VARCHAR(64) NOT NULL",
      "name": "document_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "created_at VARCHAR NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id"
      ],
      "definition": "PRIMARY KEY (collection_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "claim_id"
      ],
      "definition": "FOREIGN KEY(claim_id) REFERENCES collection_processing_claims (id) ON DELETE RESTRICT",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE RESTRICT",
        "columns": [
          "id"
        ],
        "table": "collection_processing_claims"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_derivations_fence CHECK (fence >= 1)",
      "expression": "(fence >= 1)",
      "kind": "check",
      "name": "ck_collection_derivations_fence"
    },
    {
      "columns": [
        "collection_id"
      ],
      "definition": "FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "id"
        ],
        "table": "collections"
      }
    },
    {
      "columns": [
        "execution_id"
      ],
      "definition": "UNIQUE (execution_id)",
      "kind": "unique"
    },
    {
      "definition": "CONSTRAINT ck_collection_derivations_execution_id_hex CHECK (length(execution_id) = 64 AND lower(execution_id) = execution_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(execution_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(execution_id) = 64 AND lower(execution_id) = execution_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(execution_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_derivations_execution_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_derivations_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_derivations_claim_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_derivations_document_sha256_hex CHECK (length(document_sha256) = 64 AND lower(document_sha256) = document_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(document_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(document_sha256) = 64 AND lower(document_sha256) = document_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(document_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_derivations_document_sha256_hex"
    }
  ],
  "name": "collection_derivations"
}
```

</details>
