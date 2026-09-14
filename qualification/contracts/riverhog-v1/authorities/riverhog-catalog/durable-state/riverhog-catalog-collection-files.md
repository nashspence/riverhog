# riverhog-catalog: collection_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-files:b491decc3c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-2729cfe6c9"></a>
- Table: `collection_files`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-36c735cf5e"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-2dd54321a8"></a>`path` | `VARCHAR` | no | `—` | — |
| <a id="s-ae0229fc91"></a>`bytes` | `BIGINT` | no | `—` | — |
| <a id="s-3334dd13c2"></a>`sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-c462329e99"></a>`provenance_status` | `VARCHAR` | no | `'missing'` | — |
| <a id="s-38d74ddcc7"></a>`path_sort_key` | `BYTEA` | no | `—` | — |
| <a id="s-54b239eebf"></a>`search_text` | `VARCHAR` | no | `—` | — |
| <a id="s-5cbf479950"></a>`path_search_text` | `VARCHAR` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-1de11207d3"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, path)` |
| <a id="s-02e5aeae57"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE` |
| <a id="s-be9a17bd71"></a>`check` | `ck_collection_files_bytes` | `CONSTRAINT ck_collection_files_bytes CHECK (bytes >= 0)` |
| <a id="s-7d4f3eec9b"></a>`check` | `ck_collection_files_sha256` | `CONSTRAINT ck_collection_files_sha256 CHECK (length(sha256) = 64)` |
| <a id="s-eb09cd1164"></a>`check` | `ck_collection_files_provenance_status` | `CONSTRAINT ck_collection_files_provenance_status CHECK (provenance_status IN ('captured','omitted','missing'))` |
| <a id="s-8709c6f8ea"></a>`check` | `ck_collection_files_sha256_hex` | `CONSTRAINT ck_collection_files_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [riverhog-catalog durable-state identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-b0e8e799d5"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/20`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 79f7106f5e95569ffe1249b7b63b407220c93e24b52f2881579abf5fc5cb8ddd -->

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
      "definition": "path VARCHAR NOT NULL",
      "name": "path",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "bytes BIGINT NOT NULL",
      "name": "bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "sha256 VARCHAR(64) NOT NULL",
      "name": "sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "default": "'missing'",
      "definition": "provenance_status VARCHAR DEFAULT 'missing' NOT NULL",
      "name": "provenance_status",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "path_sort_key BYTEA NOT NULL",
      "name": "path_sort_key",
      "nullable": false,
      "type": "BYTEA"
    },
    {
      "definition": "search_text VARCHAR NOT NULL",
      "name": "search_text",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "path_search_text VARCHAR NOT NULL",
      "name": "path_search_text",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "path"
      ],
      "definition": "PRIMARY KEY (collection_id, path)",
      "kind": "primary-key"
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
      "definition": "CONSTRAINT ck_collection_files_bytes CHECK (bytes >= 0)",
      "expression": "(bytes >= 0)",
      "kind": "check",
      "name": "ck_collection_files_bytes"
    },
    {
      "definition": "CONSTRAINT ck_collection_files_sha256 CHECK (length(sha256) = 64)",
      "expression": "(length(sha256) = 64)",
      "kind": "check",
      "name": "ck_collection_files_sha256"
    },
    {
      "definition": "CONSTRAINT ck_collection_files_provenance_status CHECK (provenance_status IN ('captured','omitted','missing'))",
      "expression": "(provenance_status IN ('captured','omitted','missing'))",
      "kind": "check",
      "name": "ck_collection_files_provenance_status"
    },
    {
      "definition": "CONSTRAINT ck_collection_files_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_files_sha256_hex"
    }
  ],
  "name": "collection_files"
}
```
