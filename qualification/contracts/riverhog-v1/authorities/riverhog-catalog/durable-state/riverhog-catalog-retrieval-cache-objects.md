# riverhog-catalog: retrieval_cache_objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-cache-objects:d0478880d0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-4ebb09d713"></a>

### Table: `retrieval_cache_objects`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-61245f64ba"></a>`source_store` | `VARCHAR` | no | `—` | — |
| <a id="s-f72a2f0758"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-cac3e7b313"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-3562c13a99"></a>`cache_store` | `VARCHAR` | no | `—` | — |
| <a id="s-b71b44941d"></a>`object_path` | `VARCHAR` | no | `—` | — |
| <a id="s-39ff2eb2b7"></a>`revision` | `VARCHAR` | yes | `—` | — |
| <a id="s-18d7588136"></a>`stored_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-1cd19941c2"></a>`stored_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-f98b6c6db2"></a>`cached_at` | `VARCHAR` | no | `—` | — |
| <a id="s-e20f3290bd"></a>`verified_at` | `VARCHAR` | no | `—` | — |
| <a id="s-fe79c60105"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-03fe49828f"></a>`search_text` | `VARCHAR` | no | `—` | {"generated":"GENERATED ALWAYS AS (lower(source_store \|\| ' ' \|\| cache_store \|\| ' ' \|\| object_id)) STORED"} |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-f267298b57"></a>`primary-key` | `—` | `PRIMARY KEY (source_store, collection_id, object_id)` |
| <a id="s-388b2607bd"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, source_store, object_id) REFERENCES collection_archive_objects (collection_id, store, object_id) ON DELETE CASCADE` |
| <a id="s-daaf0fc6a5"></a>`check` | `ck_retrieval_cache_objects_bytes` | `CONSTRAINT ck_retrieval_cache_objects_bytes CHECK (stored_bytes >= 0)` |
| <a id="s-8cbbde8cd4"></a>`check` | `ck_retrieval_cache_objects_sha256` | `CONSTRAINT ck_retrieval_cache_objects_sha256 CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64)` |
| <a id="s-488d44bc11"></a>`check` | `ck_retrieval_cache_objects_state` | `CONSTRAINT ck_retrieval_cache_objects_state CHECK (state IN ('ready','delete_pending','deleting'))` |
| <a id="s-9edfebb989"></a>`check` | `ck_retrieval_cache_objects_stored_sha256_hex` | `CONSTRAINT ck_retrieval_cache_objects_stored_sha256_hex CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-88d64abd6a"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/75`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a278872eeb8024415060b8b5545c08227173bfb78d916de79d78203434482742 -->

```json
{
  "columns": [
    {
      "definition": "source_store VARCHAR NOT NULL",
      "name": "source_store",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "collection_id BIGINT NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "object_id VARCHAR NOT NULL",
      "name": "object_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "cache_store VARCHAR NOT NULL",
      "name": "cache_store",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "object_path VARCHAR NOT NULL",
      "name": "object_path",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "revision VARCHAR",
      "name": "revision",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "stored_bytes BIGINT NOT NULL",
      "name": "stored_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "stored_sha256 VARCHAR(64)",
      "name": "stored_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "cached_at VARCHAR NOT NULL",
      "name": "cached_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "verified_at VARCHAR NOT NULL",
      "name": "verified_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "search_text VARCHAR GENERATED ALWAYS AS (lower(source_store || ' ' || cache_store || ' ' || object_id)) STORED NOT NULL",
      "generated": "GENERATED ALWAYS AS (lower(source_store || ' ' || cache_store || ' ' || object_id)) STORED",
      "name": "search_text",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "source_store",
        "collection_id",
        "object_id"
      ],
      "definition": "PRIMARY KEY (source_store, collection_id, object_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id",
        "source_store",
        "object_id"
      ],
      "definition": "FOREIGN KEY(collection_id, source_store, object_id) REFERENCES collection_archive_objects (collection_id, store, object_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id",
          "store",
          "object_id"
        ],
        "table": "collection_archive_objects"
      }
    },
    {
      "definition": "CONSTRAINT ck_retrieval_cache_objects_bytes CHECK (stored_bytes >= 0)",
      "expression": "(stored_bytes >= 0)",
      "kind": "check",
      "name": "ck_retrieval_cache_objects_bytes"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_cache_objects_sha256 CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64)",
      "expression": "(stored_sha256 IS NULL OR length(stored_sha256) = 64)",
      "kind": "check",
      "name": "ck_retrieval_cache_objects_sha256"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_cache_objects_state CHECK (state IN ('ready','delete_pending','deleting'))",
      "expression": "(state IN ('ready','delete_pending','deleting'))",
      "kind": "check",
      "name": "ck_retrieval_cache_objects_state"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_cache_objects_stored_sha256_hex CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_retrieval_cache_objects_stored_sha256_hex"
    }
  ],
  "name": "retrieval_cache_objects"
}
```

</details>
