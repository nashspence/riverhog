# riverhog-catalog: retrieval_cache_objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-cache-objects:df1eb3c41e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-4180ec7daa"></a>

### Table: `retrieval_cache_objects`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-2debc5c694"></a>`source_store` | `VARCHAR` | no | `—` | — |
| <a id="s-20fe97cbec"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-56b3816983"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-6f35ff0483"></a>`cache_store` | `VARCHAR` | no | `—` | — |
| <a id="s-ced0518992"></a>`object_path` | `VARCHAR` | no | `—` | — |
| <a id="s-3726538d32"></a>`revision` | `VARCHAR` | yes | `—` | — |
| <a id="s-b18aab701a"></a>`stored_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-3741858bfd"></a>`stored_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-4827eefc45"></a>`cached_at` | `VARCHAR` | no | `—` | — |
| <a id="s-cb215eafc2"></a>`verified_at` | `VARCHAR` | no | `—` | — |
| <a id="s-d11b2a4548"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-3a6f9495a6"></a>`search_text` | `VARCHAR` | no | `—` | {"generated":"GENERATED ALWAYS AS (lower(source_store \|\| ' ' \|\| cache_store \|\| ' ' \|\| object_id)) STORED"} |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-7a90e604b5"></a>`primary-key` | `—` | `PRIMARY KEY (source_store, collection_id, object_id)` |
| <a id="s-e797a7a3e6"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, source_store, object_id) REFERENCES collection_archive_objects (collection_id, store, object_id) ON DELETE CASCADE` |
| <a id="s-fdbd7b48b7"></a>`check` | `ck_retrieval_cache_objects_bytes` | `CONSTRAINT ck_retrieval_cache_objects_bytes CHECK (stored_bytes >= 0)` |
| <a id="s-ebce6aefad"></a>`check` | `ck_retrieval_cache_objects_sha256` | `CONSTRAINT ck_retrieval_cache_objects_sha256 CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64)` |
| <a id="s-8e936eea02"></a>`check` | `ck_retrieval_cache_objects_state` | `CONSTRAINT ck_retrieval_cache_objects_state CHECK (state IN ('ready','delete_pending','deleting'))` |
| <a id="s-71c07af414"></a>`check` | `ck_retrieval_cache_objects_stored_sha256_hex` | `CONSTRAINT ck_retrieval_cache_objects_stored_sha256_hex CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-9662f83653"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/74`

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
