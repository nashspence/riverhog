# riverhog-catalog: retrieval_cache_objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-cache-objects:6f3ff1dc37 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-537eab488a"></a>

### Table: `retrieval_cache_objects`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-f64ca87e75"></a>`source_store` | `VARCHAR` | no | `—` | — |
| <a id="s-59951e13aa"></a>`source_incarnation_id` | `VARCHAR(36)` | no | `—` | — |
| <a id="s-316ac23f8a"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-962e1b65b6"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-24382c7188"></a>`cache_store` | `VARCHAR` | no | `—` | — |
| <a id="s-a2f61cec1b"></a>`cache_incarnation_id` | `VARCHAR(36)` | no | `—` | — |
| <a id="s-1619bd11de"></a>`object_path` | `VARCHAR` | no | `—` | — |
| <a id="s-86e2a393f7"></a>`revision` | `VARCHAR` | yes | `—` | — |
| <a id="s-3a78da1dd4"></a>`stored_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-691bdd9d42"></a>`stored_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-4e7f3c6a41"></a>`cached_at` | `VARCHAR` | no | `—` | — |
| <a id="s-10791c28e6"></a>`verified_at` | `VARCHAR` | no | `—` | — |
| <a id="s-8360593c3b"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-0ab64123e3"></a>`search_text` | `VARCHAR` | no | `—` | {"generated":"GENERATED ALWAYS AS (lower(source_store \|\| ' ' \|\| cache_store \|\| ' ' \|\| object_id)) STORED"} |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-c68f809359"></a>`primary-key` | `—` | `PRIMARY KEY (source_store, collection_id, object_id)` |
| <a id="s-593e9abc12"></a>`foreign-key` | `—` | `FOREIGN KEY(source_incarnation_id, source_store) REFERENCES storage_incarnations (id, name)` |
| <a id="s-7e3ab1913e"></a>`foreign-key` | `—` | `FOREIGN KEY(cache_incarnation_id, cache_store) REFERENCES storage_incarnations (id, name)` |
| <a id="s-abf290f916"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, source_store, object_id) REFERENCES collection_archive_objects (collection_id, store, object_id) ON DELETE CASCADE` |
| <a id="s-29c5088f4f"></a>`check` | `ck_retrieval_cache_objects_bytes` | `CONSTRAINT ck_retrieval_cache_objects_bytes CHECK (stored_bytes >= 0)` |
| <a id="s-c233e465c4"></a>`check` | `ck_retrieval_cache_objects_sha256` | `CONSTRAINT ck_retrieval_cache_objects_sha256 CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64)` |
| <a id="s-49f60232d8"></a>`check` | `ck_retrieval_cache_objects_state` | `CONSTRAINT ck_retrieval_cache_objects_state CHECK (state IN ('ready','delete_pending','deleting'))` |
| <a id="s-3164d23c5a"></a>`check` | `ck_retrieval_cache_objects_stored_sha256_hex` | `CONSTRAINT ck_retrieval_cache_objects_stored_sha256_hex CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-7cdf09290f"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/76`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 26ad002bdcd6bec290ff9b24a6a112f8e3273c371b02f8ecf806f885ca9ca57e -->

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
      "definition": "source_incarnation_id VARCHAR(36) NOT NULL",
      "name": "source_incarnation_id",
      "nullable": false,
      "type": "VARCHAR(36)"
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
      "definition": "cache_incarnation_id VARCHAR(36) NOT NULL",
      "name": "cache_incarnation_id",
      "nullable": false,
      "type": "VARCHAR(36)"
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
        "source_incarnation_id",
        "source_store"
      ],
      "definition": "FOREIGN KEY(source_incarnation_id, source_store) REFERENCES storage_incarnations (id, name)",
      "kind": "foreign-key",
      "references": {
        "columns": [
          "id",
          "name"
        ],
        "table": "storage_incarnations"
      }
    },
    {
      "columns": [
        "cache_incarnation_id",
        "cache_store"
      ],
      "definition": "FOREIGN KEY(cache_incarnation_id, cache_store) REFERENCES storage_incarnations (id, name)",
      "kind": "foreign-key",
      "references": {
        "columns": [
          "id",
          "name"
        ],
        "table": "storage_incarnations"
      }
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
