# riverhog-catalog: collection_archive_objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-archive-objects:cb4b525317 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-534a86f2be"></a>

### Table: `collection_archive_objects`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-6917b0905f"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-2fc0f38afc"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-53bc220b21"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-922a4eccea"></a>`object_order` | `VARCHAR(65)` | no | `—` | — |
| <a id="s-652e226f59"></a>`kind` | `VARCHAR` | no | `—` | — |
| <a id="s-774413c21c"></a>`object_path` | `VARCHAR` | no | `—` | — |
| <a id="s-e5af529259"></a>`plaintext_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-1ebf4f6c5c"></a>`stored_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-e585fb2533"></a>`sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-0cfd24fd6c"></a>`stored_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-869bc32ca5"></a>`revision` | `VARCHAR` | yes | `—` | — |
| <a id="s-5059ed65b2"></a>`age_state_json` | `TEXT` | yes | `—` | — |
| <a id="s-3b68f49d21"></a>`archive_parts_json` | `TEXT` | yes | `—` | — |
| <a id="s-fa182ec714"></a>`plan_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-35a7d7ae01"></a>`index_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-72d455c9b7"></a>`uploaded_at` | `VARCHAR` | no | `—` | — |
| <a id="s-8a8e5fc9e1"></a>`verified_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-cbe8c47225"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store, object_id)` |
| <a id="s-2f7025a01f"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, store) REFERENCES collection_archive_copies (collection_id, store) ON DELETE CASCADE` |
| <a id="s-162ff679de"></a>`check` | `ck_collection_archive_objects_order` | `CONSTRAINT ck_collection_archive_objects_order CHECK (length(object_order) = 65 AND lower(object_order) = object_order AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(object_order, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-54986f64d2"></a>`check` | `ck_collection_archive_objects_plaintext` | `CONSTRAINT ck_collection_archive_objects_plaintext CHECK (plaintext_bytes >= 0)` |
| <a id="s-d9b0a86b7b"></a>`check` | `ck_collection_archive_objects_stored` | `CONSTRAINT ck_collection_archive_objects_stored CHECK (stored_bytes >= 0)` |
| <a id="s-2ad719db32"></a>`check` | `ck_collection_archive_objects_sha256_hex` | `CONSTRAINT ck_collection_archive_objects_sha256_hex CHECK (sha256 IS NULL OR length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-07d688fe63"></a>`check` | `ck_collection_archive_objects_stored_sha256_hex` | `CONSTRAINT ck_collection_archive_objects_stored_sha256_hex CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-77b405af46"></a>`check` | `ck_collection_archive_objects_plan_sha256_hex` | `CONSTRAINT ck_collection_archive_objects_plan_sha256_hex CHECK (plan_sha256 IS NULL OR length(plan_sha256) = 64 AND lower(plan_sha256) = plan_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(plan_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-e7e5dfb58a"></a>`check` | `ck_collection_archive_objects_index_sha256_hex` | `CONSTRAINT ck_collection_archive_objects_index_sha256_hex CHECK (index_sha256 IS NULL OR length(index_sha256) = 64 AND lower(index_sha256) = index_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(index_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-fb8734460b"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

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

<!-- exact-contract-value: e9171a0e12eb127b20452c9566135fbb9a8f042d81312026cadeaf14082c6707 -->

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
      "definition": "store VARCHAR NOT NULL",
      "name": "store",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "object_id VARCHAR NOT NULL",
      "name": "object_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "object_order VARCHAR(65) NOT NULL",
      "name": "object_order",
      "nullable": false,
      "type": "VARCHAR(65)"
    },
    {
      "definition": "kind VARCHAR NOT NULL",
      "name": "kind",
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
      "definition": "plaintext_bytes BIGINT NOT NULL",
      "name": "plaintext_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "stored_bytes BIGINT NOT NULL",
      "name": "stored_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "sha256 VARCHAR(64)",
      "name": "sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "stored_sha256 VARCHAR(64)",
      "name": "stored_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "revision VARCHAR",
      "name": "revision",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "age_state_json TEXT",
      "name": "age_state_json",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "archive_parts_json TEXT",
      "name": "archive_parts_json",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "plan_sha256 VARCHAR(64)",
      "name": "plan_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "index_sha256 VARCHAR(64)",
      "name": "index_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "uploaded_at VARCHAR NOT NULL",
      "name": "uploaded_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "verified_at VARCHAR",
      "name": "verified_at",
      "nullable": true,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "store",
        "object_id"
      ],
      "definition": "PRIMARY KEY (collection_id, store, object_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id",
        "store"
      ],
      "definition": "FOREIGN KEY(collection_id, store) REFERENCES collection_archive_copies (collection_id, store) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id",
          "store"
        ],
        "table": "collection_archive_copies"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_archive_objects_order CHECK (length(object_order) = 65 AND lower(object_order) = object_order AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(object_order, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(object_order) = 65 AND lower(object_order) = object_order AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(object_order, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_archive_objects_order"
    },
    {
      "definition": "CONSTRAINT ck_collection_archive_objects_plaintext CHECK (plaintext_bytes >= 0)",
      "expression": "(plaintext_bytes >= 0)",
      "kind": "check",
      "name": "ck_collection_archive_objects_plaintext"
    },
    {
      "definition": "CONSTRAINT ck_collection_archive_objects_stored CHECK (stored_bytes >= 0)",
      "expression": "(stored_bytes >= 0)",
      "kind": "check",
      "name": "ck_collection_archive_objects_stored"
    },
    {
      "definition": "CONSTRAINT ck_collection_archive_objects_sha256_hex CHECK (sha256 IS NULL OR length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(sha256 IS NULL OR length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_archive_objects_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_archive_objects_stored_sha256_hex CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_archive_objects_stored_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_archive_objects_plan_sha256_hex CHECK (plan_sha256 IS NULL OR length(plan_sha256) = 64 AND lower(plan_sha256) = plan_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(plan_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(plan_sha256 IS NULL OR length(plan_sha256) = 64 AND lower(plan_sha256) = plan_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(plan_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_archive_objects_plan_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_archive_objects_index_sha256_hex CHECK (index_sha256 IS NULL OR length(index_sha256) = 64 AND lower(index_sha256) = index_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(index_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(index_sha256 IS NULL OR length(index_sha256) = 64 AND lower(index_sha256) = index_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(index_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_archive_objects_index_sha256_hex"
    }
  ],
  "name": "collection_archive_objects"
}
```

</details>
