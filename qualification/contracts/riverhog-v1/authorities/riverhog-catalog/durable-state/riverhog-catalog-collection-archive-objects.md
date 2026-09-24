# riverhog-catalog: collection_archive_objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-archive-objects:c1b9b5c840 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-ab45e839c8"></a>

### Table: `collection_archive_objects`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-12f5a54864"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-1c25434298"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-7247072f13"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-b515fe882d"></a>`object_order` | `VARCHAR(65)` | no | `—` | — |
| <a id="s-01c30ec5f4"></a>`kind` | `VARCHAR` | no | `—` | — |
| <a id="s-5a3ad16631"></a>`object_path` | `VARCHAR` | no | `—` | — |
| <a id="s-61a2e88027"></a>`plaintext_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-83e42d1879"></a>`stored_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-da3b5fcd9a"></a>`sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-350c3a811d"></a>`stored_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-5d9fb27760"></a>`revision` | `VARCHAR` | yes | `—` | — |
| <a id="s-2bb02b082d"></a>`age_state_json` | `TEXT` | yes | `—` | — |
| <a id="s-506820d965"></a>`archive_parts_json` | `TEXT` | yes | `—` | — |
| <a id="s-f7bf506b34"></a>`plan_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-04fa0555f0"></a>`index_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-02f1a0e319"></a>`uploaded_at` | `VARCHAR` | no | `—` | — |
| <a id="s-64f9eb473f"></a>`verified_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-c082fccc70"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store, object_id)` |
| <a id="s-36929b7cff"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, store) REFERENCES collection_archive_copies (collection_id, store) ON DELETE CASCADE` |
| <a id="s-d07370ba6c"></a>`check` | `ck_collection_archive_objects_order` | `CONSTRAINT ck_collection_archive_objects_order CHECK (length(object_order) = 65 AND lower(object_order) = object_order AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(object_order, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-9d2f6de980"></a>`check` | `ck_collection_archive_objects_plaintext` | `CONSTRAINT ck_collection_archive_objects_plaintext CHECK (plaintext_bytes >= 0)` |
| <a id="s-d738b0e592"></a>`check` | `ck_collection_archive_objects_stored` | `CONSTRAINT ck_collection_archive_objects_stored CHECK (stored_bytes >= 0)` |
| <a id="s-8a430f59f8"></a>`check` | `ck_collection_archive_objects_sha256_hex` | `CONSTRAINT ck_collection_archive_objects_sha256_hex CHECK (sha256 IS NULL OR length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-ceb9d03972"></a>`check` | `ck_collection_archive_objects_stored_sha256_hex` | `CONSTRAINT ck_collection_archive_objects_stored_sha256_hex CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-ea3064e932"></a>`check` | `ck_collection_archive_objects_plan_sha256_hex` | `CONSTRAINT ck_collection_archive_objects_plan_sha256_hex CHECK (plan_sha256 IS NULL OR length(plan_sha256) = 64 AND lower(plan_sha256) = plan_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(plan_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-1ef77c1908"></a>`check` | `ck_collection_archive_objects_index_sha256_hex` | `CONSTRAINT ck_collection_archive_objects_index_sha256_hex CHECK (index_sha256 IS NULL OR length(index_sha256) = 64 AND lower(index_sha256) = index_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(index_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-cbd1205919"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/42`

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
