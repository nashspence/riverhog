# riverhog-catalog: collection_archive_file_objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-archive-file-objects:d8cdf43798 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-841b96ee19"></a>

### Table: `collection_archive_file_objects`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-20abc2cef1"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-5297f0c8b1"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-0628c3d37c"></a>`path` | `VARCHAR` | no | `—` | — |
| <a id="s-0bf019998a"></a>`sequence` | `BIGINT` | no | `—` | — |
| <a id="s-37e23b17c5"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-3ffac4b9c2"></a>`file_offset` | `BIGINT` | no | `—` | — |
| <a id="s-2cc72194df"></a>`object_offset` | `BIGINT` | no | `—` | — |
| <a id="s-030b053228"></a>`bytes` | `BIGINT` | no | `—` | — |
| <a id="s-585f88fbe1"></a>`member` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-0ef405d0d8"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store, path, sequence)` |
| <a id="s-76bb1574ec"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, store, object_id) REFERENCES collection_archive_objects (collection_id, store, object_id) ON DELETE CASCADE` |
| <a id="s-f3944a6def"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, path) REFERENCES collection_files (collection_id, path) ON DELETE CASCADE` |
| <a id="s-e6bfdc2d01"></a>`check` | `ck_archive_file_objects_sequence` | `CONSTRAINT ck_archive_file_objects_sequence CHECK (sequence >= 0)` |
| <a id="s-18d99c735a"></a>`check` | `ck_archive_file_objects_file_offset` | `CONSTRAINT ck_archive_file_objects_file_offset CHECK (file_offset >= 0)` |
| <a id="s-5fe26466b6"></a>`check` | `ck_archive_file_objects_object_offset` | `CONSTRAINT ck_archive_file_objects_object_offset CHECK (object_offset >= 0)` |
| <a id="s-2000575e14"></a>`check` | `ck_archive_file_objects_bytes` | `CONSTRAINT ck_archive_file_objects_bytes CHECK (bytes >= 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-75bcaf325c"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/70`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 592436ecade78996f17df6b08b31f3476f499ec675e2c347c3bf1f26e48eb28e -->

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
      "definition": "path VARCHAR NOT NULL",
      "name": "path",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "sequence BIGINT NOT NULL",
      "name": "sequence",
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
      "definition": "file_offset BIGINT NOT NULL",
      "name": "file_offset",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "object_offset BIGINT NOT NULL",
      "name": "object_offset",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "bytes BIGINT NOT NULL",
      "name": "bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "member VARCHAR",
      "name": "member",
      "nullable": true,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "store",
        "path",
        "sequence"
      ],
      "definition": "PRIMARY KEY (collection_id, store, path, sequence)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id",
        "store",
        "object_id"
      ],
      "definition": "FOREIGN KEY(collection_id, store, object_id) REFERENCES collection_archive_objects (collection_id, store, object_id) ON DELETE CASCADE",
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
      "columns": [
        "collection_id",
        "path"
      ],
      "definition": "FOREIGN KEY(collection_id, path) REFERENCES collection_files (collection_id, path) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id",
          "path"
        ],
        "table": "collection_files"
      }
    },
    {
      "definition": "CONSTRAINT ck_archive_file_objects_sequence CHECK (sequence >= 0)",
      "expression": "(sequence >= 0)",
      "kind": "check",
      "name": "ck_archive_file_objects_sequence"
    },
    {
      "definition": "CONSTRAINT ck_archive_file_objects_file_offset CHECK (file_offset >= 0)",
      "expression": "(file_offset >= 0)",
      "kind": "check",
      "name": "ck_archive_file_objects_file_offset"
    },
    {
      "definition": "CONSTRAINT ck_archive_file_objects_object_offset CHECK (object_offset >= 0)",
      "expression": "(object_offset >= 0)",
      "kind": "check",
      "name": "ck_archive_file_objects_object_offset"
    },
    {
      "definition": "CONSTRAINT ck_archive_file_objects_bytes CHECK (bytes >= 0)",
      "expression": "(bytes >= 0)",
      "kind": "check",
      "name": "ck_archive_file_objects_bytes"
    }
  ],
  "name": "collection_archive_file_objects"
}
```

</details>
