# riverhog-catalog: collection_archive_file_objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-archive-file-objects:e45395be88 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-dcea214d6b"></a>

### Table: `collection_archive_file_objects`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-84e33dea59"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-01e4ab62b3"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-91b79ad15a"></a>`path` | `VARCHAR` | no | `—` | — |
| <a id="s-2895a5a292"></a>`sequence` | `BIGINT` | no | `—` | — |
| <a id="s-0f69aae951"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-f4eae8af8f"></a>`file_offset` | `BIGINT` | no | `—` | — |
| <a id="s-8e9588bd72"></a>`object_offset` | `BIGINT` | no | `—` | — |
| <a id="s-4b05d6272b"></a>`bytes` | `BIGINT` | no | `—` | — |
| <a id="s-7fbb554fa1"></a>`member` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-41ec05c921"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store, path, sequence)` |
| <a id="s-3080f32aac"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, store, object_id) REFERENCES collection_archive_objects (collection_id, store, object_id) ON DELETE CASCADE` |
| <a id="s-5833f4eb0b"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, path) REFERENCES collection_files (collection_id, path) ON DELETE CASCADE` |
| <a id="s-aa27c23f66"></a>`check` | `ck_archive_file_objects_sequence` | `CONSTRAINT ck_archive_file_objects_sequence CHECK (sequence >= 0)` |
| <a id="s-e50cc0b2df"></a>`check` | `ck_archive_file_objects_file_offset` | `CONSTRAINT ck_archive_file_objects_file_offset CHECK (file_offset >= 0)` |
| <a id="s-f5fb204bd6"></a>`check` | `ck_archive_file_objects_object_offset` | `CONSTRAINT ck_archive_file_objects_object_offset CHECK (object_offset >= 0)` |
| <a id="s-24251e3321"></a>`check` | `ck_archive_file_objects_bytes` | `CONSTRAINT ck_archive_file_objects_bytes CHECK (bytes >= 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-3085d323d8"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/69`

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
