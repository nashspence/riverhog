# riverhog-catalog: collection_archive_object_uploads

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-archive-object-uploads:f926209313 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-2729cfe6c9"></a>

### Table: `collection_archive_object_uploads`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-36c735cf5e"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-2dd54321a8"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-ae0229fc91"></a>`sequence` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-3334dd13c2"></a>`kind` | `VARCHAR` | no | `—` | — |
| <a id="s-c462329e99"></a>`relative_path` | `VARCHAR` | no | `—` | — |
| <a id="s-38d74ddcc7"></a>`object_path` | `VARCHAR` | no | `—` | — |
| <a id="s-54b239eebf"></a>`plaintext_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-5cbf479950"></a>`source_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-7ae27d824f"></a>`source_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-06ebf345c6"></a>`source_first_part` | `BIGINT` | yes | `—` | — |
| <a id="s-02fc2a8a88"></a>`source_part_count` | `BIGINT` | yes | `—` | — |
| <a id="s-5d0a624fd4"></a>`unit_plaintext_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-7c8d15b0dc"></a>`plan_json` | `TEXT` | no | `—` | — |
| <a id="s-449c9bc05a"></a>`plan_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-c8ac7d0d6b"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-59f14e2d9b"></a>`checkpoint_json` | `TEXT` | yes | `—` | — |
| <a id="s-d0cddcf0b0"></a>`sealed_receipt_json` | `TEXT` | yes | `—` | — |
| <a id="s-7ce0ecebe2"></a>`metadata_receipt_json` | `TEXT` | yes | `—` | — |
| <a id="s-79ff963236"></a>`failure` | `TEXT` | yes | `—` | — |
| <a id="s-7606461346"></a>`uploaded_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-f25cf77f4d"></a>`uploaded_units` | `INTEGER` | no | `—` | — |
| <a id="s-52ea707285"></a>`total_units` | `INTEGER` | no | `—` | — |
| <a id="s-d15c4c57e7"></a>`updated_at` | `VARCHAR` | no | `—` | — |
| <a id="s-059a515440"></a>`sealed_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-1de11207d3"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, object_id)` |
| <a id="s-02e5aeae57"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_uploads (collection_id) ON DELETE CASCADE` |
| <a id="s-be9a17bd71"></a>`check` | `ck_archive_object_uploads_sequence` | `CONSTRAINT ck_archive_object_uploads_sequence CHECK (length(sequence) = 64 AND lower(sequence) = sequence AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sequence, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-7d4f3eec9b"></a>`check` | `ck_archive_object_uploads_plaintext` | `CONSTRAINT ck_archive_object_uploads_plaintext CHECK (plaintext_bytes >= 0)` |
| <a id="s-eb09cd1164"></a>`check` | `ck_archive_object_uploads_source` | `CONSTRAINT ck_archive_object_uploads_source CHECK (source_bytes >= 0)` |
| <a id="s-8709c6f8ea"></a>`check` | `ck_archive_object_uploads_source_parts` | `CONSTRAINT ck_archive_object_uploads_source_parts CHECK (kind = 'pack' AND source_path IS NULL AND source_first_part IS NULL AND source_part_count IS NULL OR kind = 'segment' AND source_path IS NOT NULL AND source_first_part >= 0 AND source_part_count > 0)` |
| <a id="s-302c4acf70"></a>`check` | `ck_archive_object_uploads_unit` | `CONSTRAINT ck_archive_object_uploads_unit CHECK (unit_plaintext_bytes > 0)` |
| <a id="s-3bc0c0f9c8"></a>`check` | `ck_archive_object_uploads_state` | `CONSTRAINT ck_archive_object_uploads_state CHECK (state IN ('planned','uploading','sealed'))` |
| <a id="s-f5f55fa909"></a>`check` | `ck_archive_object_uploads_uploaded_bytes` | `CONSTRAINT ck_archive_object_uploads_uploaded_bytes CHECK (uploaded_bytes >= 0)` |
| <a id="s-28c9ef04d4"></a>`check` | `ck_archive_object_uploads_uploaded_units` | `CONSTRAINT ck_archive_object_uploads_uploaded_units CHECK (uploaded_units >= 0)` |
| <a id="s-ad69a448fc"></a>`check` | `ck_archive_object_uploads_total_units` | `CONSTRAINT ck_archive_object_uploads_total_units CHECK (total_units >= 0)` |
| <a id="s-48d1059751"></a>`check` | `ck_archive_object_uploads_unit_progress` | `CONSTRAINT ck_archive_object_uploads_unit_progress CHECK (uploaded_units <= total_units)` |
| <a id="s-6788773293"></a>`check` | `ck_collection_archive_object_uploads_plan_sha256_hex` | `CONSTRAINT ck_collection_archive_object_uploads_plan_sha256_hex CHECK (length(plan_sha256) = 64 AND lower(plan_sha256) = plan_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(plan_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-d9d2b5d4a8"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/20`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0d6a9d428b8a475c9d5b57ecf759690e5b592c1cb5e0fa5664e5ccecb4fab6bb -->

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
      "definition": "object_id VARCHAR NOT NULL",
      "name": "object_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "sequence VARCHAR(64) NOT NULL",
      "name": "sequence",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "kind VARCHAR NOT NULL",
      "name": "kind",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "relative_path VARCHAR NOT NULL",
      "name": "relative_path",
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
      "definition": "source_bytes BIGINT NOT NULL",
      "name": "source_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "source_path VARCHAR",
      "name": "source_path",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "source_first_part BIGINT",
      "name": "source_first_part",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "source_part_count BIGINT",
      "name": "source_part_count",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "unit_plaintext_bytes BIGINT NOT NULL",
      "name": "unit_plaintext_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "plan_json TEXT NOT NULL",
      "name": "plan_json",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "plan_sha256 VARCHAR(64) NOT NULL",
      "name": "plan_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "checkpoint_json TEXT",
      "name": "checkpoint_json",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "sealed_receipt_json TEXT",
      "name": "sealed_receipt_json",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "metadata_receipt_json TEXT",
      "name": "metadata_receipt_json",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "failure TEXT",
      "name": "failure",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "uploaded_bytes BIGINT NOT NULL",
      "name": "uploaded_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "uploaded_units INTEGER NOT NULL",
      "name": "uploaded_units",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "total_units INTEGER NOT NULL",
      "name": "total_units",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "updated_at VARCHAR NOT NULL",
      "name": "updated_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "sealed_at VARCHAR",
      "name": "sealed_at",
      "nullable": true,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "object_id"
      ],
      "definition": "PRIMARY KEY (collection_id, object_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id"
      ],
      "definition": "FOREIGN KEY(collection_id) REFERENCES collection_uploads (collection_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id"
        ],
        "table": "collection_uploads"
      }
    },
    {
      "definition": "CONSTRAINT ck_archive_object_uploads_sequence CHECK (length(sequence) = 64 AND lower(sequence) = sequence AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sequence, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(sequence) = 64 AND lower(sequence) = sequence AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sequence, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_archive_object_uploads_sequence"
    },
    {
      "definition": "CONSTRAINT ck_archive_object_uploads_plaintext CHECK (plaintext_bytes >= 0)",
      "expression": "(plaintext_bytes >= 0)",
      "kind": "check",
      "name": "ck_archive_object_uploads_plaintext"
    },
    {
      "definition": "CONSTRAINT ck_archive_object_uploads_source CHECK (source_bytes >= 0)",
      "expression": "(source_bytes >= 0)",
      "kind": "check",
      "name": "ck_archive_object_uploads_source"
    },
    {
      "definition": "CONSTRAINT ck_archive_object_uploads_source_parts CHECK (kind = 'pack' AND source_path IS NULL AND source_first_part IS NULL AND source_part_count IS NULL OR kind = 'segment' AND source_path IS NOT NULL AND source_first_part >= 0 AND source_part_count > 0)",
      "expression": "(kind = 'pack' AND source_path IS NULL AND source_first_part IS NULL AND source_part_count IS NULL OR kind = 'segment' AND source_path IS NOT NULL AND source_first_part >= 0 AND source_part_count > 0)",
      "kind": "check",
      "name": "ck_archive_object_uploads_source_parts"
    },
    {
      "definition": "CONSTRAINT ck_archive_object_uploads_unit CHECK (unit_plaintext_bytes > 0)",
      "expression": "(unit_plaintext_bytes > 0)",
      "kind": "check",
      "name": "ck_archive_object_uploads_unit"
    },
    {
      "definition": "CONSTRAINT ck_archive_object_uploads_state CHECK (state IN ('planned','uploading','sealed'))",
      "expression": "(state IN ('planned','uploading','sealed'))",
      "kind": "check",
      "name": "ck_archive_object_uploads_state"
    },
    {
      "definition": "CONSTRAINT ck_archive_object_uploads_uploaded_bytes CHECK (uploaded_bytes >= 0)",
      "expression": "(uploaded_bytes >= 0)",
      "kind": "check",
      "name": "ck_archive_object_uploads_uploaded_bytes"
    },
    {
      "definition": "CONSTRAINT ck_archive_object_uploads_uploaded_units CHECK (uploaded_units >= 0)",
      "expression": "(uploaded_units >= 0)",
      "kind": "check",
      "name": "ck_archive_object_uploads_uploaded_units"
    },
    {
      "definition": "CONSTRAINT ck_archive_object_uploads_total_units CHECK (total_units >= 0)",
      "expression": "(total_units >= 0)",
      "kind": "check",
      "name": "ck_archive_object_uploads_total_units"
    },
    {
      "definition": "CONSTRAINT ck_archive_object_uploads_unit_progress CHECK (uploaded_units <= total_units)",
      "expression": "(uploaded_units <= total_units)",
      "kind": "check",
      "name": "ck_archive_object_uploads_unit_progress"
    },
    {
      "definition": "CONSTRAINT ck_collection_archive_object_uploads_plan_sha256_hex CHECK (length(plan_sha256) = 64 AND lower(plan_sha256) = plan_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(plan_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(plan_sha256) = 64 AND lower(plan_sha256) = plan_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(plan_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_archive_object_uploads_plan_sha256_hex"
    }
  ],
  "name": "collection_archive_object_uploads"
}
```

</details>
