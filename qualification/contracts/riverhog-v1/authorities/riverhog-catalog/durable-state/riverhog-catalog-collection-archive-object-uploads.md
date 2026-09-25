# riverhog-catalog: collection_archive_object_uploads

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-archive-object-uploads:c0183c59f4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-478693b7ab"></a>

### Table: `collection_archive_object_uploads`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-7e8b800be8"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-6f450dc8a0"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-4992dd437b"></a>`sequence` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-41479d28b2"></a>`kind` | `VARCHAR` | no | `—` | — |
| <a id="s-64c18c5dcb"></a>`relative_path` | `VARCHAR` | no | `—` | — |
| <a id="s-f3058a0634"></a>`object_path` | `VARCHAR` | no | `—` | — |
| <a id="s-fd476dcfa2"></a>`plaintext_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-2a8d8794d3"></a>`source_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-3d0067dbda"></a>`source_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-368c897c64"></a>`source_first_part` | `BIGINT` | yes | `—` | — |
| <a id="s-bed5dab861"></a>`source_part_count` | `BIGINT` | yes | `—` | — |
| <a id="s-64bda1811d"></a>`unit_plaintext_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-5dfd0e565d"></a>`plan_json` | `TEXT` | no | `—` | — |
| <a id="s-7b9bc626bb"></a>`plan_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-018582e80b"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-6138493270"></a>`checkpoint_json` | `TEXT` | yes | `—` | — |
| <a id="s-fdece8f838"></a>`sealed_receipt_json` | `TEXT` | yes | `—` | — |
| <a id="s-56d43600de"></a>`metadata_receipt_json` | `TEXT` | yes | `—` | — |
| <a id="s-fb28d4c9cd"></a>`failure` | `TEXT` | yes | `—` | — |
| <a id="s-e0686803a0"></a>`uploaded_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-b94b15812b"></a>`uploaded_units` | `INTEGER` | no | `—` | — |
| <a id="s-564fc67988"></a>`total_units` | `INTEGER` | no | `—` | — |
| <a id="s-a6ff9cab0a"></a>`updated_at` | `VARCHAR` | no | `—` | — |
| <a id="s-ad632d8027"></a>`sealed_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-d8c8b0e273"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, object_id)` |
| <a id="s-145dbd0b05"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_uploads (collection_id) ON DELETE CASCADE` |
| <a id="s-7de19128fa"></a>`check` | `ck_archive_object_uploads_sequence` | `CONSTRAINT ck_archive_object_uploads_sequence CHECK (length(sequence) = 64 AND lower(sequence) = sequence AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sequence, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-0ce1453820"></a>`check` | `ck_archive_object_uploads_plaintext` | `CONSTRAINT ck_archive_object_uploads_plaintext CHECK (plaintext_bytes >= 0)` |
| <a id="s-54e2cb8112"></a>`check` | `ck_archive_object_uploads_source` | `CONSTRAINT ck_archive_object_uploads_source CHECK (source_bytes >= 0)` |
| <a id="s-32d808aa1f"></a>`check` | `ck_archive_object_uploads_source_parts` | `CONSTRAINT ck_archive_object_uploads_source_parts CHECK (kind = 'pack' AND source_path IS NULL AND source_first_part IS NULL AND source_part_count IS NULL OR kind = 'segment' AND source_path IS NOT NULL AND source_first_part >= 0 AND source_part_count > 0)` |
| <a id="s-a179979bce"></a>`check` | `ck_archive_object_uploads_unit` | `CONSTRAINT ck_archive_object_uploads_unit CHECK (unit_plaintext_bytes > 0)` |
| <a id="s-7857358f39"></a>`check` | `ck_archive_object_uploads_state` | `CONSTRAINT ck_archive_object_uploads_state CHECK (state IN ('planned','uploading','sealed'))` |
| <a id="s-60084fe685"></a>`check` | `ck_archive_object_uploads_uploaded_bytes` | `CONSTRAINT ck_archive_object_uploads_uploaded_bytes CHECK (uploaded_bytes >= 0)` |
| <a id="s-219c087f16"></a>`check` | `ck_archive_object_uploads_uploaded_units` | `CONSTRAINT ck_archive_object_uploads_uploaded_units CHECK (uploaded_units >= 0)` |
| <a id="s-5194863962"></a>`check` | `ck_archive_object_uploads_total_units` | `CONSTRAINT ck_archive_object_uploads_total_units CHECK (total_units >= 0)` |
| <a id="s-0d5abeb5d5"></a>`check` | `ck_archive_object_uploads_unit_progress` | `CONSTRAINT ck_archive_object_uploads_unit_progress CHECK (uploaded_units <= total_units)` |
| <a id="s-cc5d1cec3a"></a>`check` | `ck_collection_archive_object_uploads_plan_sha256_hex` | `CONSTRAINT ck_collection_archive_object_uploads_plan_sha256_hex CHECK (length(plan_sha256) = 64 AND lower(plan_sha256) = plan_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(plan_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-35cc6772f8"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/21`

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
