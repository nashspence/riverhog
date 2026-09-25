# riverhog-catalog: collection_upload_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-files:c955316dd7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-0a7b502c4e"></a>

### Table: `collection_upload_files`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-5fb26066c8"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-96b02c1cad"></a>`path` | `VARCHAR` | no | `—` | — |
| <a id="s-421af34327"></a>`path_sort_key` | `BYTEA` | no | `—` | — |
| <a id="s-7e8d32cfa9"></a>`semantic_order_rank` | `INTEGER` | no | `—` | — |
| <a id="s-ac3195928b"></a>`file_order` | `INTEGER` | no | `—` | — |
| <a id="s-6c2e636236"></a>`bytes` | `BIGINT` | no | `—` | — |
| <a id="s-f702a22856"></a>`sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-bb8fde4e17"></a>`raw_part_plaintext_bytes` | `BIGINT` | yes | `—` | — |
| <a id="s-62a1cca1e7"></a>`raw_part_count` | `BIGINT` | yes | `—` | — |
| <a id="s-994c30a83b"></a>`raw_part_ordered_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-29e4238001"></a>`raw_parts_accepted` | `BIGINT` | no | `0` | — |
| <a id="s-3b9243d7b9"></a>`raw_part_commitment_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-ac8cf94016"></a>`provenance_status` | `VARCHAR` | no | `—` | — |
| <a id="s-74a4dd4ea9"></a>`provenance_journal_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-dbefe2bfa6"></a>`provenance_current_state_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-b37bd6043f"></a>`provenance_omission_reason` | `TEXT` | yes | `—` | — |
| <a id="s-33ff573e88"></a>`custodied_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-32ecf49e80"></a>`custody_receipt_json` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-62f9c0d071"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, path)` |
| <a id="s-0e4eccc667"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_uploads (collection_id) ON DELETE CASCADE` |
| <a id="s-26369da6d0"></a>`check` | `ck_collection_upload_files_order` | `CONSTRAINT ck_collection_upload_files_order CHECK (file_order >= 0)` |
| <a id="s-b63cf1b5d9"></a>`check` | `ck_collection_upload_files_semantic_order_rank` | `CONSTRAINT ck_collection_upload_files_semantic_order_rank CHECK (semantic_order_rank >= 0 AND semantic_order_rank <= 2)` |
| <a id="s-02173694c3"></a>`check` | `ck_collection_upload_files_bytes` | `CONSTRAINT ck_collection_upload_files_bytes CHECK (bytes >= 0)` |
| <a id="s-a7eb28e2d6"></a>`check` | `ck_collection_upload_files_raw_parts` | `CONSTRAINT ck_collection_upload_files_raw_parts CHECK (raw_parts_accepted >= 0)` |
| <a id="s-fcfbd2e204"></a>`check` | `ck_collection_upload_files_sha256` | `CONSTRAINT ck_collection_upload_files_sha256 CHECK (length(sha256) = 64)` |
| <a id="s-a8354d32bb"></a>`check` | `ck_collection_upload_files_sha256_hex` | `CONSTRAINT ck_collection_upload_files_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-c066e407a9"></a>`check` | `ck_collection_upload_files_raw_part_ordered_sha256_hex` | `CONSTRAINT ck_collection_upload_files_raw_part_ordered_sha256_hex CHECK (raw_part_ordered_sha256 IS NULL OR length(raw_part_ordered_sha256) = 64 AND lower(raw_part_ordered_sha256) = raw_part_ordered_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(raw_part_ordered_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-fe6739f7f4"></a>`check` | `ck_collection_upload_files_raw_part_commitment_sha256_hex` | `CONSTRAINT ck_collection_upload_files_raw_part_commitment_sha256_hex CHECK (raw_part_commitment_sha256 IS NULL OR length(raw_part_commitment_sha256) = 64 AND lower(raw_part_commitment_sha256) = raw_part_commitment_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(raw_part_commitment_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-2278dd5f6e"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/30`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 532162fc5685167b706efd51b7d7bfec7582117c5c9c6cadb4ea904603ce217d -->

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
      "definition": "path_sort_key BYTEA NOT NULL",
      "name": "path_sort_key",
      "nullable": false,
      "type": "BYTEA"
    },
    {
      "definition": "semantic_order_rank INTEGER NOT NULL",
      "name": "semantic_order_rank",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "file_order INTEGER NOT NULL",
      "name": "file_order",
      "nullable": false,
      "type": "INTEGER"
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
      "definition": "raw_part_plaintext_bytes BIGINT",
      "name": "raw_part_plaintext_bytes",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "raw_part_count BIGINT",
      "name": "raw_part_count",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "raw_part_ordered_sha256 VARCHAR(64)",
      "name": "raw_part_ordered_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "default": "0",
      "definition": "raw_parts_accepted BIGINT DEFAULT 0 NOT NULL",
      "name": "raw_parts_accepted",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "raw_part_commitment_sha256 VARCHAR(64)",
      "name": "raw_part_commitment_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "provenance_status VARCHAR NOT NULL",
      "name": "provenance_status",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "provenance_journal_id VARCHAR",
      "name": "provenance_journal_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "provenance_current_state_id VARCHAR",
      "name": "provenance_current_state_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "provenance_omission_reason TEXT",
      "name": "provenance_omission_reason",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "custodied_at VARCHAR",
      "name": "custodied_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "custody_receipt_json TEXT",
      "name": "custody_receipt_json",
      "nullable": true,
      "type": "TEXT"
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
      "definition": "CONSTRAINT ck_collection_upload_files_order CHECK (file_order >= 0)",
      "expression": "(file_order >= 0)",
      "kind": "check",
      "name": "ck_collection_upload_files_order"
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_files_semantic_order_rank CHECK (semantic_order_rank >= 0 AND semantic_order_rank <= 2)",
      "expression": "(semantic_order_rank >= 0 AND semantic_order_rank <= 2)",
      "kind": "check",
      "name": "ck_collection_upload_files_semantic_order_rank"
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_files_bytes CHECK (bytes >= 0)",
      "expression": "(bytes >= 0)",
      "kind": "check",
      "name": "ck_collection_upload_files_bytes"
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_files_raw_parts CHECK (raw_parts_accepted >= 0)",
      "expression": "(raw_parts_accepted >= 0)",
      "kind": "check",
      "name": "ck_collection_upload_files_raw_parts"
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_files_sha256 CHECK (length(sha256) = 64)",
      "expression": "(length(sha256) = 64)",
      "kind": "check",
      "name": "ck_collection_upload_files_sha256"
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_files_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_upload_files_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_files_raw_part_ordered_sha256_hex CHECK (raw_part_ordered_sha256 IS NULL OR length(raw_part_ordered_sha256) = 64 AND lower(raw_part_ordered_sha256) = raw_part_ordered_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(raw_part_ordered_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(raw_part_ordered_sha256 IS NULL OR length(raw_part_ordered_sha256) = 64 AND lower(raw_part_ordered_sha256) = raw_part_ordered_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(raw_part_ordered_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_upload_files_raw_part_ordered_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_files_raw_part_commitment_sha256_hex CHECK (raw_part_commitment_sha256 IS NULL OR length(raw_part_commitment_sha256) = 64 AND lower(raw_part_commitment_sha256) = raw_part_commitment_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(raw_part_commitment_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(raw_part_commitment_sha256 IS NULL OR length(raw_part_commitment_sha256) = 64 AND lower(raw_part_commitment_sha256) = raw_part_commitment_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(raw_part_commitment_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_upload_files_raw_part_commitment_sha256_hex"
    }
  ],
  "name": "collection_upload_files"
}
```

</details>
