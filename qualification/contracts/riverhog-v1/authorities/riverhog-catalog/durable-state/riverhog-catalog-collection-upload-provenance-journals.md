# riverhog-catalog: collection_upload_provenance_journals

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-proven-802158f96a:911c99af68 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-a2db780a45"></a>

### Table: `collection_upload_provenance_journals`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1bb399350c"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-2e2de983dc"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-f01985f377"></a>`bytes` | `BIGINT` | no | `—` | — |
| <a id="s-89181597fc"></a>`sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-4c4d33766e"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-4e074482ca"></a>`accepted_bytes` | `BIGINT` | no | `0` | — |
| <a id="s-6774180e24"></a>`next_chunk_ordinal` | `VARCHAR(64)` | no | `'0000000000000000000000000000000000000000000000000000000000000000'` | — |
| <a id="s-1f0e96365d"></a>`content_hash_state` | `TEXT` | no | `—` | — |
| <a id="s-fb9cb459d6"></a>`validation_byte_offset` | `BIGINT` | no | `0` | — |
| <a id="s-ea51162aac"></a>`validation_sequence` | `BIGINT` | no | `0` | — |
| <a id="s-608541729c"></a>`validation_previous_entry_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-695c260670"></a>`validation_previous_json_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-d451d03c09"></a>`primary_lineage_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-115286a686"></a>`entity_counts_json` | `TEXT` | no | `'{}'` | — |
| <a id="s-e83fafb209"></a>`failure` | `TEXT` | yes | `—` | — |
| <a id="s-4a0a4f2ef2"></a>`current_state_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-f5ef6152f5"></a>`current_entry_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-802dcb28ed"></a>`current_entry_json_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-0a62bf0855"></a>`current_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-ab8c1867c5"></a>`current_bytes` | `BIGINT` | yes | `—` | — |
| <a id="s-10b3b05d74"></a>`current_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-30c57401b0"></a>`generated_output_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-40c08f7222"></a>`generation_after_journal_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-acae60f8e4"></a>`generation_after_state_id` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-00a009e43e"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, journal_id)` |
| <a id="s-ed22eff2e3"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_uploads (collection_id) ON DELETE CASCADE` |
| <a id="s-a9dedea45a"></a>`check` | `ck_upload_provenance_journals_bytes` | `CONSTRAINT ck_upload_provenance_journals_bytes CHECK (bytes >= 0)` |
| <a id="s-be50a8c351"></a>`check` | `ck_upload_provenance_journals_accepted_bytes` | `CONSTRAINT ck_upload_provenance_journals_accepted_bytes CHECK (accepted_bytes >= 0 AND accepted_bytes <= bytes)` |
| <a id="s-0ba57b918c"></a>`check` | `ck_upload_provenance_journals_next_chunk_ordinal` | `CONSTRAINT ck_upload_provenance_journals_next_chunk_ordinal CHECK (length(next_chunk_ordinal) = 64 AND lower(next_chunk_ordinal) = next_chunk_ordinal AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(next_chunk_ordinal, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-2d54d01f5e"></a>`check` | `ck_upload_provenance_journals_validation_offset` | `CONSTRAINT ck_upload_provenance_journals_validation_offset CHECK (validation_byte_offset >= 0 AND validation_byte_offset <= accepted_bytes)` |
| <a id="s-83030ff3c8"></a>`check` | `ck_upload_provenance_journals_validation_sequence` | `CONSTRAINT ck_upload_provenance_journals_validation_sequence CHECK (validation_sequence >= 0)` |
| <a id="s-8cbaef8329"></a>`check` | `ck_upload_provenance_journals_state` | `CONSTRAINT ck_upload_provenance_journals_state CHECK (state IN ('accepting','generating','validating','sealed','failed'))` |
| <a id="s-71e81b61d3"></a>`check` | `ck_upload_provenance_journals_current_bytes` | `CONSTRAINT ck_upload_provenance_journals_current_bytes CHECK (current_bytes IS NULL OR current_bytes >= 0)` |
| <a id="s-87eba8a898"></a>`check` | `ck_upload_provenance_journals_sha256` | `CONSTRAINT ck_upload_provenance_journals_sha256 CHECK (length(sha256) = 64)` |
| <a id="s-b5781b81e0"></a>`check` | `ck_collection_upload_provenance_journals_sha256_hex` | `CONSTRAINT ck_collection_upload_provenance_journals_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-8e07101763"></a>`check` | `ck_sha256_164241c9bc3b84b3` | `CONSTRAINT ck_sha256_164241c9bc3b84b3 CHECK (validation_previous_json_sha256 IS NULL OR length(validation_previous_json_sha256) = 64 AND lower(validation_previous_json_sha256) = validation_previous_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(validation_previous_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-8133c66e96"></a>`check` | `ck_sha256_beecad13199605d9` | `CONSTRAINT ck_sha256_beecad13199605d9 CHECK (current_entry_json_sha256 IS NULL OR length(current_entry_json_sha256) = 64 AND lower(current_entry_json_sha256) = current_entry_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(current_entry_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-4668a1e92a"></a>`check` | `ck_collection_upload_provenance_journals_current_sha256_hex` | `CONSTRAINT ck_collection_upload_provenance_journals_current_sha256_hex CHECK (current_sha256 IS NULL OR length(current_sha256) = 64 AND lower(current_sha256) = current_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(current_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-213b2ff6c7"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/32`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 81ef884e1554fd867c47e07d782de8ca9bc49a219e7b2d552fc5dea94b1beb94 -->

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
      "definition": "journal_id VARCHAR NOT NULL",
      "name": "journal_id",
      "nullable": false,
      "type": "VARCHAR"
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
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "default": "0",
      "definition": "accepted_bytes BIGINT DEFAULT 0 NOT NULL",
      "name": "accepted_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "default": "'0000000000000000000000000000000000000000000000000000000000000000'",
      "definition": "next_chunk_ordinal VARCHAR(64) DEFAULT '0000000000000000000000000000000000000000000000000000000000000000' NOT NULL",
      "name": "next_chunk_ordinal",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "content_hash_state TEXT NOT NULL",
      "name": "content_hash_state",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "default": "0",
      "definition": "validation_byte_offset BIGINT DEFAULT 0 NOT NULL",
      "name": "validation_byte_offset",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "default": "0",
      "definition": "validation_sequence BIGINT DEFAULT 0 NOT NULL",
      "name": "validation_sequence",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "validation_previous_entry_id VARCHAR",
      "name": "validation_previous_entry_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "validation_previous_json_sha256 VARCHAR(64)",
      "name": "validation_previous_json_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "primary_lineage_id VARCHAR",
      "name": "primary_lineage_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "default": "'{}'",
      "definition": "entity_counts_json TEXT DEFAULT '{}' NOT NULL",
      "name": "entity_counts_json",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "failure TEXT",
      "name": "failure",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "current_state_id VARCHAR",
      "name": "current_state_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "current_entry_id VARCHAR",
      "name": "current_entry_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "current_entry_json_sha256 VARCHAR(64)",
      "name": "current_entry_json_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "current_path VARCHAR",
      "name": "current_path",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "current_bytes BIGINT",
      "name": "current_bytes",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "current_sha256 VARCHAR(64)",
      "name": "current_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "generated_output_path VARCHAR",
      "name": "generated_output_path",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "generation_after_journal_id VARCHAR",
      "name": "generation_after_journal_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "generation_after_state_id VARCHAR",
      "name": "generation_after_state_id",
      "nullable": true,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "journal_id"
      ],
      "definition": "PRIMARY KEY (collection_id, journal_id)",
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
      "definition": "CONSTRAINT ck_upload_provenance_journals_bytes CHECK (bytes >= 0)",
      "expression": "(bytes >= 0)",
      "kind": "check",
      "name": "ck_upload_provenance_journals_bytes"
    },
    {
      "definition": "CONSTRAINT ck_upload_provenance_journals_accepted_bytes CHECK (accepted_bytes >= 0 AND accepted_bytes <= bytes)",
      "expression": "(accepted_bytes >= 0 AND accepted_bytes <= bytes)",
      "kind": "check",
      "name": "ck_upload_provenance_journals_accepted_bytes"
    },
    {
      "definition": "CONSTRAINT ck_upload_provenance_journals_next_chunk_ordinal CHECK (length(next_chunk_ordinal) = 64 AND lower(next_chunk_ordinal) = next_chunk_ordinal AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(next_chunk_ordinal, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(next_chunk_ordinal) = 64 AND lower(next_chunk_ordinal) = next_chunk_ordinal AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(next_chunk_ordinal, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_upload_provenance_journals_next_chunk_ordinal"
    },
    {
      "definition": "CONSTRAINT ck_upload_provenance_journals_validation_offset CHECK (validation_byte_offset >= 0 AND validation_byte_offset <= accepted_bytes)",
      "expression": "(validation_byte_offset >= 0 AND validation_byte_offset <= accepted_bytes)",
      "kind": "check",
      "name": "ck_upload_provenance_journals_validation_offset"
    },
    {
      "definition": "CONSTRAINT ck_upload_provenance_journals_validation_sequence CHECK (validation_sequence >= 0)",
      "expression": "(validation_sequence >= 0)",
      "kind": "check",
      "name": "ck_upload_provenance_journals_validation_sequence"
    },
    {
      "definition": "CONSTRAINT ck_upload_provenance_journals_state CHECK (state IN ('accepting','generating','validating','sealed','failed'))",
      "expression": "(state IN ('accepting','generating','validating','sealed','failed'))",
      "kind": "check",
      "name": "ck_upload_provenance_journals_state"
    },
    {
      "definition": "CONSTRAINT ck_upload_provenance_journals_current_bytes CHECK (current_bytes IS NULL OR current_bytes >= 0)",
      "expression": "(current_bytes IS NULL OR current_bytes >= 0)",
      "kind": "check",
      "name": "ck_upload_provenance_journals_current_bytes"
    },
    {
      "definition": "CONSTRAINT ck_upload_provenance_journals_sha256 CHECK (length(sha256) = 64)",
      "expression": "(length(sha256) = 64)",
      "kind": "check",
      "name": "ck_upload_provenance_journals_sha256"
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_provenance_journals_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_upload_provenance_journals_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_sha256_164241c9bc3b84b3 CHECK (validation_previous_json_sha256 IS NULL OR length(validation_previous_json_sha256) = 64 AND lower(validation_previous_json_sha256) = validation_previous_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(validation_previous_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(validation_previous_json_sha256 IS NULL OR length(validation_previous_json_sha256) = 64 AND lower(validation_previous_json_sha256) = validation_previous_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(validation_previous_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_164241c9bc3b84b3"
    },
    {
      "definition": "CONSTRAINT ck_sha256_beecad13199605d9 CHECK (current_entry_json_sha256 IS NULL OR length(current_entry_json_sha256) = 64 AND lower(current_entry_json_sha256) = current_entry_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(current_entry_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(current_entry_json_sha256 IS NULL OR length(current_entry_json_sha256) = 64 AND lower(current_entry_json_sha256) = current_entry_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(current_entry_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_beecad13199605d9"
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_provenance_journals_current_sha256_hex CHECK (current_sha256 IS NULL OR length(current_sha256) = 64 AND lower(current_sha256) = current_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(current_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(current_sha256 IS NULL OR length(current_sha256) = 64 AND lower(current_sha256) = current_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(current_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_upload_provenance_journals_current_sha256_hex"
    }
  ],
  "name": "collection_upload_provenance_journals"
}
```

</details>
