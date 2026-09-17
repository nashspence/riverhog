# riverhog-catalog: collection_upload_provenance_journals

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-proven-802158f96a:6d6ce569e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-0a7b502c4e"></a>

### Table: `collection_upload_provenance_journals`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-5fb26066c8"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-96b02c1cad"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-421af34327"></a>`bytes` | `BIGINT` | no | `—` | — |
| <a id="s-7e8d32cfa9"></a>`sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-ac3195928b"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-6c2e636236"></a>`accepted_bytes` | `BIGINT` | no | `0` | — |
| <a id="s-f702a22856"></a>`next_chunk_ordinal` | `VARCHAR(64)` | no | `'0000000000000000000000000000000000000000000000000000000000000000'` | — |
| <a id="s-bb8fde4e17"></a>`content_hash_state` | `TEXT` | no | `—` | — |
| <a id="s-62a1cca1e7"></a>`validation_byte_offset` | `BIGINT` | no | `0` | — |
| <a id="s-994c30a83b"></a>`validation_sequence` | `BIGINT` | no | `0` | — |
| <a id="s-29e4238001"></a>`validation_previous_entry_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-3b9243d7b9"></a>`validation_previous_json_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-ac8cf94016"></a>`primary_lineage_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-74a4dd4ea9"></a>`entity_counts_json` | `TEXT` | no | `'{}'` | — |
| <a id="s-dbefe2bfa6"></a>`failure` | `TEXT` | yes | `—` | — |
| <a id="s-b37bd6043f"></a>`current_state_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-33ff573e88"></a>`current_entry_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-32ecf49e80"></a>`current_entry_json_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-8681b45c02"></a>`current_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-bced564447"></a>`current_bytes` | `BIGINT` | yes | `—` | — |
| <a id="s-124f5c5e31"></a>`current_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-2db47f7470"></a>`generated_output_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-f3cbea9335"></a>`generation_after_journal_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-737b81dbdb"></a>`generation_after_state_id` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-62f9c0d071"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, journal_id)` |
| <a id="s-0e4eccc667"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_uploads (collection_id) ON DELETE CASCADE` |
| <a id="s-26369da6d0"></a>`check` | `ck_upload_provenance_journals_bytes` | `CONSTRAINT ck_upload_provenance_journals_bytes CHECK (bytes >= 0)` |
| <a id="s-b63cf1b5d9"></a>`check` | `ck_upload_provenance_journals_accepted_bytes` | `CONSTRAINT ck_upload_provenance_journals_accepted_bytes CHECK (accepted_bytes >= 0 AND accepted_bytes <= bytes)` |
| <a id="s-02173694c3"></a>`check` | `ck_upload_provenance_journals_next_chunk_ordinal` | `CONSTRAINT ck_upload_provenance_journals_next_chunk_ordinal CHECK (length(next_chunk_ordinal) = 64 AND lower(next_chunk_ordinal) = next_chunk_ordinal AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(next_chunk_ordinal, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-a7eb28e2d6"></a>`check` | `ck_upload_provenance_journals_validation_offset` | `CONSTRAINT ck_upload_provenance_journals_validation_offset CHECK (validation_byte_offset >= 0 AND validation_byte_offset <= accepted_bytes)` |
| <a id="s-fcfbd2e204"></a>`check` | `ck_upload_provenance_journals_validation_sequence` | `CONSTRAINT ck_upload_provenance_journals_validation_sequence CHECK (validation_sequence >= 0)` |
| <a id="s-a8354d32bb"></a>`check` | `ck_upload_provenance_journals_state` | `CONSTRAINT ck_upload_provenance_journals_state CHECK (state IN ('accepting','generating','validating','sealed','failed'))` |
| <a id="s-c066e407a9"></a>`check` | `ck_upload_provenance_journals_current_bytes` | `CONSTRAINT ck_upload_provenance_journals_current_bytes CHECK (current_bytes IS NULL OR current_bytes >= 0)` |
| <a id="s-fe6739f7f4"></a>`check` | `ck_upload_provenance_journals_sha256` | `CONSTRAINT ck_upload_provenance_journals_sha256 CHECK (length(sha256) = 64)` |
| <a id="s-ba55d0a0aa"></a>`check` | `ck_collection_upload_provenance_journals_sha256_hex` | `CONSTRAINT ck_collection_upload_provenance_journals_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-ab3a68eb28"></a>`check` | `ck_sha256_164241c9bc3b84b3` | `CONSTRAINT ck_sha256_164241c9bc3b84b3 CHECK (validation_previous_json_sha256 IS NULL OR length(validation_previous_json_sha256) = 64 AND lower(validation_previous_json_sha256) = validation_previous_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(validation_previous_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-1285d4f995"></a>`check` | `ck_sha256_beecad13199605d9` | `CONSTRAINT ck_sha256_beecad13199605d9 CHECK (current_entry_json_sha256 IS NULL OR length(current_entry_json_sha256) = 64 AND lower(current_entry_json_sha256) = current_entry_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(current_entry_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-d7efd13ed5"></a>`check` | `ck_collection_upload_provenance_journals_current_sha256_hex` | `CONSTRAINT ck_collection_upload_provenance_journals_current_sha256_hex CHECK (current_sha256 IS NULL OR length(current_sha256) = 64 AND lower(current_sha256) = current_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(current_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-677ddb002b"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/30`

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
