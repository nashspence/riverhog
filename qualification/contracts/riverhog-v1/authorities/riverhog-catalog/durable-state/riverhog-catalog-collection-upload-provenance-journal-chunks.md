# riverhog-catalog: collection_upload_provenance_journal_chunks

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-proven-70ee607731:3d2f14b3aa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-14a1d0b03b"></a>
- Table: `collection_upload_provenance_journal_chunks`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-511a1c3d43"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-07a19f8cac"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-e5d2e81b45"></a>`ordinal` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-a29f71867b"></a>`byte_offset` | `BIGINT` | no | `—` | — |
| <a id="s-39e90c3d97"></a>`content` | `BYTEA` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-d17edcac9e"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, journal_id, ordinal)` |
| <a id="s-634d13d6c4"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, journal_id) REFERENCES collection_upload_provenance_journals (collection_id, journal_id) ON DELETE CASCADE` |
| <a id="s-8772df329a"></a>`check` | `ck_upload_provenance_journal_chunks_ordinal` | `CONSTRAINT ck_upload_provenance_journal_chunks_ordinal CHECK (length(ordinal) = 64 AND lower(ordinal) = ordinal AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ordinal, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-089433720a"></a>`check` | `ck_upload_provenance_journal_chunks_offset` | `CONSTRAINT ck_upload_provenance_journal_chunks_offset CHECK (byte_offset >= 0)` |
| <a id="s-243ab0fdd4"></a>`check` | `ck_upload_provenance_journal_chunks_content` | `CONSTRAINT ck_upload_provenance_journal_chunks_content CHECK (length(content) > 0)` |

## Maintained corroboration

### Related interface records

- [riverhog-catalog durable-state identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-406ebf0228"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/62`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e6992e8aca9f0e033e0186ecd9392dce2682176e3561004351a2112cac8c1d57 -->

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
      "definition": "ordinal VARCHAR(64) NOT NULL",
      "name": "ordinal",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "byte_offset BIGINT NOT NULL",
      "name": "byte_offset",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "content BYTEA NOT NULL",
      "name": "content",
      "nullable": false,
      "type": "BYTEA"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "journal_id",
        "ordinal"
      ],
      "definition": "PRIMARY KEY (collection_id, journal_id, ordinal)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id",
        "journal_id"
      ],
      "definition": "FOREIGN KEY(collection_id, journal_id) REFERENCES collection_upload_provenance_journals (collection_id, journal_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id",
          "journal_id"
        ],
        "table": "collection_upload_provenance_journals"
      }
    },
    {
      "definition": "CONSTRAINT ck_upload_provenance_journal_chunks_ordinal CHECK (length(ordinal) = 64 AND lower(ordinal) = ordinal AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ordinal, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(ordinal) = 64 AND lower(ordinal) = ordinal AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ordinal, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_upload_provenance_journal_chunks_ordinal"
    },
    {
      "definition": "CONSTRAINT ck_upload_provenance_journal_chunks_offset CHECK (byte_offset >= 0)",
      "expression": "(byte_offset >= 0)",
      "kind": "check",
      "name": "ck_upload_provenance_journal_chunks_offset"
    },
    {
      "definition": "CONSTRAINT ck_upload_provenance_journal_chunks_content CHECK (length(content) > 0)",
      "expression": "(length(content) > 0)",
      "kind": "check",
      "name": "ck_upload_provenance_journal_chunks_content"
    }
  ],
  "name": "collection_upload_provenance_journal_chunks"
}
```
