# riverhog-catalog: collection_upload_provenance_journal_chunks

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-proven-70ee607731:d7025dd2b8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-d332239d07"></a>

### Table: `collection_upload_provenance_journal_chunks`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-da0d3b8e3f"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-bf2d869b1b"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-dc393254ef"></a>`ordinal` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-6bc9e37048"></a>`byte_offset` | `BIGINT` | no | `—` | — |
| <a id="s-34c470b2f3"></a>`content` | `BYTEA` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-0200030ffa"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, journal_id, ordinal)` |
| <a id="s-a6fcd34690"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, journal_id) REFERENCES collection_upload_provenance_journals (collection_id, journal_id) ON DELETE CASCADE` |
| <a id="s-351e0c5a9e"></a>`check` | `ck_upload_provenance_journal_chunks_ordinal` | `CONSTRAINT ck_upload_provenance_journal_chunks_ordinal CHECK (length(ordinal) = 64 AND lower(ordinal) = ordinal AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ordinal, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-65cc30834f"></a>`check` | `ck_upload_provenance_journal_chunks_offset` | `CONSTRAINT ck_upload_provenance_journal_chunks_offset CHECK (byte_offset >= 0)` |
| <a id="s-b8049e4d7f"></a>`check` | `ck_upload_provenance_journal_chunks_content` | `CONSTRAINT ck_upload_provenance_journal_chunks_content CHECK (length(content) > 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-74d7705c35"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/64`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
