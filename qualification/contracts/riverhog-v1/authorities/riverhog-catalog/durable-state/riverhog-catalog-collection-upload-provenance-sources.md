# riverhog-catalog: collection_upload_provenance_sources

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-proven-7e0f929438:fa6f4427ea -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-35e966f894"></a>

### Table: `collection_upload_provenance_sources`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-ea493bbffc"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-29c9d79c71"></a>`source_collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-3717432186"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-dad8e45b59"></a>`expanded` | `BOOLEAN` | no | `false` | — |
| <a id="s-bc6c485679"></a>`after_to_journal_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-898380b330"></a>`after_entry_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-006634d24d"></a>`after_state_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-7aee800e69"></a>`copied` | `BOOLEAN` | no | `false` | — |
| <a id="s-462c77a378"></a>`copy_offset` | `BIGINT` | no | `0` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-7241ef41e2"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, source_collection_id, journal_id)` |
| <a id="s-cbb2e7b7d5"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_uploads (collection_id) ON DELETE CASCADE` |
| <a id="s-7340734734"></a>`foreign-key` | `—` | `FOREIGN KEY(source_collection_id, journal_id) REFERENCES collection_provenance_journals (collection_id, journal_id) ON DELETE RESTRICT` |
| <a id="s-ffbf62fbfe"></a>`check` | `ck_upload_provenance_sources_offset` | `CONSTRAINT ck_upload_provenance_sources_offset CHECK (copy_offset >= 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-92c4702d99"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/66`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a3fee45f89ab71519df79aed2cd54e946c4c02a4efd6f442f27bb5c45175de10 -->

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
      "definition": "source_collection_id BIGINT NOT NULL",
      "name": "source_collection_id",
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
      "default": "false",
      "definition": "expanded BOOLEAN DEFAULT false NOT NULL",
      "name": "expanded",
      "nullable": false,
      "type": "BOOLEAN"
    },
    {
      "definition": "after_to_journal_id VARCHAR",
      "name": "after_to_journal_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "after_entry_id VARCHAR",
      "name": "after_entry_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "after_state_id VARCHAR",
      "name": "after_state_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "default": "false",
      "definition": "copied BOOLEAN DEFAULT false NOT NULL",
      "name": "copied",
      "nullable": false,
      "type": "BOOLEAN"
    },
    {
      "default": "0",
      "definition": "copy_offset BIGINT DEFAULT 0 NOT NULL",
      "name": "copy_offset",
      "nullable": false,
      "type": "BIGINT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "source_collection_id",
        "journal_id"
      ],
      "definition": "PRIMARY KEY (collection_id, source_collection_id, journal_id)",
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
      "columns": [
        "source_collection_id",
        "journal_id"
      ],
      "definition": "FOREIGN KEY(source_collection_id, journal_id) REFERENCES collection_provenance_journals (collection_id, journal_id) ON DELETE RESTRICT",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE RESTRICT",
        "columns": [
          "collection_id",
          "journal_id"
        ],
        "table": "collection_provenance_journals"
      }
    },
    {
      "definition": "CONSTRAINT ck_upload_provenance_sources_offset CHECK (copy_offset >= 0)",
      "expression": "(copy_offset >= 0)",
      "kind": "check",
      "name": "ck_upload_provenance_sources_offset"
    }
  ],
  "name": "collection_upload_provenance_sources"
}
```

</details>
