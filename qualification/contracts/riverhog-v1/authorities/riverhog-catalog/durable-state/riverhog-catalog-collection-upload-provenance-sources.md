# riverhog-catalog: collection_upload_provenance_sources

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-proven-7e0f929438:e7cc394b08 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-d332239d07"></a>

### Table: `collection_upload_provenance_sources`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-da0d3b8e3f"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-bf2d869b1b"></a>`source_collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-dc393254ef"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-6bc9e37048"></a>`expanded` | `BOOLEAN` | no | `false` | — |
| <a id="s-34c470b2f3"></a>`after_to_journal_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-d5d8bf9e94"></a>`after_entry_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-8bc40d099b"></a>`after_state_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-8684c316a7"></a>`copied` | `BOOLEAN` | no | `false` | — |
| <a id="s-d5c140fadd"></a>`copy_offset` | `BIGINT` | no | `0` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-0200030ffa"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, source_collection_id, journal_id)` |
| <a id="s-a6fcd34690"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_uploads (collection_id) ON DELETE CASCADE` |
| <a id="s-351e0c5a9e"></a>`foreign-key` | `—` | `FOREIGN KEY(source_collection_id, journal_id) REFERENCES collection_provenance_journals (collection_id, journal_id) ON DELETE RESTRICT` |
| <a id="s-65cc30834f"></a>`check` | `ck_upload_provenance_sources_offset` | `CONSTRAINT ck_upload_provenance_sources_offset CHECK (copy_offset >= 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-71d899abca"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

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
