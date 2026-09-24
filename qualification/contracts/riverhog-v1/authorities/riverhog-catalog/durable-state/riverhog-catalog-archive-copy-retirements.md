# riverhog-catalog: archive_copy_retirements

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-archive-copy-retirements:c7fd950a35 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-2ebafc64bb"></a>

### Table: `archive_copy_retirements`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-f63ffef409"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-57624bf7ea"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-5cdf070c38"></a>`challenge` | `VARCHAR` | no | `—` | — |
| <a id="s-4feee4738f"></a>`plan_json` | `TEXT` | no | `—` | — |
| <a id="s-5f19f7960d"></a>`started_at` | `VARCHAR` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-464336e6f6"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store)` |
| <a id="s-360f676840"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, store) REFERENCES collection_archive_copies (collection_id, store) ON DELETE CASCADE` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-f167f8dc7e"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/41`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa36175648284b8fd0498175538998bf7faad12308390c7fe455c5bd6b949037 -->

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
      "definition": "challenge VARCHAR NOT NULL",
      "name": "challenge",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "plan_json TEXT NOT NULL",
      "name": "plan_json",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "started_at VARCHAR NOT NULL",
      "name": "started_at",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "store"
      ],
      "definition": "PRIMARY KEY (collection_id, store)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id",
        "store"
      ],
      "definition": "FOREIGN KEY(collection_id, store) REFERENCES collection_archive_copies (collection_id, store) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id",
          "store"
        ],
        "table": "collection_archive_copies"
      }
    }
  ],
  "name": "archive_copy_retirements"
}
```

</details>
