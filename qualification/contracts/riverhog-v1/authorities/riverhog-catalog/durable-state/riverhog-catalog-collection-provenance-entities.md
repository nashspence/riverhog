# riverhog-catalog: collection_provenance_entities

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-provenance-entities:da83c1628f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-1d977b4003"></a>

### Table: `collection_provenance_entities`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-d74300e3b0"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-863018529b"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-7527d8fd88"></a>`entity_type` | `VARCHAR` | no | `—` | — |
| <a id="s-bf9e18802a"></a>`entity_id` | `VARCHAR` | no | `—` | — |
| <a id="s-c10a8e8fa3"></a>`entry_id` | `VARCHAR` | no | `—` | — |
| <a id="s-6a3e4c9479"></a>`document_json` | `TEXT` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-c4569520c9"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, journal_id, entity_type, entity_id)` |
| <a id="s-98337ecde5"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, journal_id) REFERENCES collection_provenance_journals (collection_id, journal_id) ON DELETE CASCADE` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-e9cb57d5dd"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/49`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dfabcde85e075dc2eb932e7ef2319c7f597ecc108e15083c24876430f40df330 -->

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
      "definition": "entity_type VARCHAR NOT NULL",
      "name": "entity_type",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "entity_id VARCHAR NOT NULL",
      "name": "entity_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "entry_id VARCHAR NOT NULL",
      "name": "entry_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "document_json TEXT NOT NULL",
      "name": "document_json",
      "nullable": false,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "journal_id",
        "entity_type",
        "entity_id"
      ],
      "definition": "PRIMARY KEY (collection_id, journal_id, entity_type, entity_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id",
        "journal_id"
      ],
      "definition": "FOREIGN KEY(collection_id, journal_id) REFERENCES collection_provenance_journals (collection_id, journal_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id",
          "journal_id"
        ],
        "table": "collection_provenance_journals"
      }
    }
  ],
  "name": "collection_provenance_entities"
}
```

</details>
