# riverhog-catalog: collection_provenance_journal_agents

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-provenance-jo-9c9be2cec8:97da077dc9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-b3ee13fb41"></a>

### Table: `collection_provenance_journal_agents`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-fcdf80f3e2"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-6e4b7c9414"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-05e24eb8bc"></a>`agent_id` | `VARCHAR` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-166cd00937"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, journal_id, agent_id)` |
| <a id="s-0c8b5c98c2"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, journal_id) REFERENCES collection_provenance_journals (collection_id, journal_id) ON DELETE CASCADE` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-57cce8a833"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/51`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd3f11abe121194d19f261e195d193f6758f47d2c8914c97c86f43d1ebd71a07 -->

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
      "definition": "agent_id VARCHAR NOT NULL",
      "name": "agent_id",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "journal_id",
        "agent_id"
      ],
      "definition": "PRIMARY KEY (collection_id, journal_id, agent_id)",
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
  "name": "collection_provenance_journal_agents"
}
```

</details>
