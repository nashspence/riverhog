# riverhog-catalog: lifecycle_events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-lifecycle-events:784e0ee283 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-5367ff929e"></a>

### Table: `lifecycle_events`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-8c8a1721c6"></a>`sequence` | `SERIAL` | no | `—` | — |
| <a id="s-bd76206005"></a>`event_id` | `VARCHAR` | no | `—` | — |
| <a id="s-ab21e4863b"></a>`owner_principal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-0c48fe4f11"></a>`subject` | `VARCHAR` | yes | `—` | — |
| <a id="s-dced5fb5ac"></a>`event_json` | `TEXT` | no | `—` | — |
| <a id="s-a4d897bdd3"></a>`context_json` | `TEXT` | yes | `—` | — |
| <a id="s-cbf7c84e21"></a>`context_expires_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-a65fc8e13e"></a>`primary-key` | `—` | `PRIMARY KEY (sequence)` |
| <a id="s-da7808fbdc"></a>`unique` | `—` | `UNIQUE (event_id)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-2aa05fc618"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/14`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b4796026a4c086acf22889a6078d43b2d99f475cba99fc1290f7e12b0c812efd -->

```json
{
  "columns": [
    {
      "definition": "sequence SERIAL NOT NULL",
      "name": "sequence",
      "nullable": false,
      "type": "SERIAL"
    },
    {
      "definition": "event_id VARCHAR NOT NULL",
      "name": "event_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "owner_principal_id VARCHAR NOT NULL",
      "name": "owner_principal_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "subject VARCHAR",
      "name": "subject",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "event_json TEXT NOT NULL",
      "name": "event_json",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "context_json TEXT",
      "name": "context_json",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "context_expires_at VARCHAR",
      "name": "context_expires_at",
      "nullable": true,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "sequence"
      ],
      "definition": "PRIMARY KEY (sequence)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "event_id"
      ],
      "definition": "UNIQUE (event_id)",
      "kind": "unique"
    }
  ],
  "name": "lifecycle_events"
}
```

</details>
