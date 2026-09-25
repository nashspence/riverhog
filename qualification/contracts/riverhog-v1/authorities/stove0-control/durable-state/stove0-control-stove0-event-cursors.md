# stove0-control: stove0_event_cursors

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-event-cursors:9430e3422c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-ac22ba19eb"></a>

### Table: `stove0_event_cursors`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-afae72fadc"></a>`stream` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-99371c1362"></a>`cursor` | `VARCHAR(500)` | no | `—` | — |
| <a id="s-61f80863fb"></a>`revision` | `INTEGER` | no | `—` | — |
| <a id="s-7591af686a"></a>`updated_at` | `VARCHAR(40)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-d5017eec9a"></a>`primary-key` | `—` | `PRIMARY KEY (stream)` |
| <a id="s-de45cfe67c"></a>`check` | `ck_stove0_event_cursors_revision` | `CONSTRAINT ck_stove0_event_cursors_revision CHECK (revision >= 1)` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-19c9ccc85f"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/5/structure/tables/9`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3620e611a37c74f2a12118ec0764392c3eb08d300e5043c3cd65d9552a8a005c -->

```json
{
  "columns": [
    {
      "definition": "stream VARCHAR(160) NOT NULL",
      "name": "stream",
      "nullable": false,
      "type": "VARCHAR(160)"
    },
    {
      "definition": "cursor VARCHAR(500) NOT NULL",
      "name": "cursor",
      "nullable": false,
      "type": "VARCHAR(500)"
    },
    {
      "definition": "revision INTEGER NOT NULL",
      "name": "revision",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "updated_at VARCHAR(40) NOT NULL",
      "name": "updated_at",
      "nullable": false,
      "type": "VARCHAR(40)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "stream"
      ],
      "definition": "PRIMARY KEY (stream)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_stove0_event_cursors_revision CHECK (revision >= 1)",
      "expression": "(revision >= 1)",
      "kind": "check",
      "name": "ck_stove0_event_cursors_revision"
    }
  ],
  "name": "stove0_event_cursors"
}
```

</details>
