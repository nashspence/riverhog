# stove0-control: stove0_lifecycle_events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-lifecycle-events:820f9789aa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-1a24f3ea03"></a>

### Table: `stove0_lifecycle_events`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-0a70aa7b1a"></a>`sequence` | `SERIAL` | no | `—` | — |
| <a id="s-65a1cd9f71"></a>`created_at` | `VARCHAR(40)` | no | `—` | — |
| <a id="s-dfc00cda2e"></a>`event_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-0a67f5efaa"></a>`event_json` | `TEXT` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-39f07eb53f"></a>`primary-key` | `—` | `PRIMARY KEY (sequence)` |
| <a id="s-8044b13c40"></a>`check` | `ck_stove0_lifecycle_events_event_bytes` | `CONSTRAINT ck_stove0_lifecycle_events_event_bytes CHECK (event_bytes >= 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-7c76b3f363"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:stove0-control](../../../evidence/sources.md#src-45e44b17fd) — `reference/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/3/structure/tables/7`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7973e37f629b9c80ba09bcc5c0cdc2cc476a9277767003a4e27c0692aae71fcd -->

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
      "definition": "created_at VARCHAR(40) NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR(40)"
    },
    {
      "definition": "event_bytes BIGINT NOT NULL",
      "name": "event_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "event_json TEXT NOT NULL",
      "name": "event_json",
      "nullable": false,
      "type": "TEXT"
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
      "definition": "CONSTRAINT ck_stove0_lifecycle_events_event_bytes CHECK (event_bytes >= 0)",
      "expression": "(event_bytes >= 0)",
      "kind": "check",
      "name": "ck_stove0_lifecycle_events_event_bytes"
    }
  ],
  "name": "stove0_lifecycle_events"
}
```

</details>
