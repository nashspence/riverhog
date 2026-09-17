# riverhog-catalog: lifecycle_events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-lifecycle-events:5f334b1cf0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-c29b83c623"></a>

### Table: `lifecycle_events`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-3ac7e44768"></a>`sequence` | `SERIAL` | no | `—` | — |
| <a id="s-884731af6c"></a>`event_id` | `VARCHAR` | no | `—` | — |
| <a id="s-2e0b94d2ef"></a>`owner_app` | `VARCHAR` | no | `—` | — |
| <a id="s-e59568dd7c"></a>`subject` | `VARCHAR` | yes | `—` | — |
| <a id="s-dbd374c071"></a>`event_json` | `TEXT` | no | `—` | — |
| <a id="s-cefb50b33c"></a>`context_json` | `TEXT` | yes | `—` | — |
| <a id="s-15ca3fce71"></a>`context_expires_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-a0c685c4a7"></a>`primary-key` | `—` | `PRIMARY KEY (sequence)` |
| <a id="s-6330a66a8e"></a>`unique` | `—` | `UNIQUE (event_id)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-b20e6d1eba"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/12`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 19581ffe20f074c4cb3ecc33f3c352f7e82ed8bc1f1eb748c52a56403f5016e5 -->

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
      "definition": "owner_app VARCHAR NOT NULL",
      "name": "owner_app",
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
