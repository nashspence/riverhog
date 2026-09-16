# gogurt-listener: listener_meta

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:gogurt-listener:gogurt-listener-listener-meta:ea8d253c08 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-636a85068a"></a>

### Table: `listener_meta`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-0a6bcdef95"></a>`key` | `TEXT` | no | `—` | {"primary_key":true} |
| <a id="s-8705138789"></a>`value` | `TEXT` | no | `—` | — |

## Maintained corroboration

### Related interface records

- [Schema identity](gogurt-listener-durable-state-identity.md)

## Governing policies

- <a id="pa-a319da3b00"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:gogurt-listener](../../../evidence/sources.md#src-6b3ecfced3) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/listener.py`

### Machine authority

- `/external_contract/durable_state/owners/4/structure/tables/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 440c1b22dc97e9556b8d32034f51383a90afd424df17bf16ce1118abb62113f2 -->

```json
{
  "columns": [
    {
      "definition": "key TEXT NOT NULL PRIMARY KEY",
      "name": "key",
      "nullable": false,
      "primary_key": true,
      "type": "TEXT"
    },
    {
      "definition": "value TEXT NOT NULL",
      "name": "value",
      "nullable": false,
      "type": "TEXT"
    }
  ],
  "constraints": [],
  "name": "listener_meta"
}
```

</details>
