# gogurt-listener: observed_mounts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:gogurt-listener:gogurt-listener-observed-mounts:54790297ca -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-1982d018ff"></a>

### Table: `observed_mounts`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-4342ec0d6f"></a>`mount_point` | `TEXT` | no | `—` | {"primary_key":true} |
| <a id="s-7744cf4205"></a>`present` | `INTEGER` | no | `—` | {"checks":["(present IN (0, 1))"]} |
| <a id="s-907a253d6d"></a>`generation` | `INTEGER` | no | `—` | {"checks":["(generation >= 1)"]} |
| <a id="s-fd136fa2a9"></a>`marker_identity` | `TEXT` | yes | `—` | — |

## Maintained corroboration

### Related interface records

- [Schema identity](gogurt-listener-durable-state-identity.md)

## Governing policies

- <a id="pa-e4fb6a8fc8"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:gogurt-listener](../../../evidence/sources.md#src-6b3ecfced3) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/listener.py`

### Machine authority

- `/external_contract/durable_state/owners/4/structure/tables/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9a8d1ec5c8b774cfe9fd50d0d1da9e9021722f8f7e40dd6dd9c773a02dc642d0 -->

```json
{
  "columns": [
    {
      "definition": "mount_point TEXT NOT NULL PRIMARY KEY",
      "name": "mount_point",
      "nullable": false,
      "primary_key": true,
      "type": "TEXT"
    },
    {
      "checks": [
        "(present IN (0, 1))"
      ],
      "definition": "present INTEGER NOT NULL CHECK (present IN (0, 1))",
      "name": "present",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "checks": [
        "(generation >= 1)"
      ],
      "definition": "generation INTEGER NOT NULL CHECK (generation >= 1)",
      "name": "generation",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "marker_identity TEXT",
      "name": "marker_identity",
      "nullable": true,
      "type": "TEXT"
    }
  ],
  "constraints": [],
  "name": "observed_mounts"
}
```

</details>
