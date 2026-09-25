# gogurt-listener: dispatches

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:gogurt-listener:gogurt-listener-dispatches:a44bb60fc3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-d23dd460cd"></a>

### Table: `dispatches`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-30c311fcc3"></a>`dispatch_id` | `TEXT` | no | `—` | {"primary_key":true} |
| <a id="s-1f94f6115e"></a>`mount_point` | `TEXT` | no | `—` | — |
| <a id="s-dd4e8e0b50"></a>`generation` | `INTEGER` | no | `—` | {"checks":["(generation >= 1)"]} |
| <a id="s-46ebe8a40e"></a>`marker_identity` | `TEXT` | no | `—` | — |
| <a id="s-46780e5fb0"></a>`route` | `TEXT` | no | `—` | — |
| <a id="s-e5565c959a"></a>`plan_json` | `TEXT` | no | `—` | {"checks":["(json_valid(plan_json))"]} |
| <a id="s-6ce781081f"></a>`state` | `TEXT` | no | `—` | {"checks":["(\n        state IN ('queued', 'running', 'retry', 'uncertain', 'completed', 'failed')\n    )"]} |
| <a id="s-dad5c3530f"></a>`attempts` | `INTEGER` | no | `0` | {"checks":["(attempts >= 0)"]} |
| <a id="s-d178e919ed"></a>`observed_at` | `REAL` | no | `—` | — |
| <a id="s-980a917893"></a>`started_at` | `REAL` | yes | `—` | — |
| <a id="s-96e788a101"></a>`completed_at` | `REAL` | yes | `—` | — |
| <a id="s-fe445d637c"></a>`next_retry_at` | `REAL` | yes | `—` | — |
| <a id="s-d4062257ec"></a>`exit_code` | `INTEGER` | yes | `—` | — |
| <a id="s-ad1b66fd6a"></a>`error` | `TEXT` | yes | `—` | — |

## Maintained corroboration

### Related interface records

- [Schema identity](gogurt-listener-durable-state-identity.md)

## Governing policies

- <a id="pa-97aad20701"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:gogurt-listener](../../../evidence/sources/authorities.md#src-6b3ecfced3) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/listener.py::\_LISTENER\_STATE\_DDL](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/listener.py)

### Machine authority

- `/external_contract/durable_state/owners/6/structure/tables/2`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fca7ea0329a58a034977e05eb294954d601057e7097174be9ac8abef98be055b -->

```json
{
  "columns": [
    {
      "definition": "dispatch_id TEXT NOT NULL PRIMARY KEY",
      "name": "dispatch_id",
      "nullable": false,
      "primary_key": true,
      "type": "TEXT"
    },
    {
      "definition": "mount_point TEXT NOT NULL",
      "name": "mount_point",
      "nullable": false,
      "type": "TEXT"
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
      "definition": "marker_identity TEXT NOT NULL",
      "name": "marker_identity",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "route TEXT NOT NULL",
      "name": "route",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "checks": [
        "(json_valid(plan_json))"
      ],
      "definition": "plan_json TEXT NOT NULL CHECK (json_valid(plan_json))",
      "name": "plan_json",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "checks": [
        "(\n        state IN ('queued', 'running', 'retry', 'uncertain', 'completed', 'failed')\n    )"
      ],
      "definition": "state TEXT NOT NULL CHECK (\n        state IN ('queued', 'running', 'retry', 'uncertain', 'completed', 'failed')\n    )",
      "name": "state",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "checks": [
        "(attempts >= 0)"
      ],
      "default": "0",
      "definition": "attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0)",
      "name": "attempts",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "observed_at REAL NOT NULL",
      "name": "observed_at",
      "nullable": false,
      "type": "REAL"
    },
    {
      "definition": "started_at REAL",
      "name": "started_at",
      "nullable": true,
      "type": "REAL"
    },
    {
      "definition": "completed_at REAL",
      "name": "completed_at",
      "nullable": true,
      "type": "REAL"
    },
    {
      "definition": "next_retry_at REAL",
      "name": "next_retry_at",
      "nullable": true,
      "type": "REAL"
    },
    {
      "definition": "exit_code INTEGER",
      "name": "exit_code",
      "nullable": true,
      "type": "INTEGER"
    },
    {
      "definition": "error TEXT",
      "name": "error",
      "nullable": true,
      "type": "TEXT"
    }
  ],
  "constraints": [],
  "name": "dispatches"
}
```

</details>
