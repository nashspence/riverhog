# gogurt-listener: dispatches

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:gogurt-listener:gogurt-listener-dispatches:6e2e657bb9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-dd804d3f81"></a>

### Table: `dispatches`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-35e81c8718"></a>`dispatch_id` | `TEXT` | no | `—` | {"primary_key":true} |
| <a id="s-245d35832a"></a>`mount_point` | `TEXT` | no | `—` | — |
| <a id="s-d545428730"></a>`generation` | `INTEGER` | no | `—` | {"checks":["(generation >= 1)"]} |
| <a id="s-5af5d654b2"></a>`marker_identity` | `TEXT` | no | `—` | — |
| <a id="s-e0e7779d0d"></a>`route` | `TEXT` | no | `—` | — |
| <a id="s-6dc92ddf56"></a>`plan_json` | `TEXT` | no | `—` | {"checks":["(json_valid(plan_json))"]} |
| <a id="s-3f964a509b"></a>`state` | `TEXT` | no | `—` | {"checks":["(\n        state IN ('queued', 'running', 'retry', 'uncertain', 'completed', 'failed')\n    )"]} |
| <a id="s-34a1b2b93f"></a>`attempts` | `INTEGER` | no | `0` | {"checks":["(attempts >= 0)"]} |
| <a id="s-5a18d9b950"></a>`observed_at` | `REAL` | no | `—` | — |
| <a id="s-e8266ba831"></a>`started_at` | `REAL` | yes | `—` | — |
| <a id="s-f33a179b60"></a>`completed_at` | `REAL` | yes | `—` | — |
| <a id="s-8ef0ed5e17"></a>`next_retry_at` | `REAL` | yes | `—` | — |
| <a id="s-b093998e63"></a>`exit_code` | `INTEGER` | yes | `—` | — |
| <a id="s-184e163e2e"></a>`error` | `TEXT` | yes | `—` | — |

## Maintained corroboration

### Related interface records

- [Schema identity](gogurt-listener-durable-state-identity.md)

## Governing policies

- <a id="pa-53b33b8833"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:gogurt-listener](../../../evidence/sources.md#src-6b3ecfced3) — [reference/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/listener.py::\_LISTENER\_STATE\_DDL](../../../../../../reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/listener.py)

### Machine authority

- `/external_contract/durable_state/owners/4/structure/tables/2`

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
