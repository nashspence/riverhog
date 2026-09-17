# piggity-local: retrieval_jobs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:piggity-local:piggity-local-retrieval-jobs:89e4225c54 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity-local](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-ce3aba518c"></a>

### Table: `retrieval_jobs`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-3237ec0ed7"></a>`id` | `TEXT` | no | `—` | — |
| <a id="s-f6d8ce69bf"></a>`state` | `TEXT` | no | `—` | — |
| <a id="s-329fdd081c"></a>`updated_at` | `TEXT` | no | `CURRENT_TIMESTAMP` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-acf3d2fe35"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |
| <a id="s-6b24f984ae"></a>`check` | `ck_retrieval_jobs_state` | `CONSTRAINT ck_retrieval_jobs_state CHECK (state IN ('requested', 'ready', 'completed', 'expired', 'failed', 'canceled'))` |

## Maintained corroboration

### Related interface records

- [Schema identity](piggity-local-durable-state-identity.md)

## Governing policies

- <a id="pa-cd09452534"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:piggity-local](../../../evidence/sources/authorities.md#src-f6a1289f67) — [reference/riverhog/applications/piggity/src/piggity/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../reference/riverhog/applications/piggity/src/piggity/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/1/structure/tables/2`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ef34e10b294c97570da0f2c73df194791a48947ad7d9ede1541b9e1da70a0901 -->

```json
{
  "columns": [
    {
      "definition": "id TEXT NOT NULL",
      "name": "id",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "state TEXT NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "default": "CURRENT_TIMESTAMP",
      "definition": "updated_at TEXT DEFAULT CURRENT_TIMESTAMP NOT NULL",
      "name": "updated_at",
      "nullable": false,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "id"
      ],
      "definition": "PRIMARY KEY (id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_jobs_state CHECK (state IN ('requested', 'ready', 'completed', 'expired', 'failed', 'canceled'))",
      "expression": "(state IN ('requested', 'ready', 'completed', 'expired', 'failed', 'canceled'))",
      "kind": "check",
      "name": "ck_retrieval_jobs_state"
    }
  ],
  "name": "retrieval_jobs"
}
```

</details>
