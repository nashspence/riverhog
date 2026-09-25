# riverhog-catalog: retrieval_jobs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-jobs:b4d8a33cad -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-574565769b"></a>

### Table: `retrieval_jobs`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-0c082cc8f9"></a>`id` | `VARCHAR` | no | `—` | — |
| <a id="s-78195d1637"></a>`plan_id` | `VARCHAR` | no | `—` | — |
| <a id="s-b41bf71615"></a>`principal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-0a2c76d81d"></a>`initiated_by_key_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-6d53532837"></a>`event_context_json` | `TEXT` | yes | `—` | — |
| <a id="s-53f44a7a77"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-d7036af01a"></a>`plan_etag` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-4d6cbd363f"></a>`lease_seconds` | `BIGINT` | no | `—` | — |
| <a id="s-31d0d1bf15"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-7c1511c985"></a>`requested_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-5d7355ee61"></a>`restore_requested_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-8cf34deff9"></a>`ready_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-84fd0d7c7e"></a>`expires_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-d03bfe3ea1"></a>`next_poll_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-edf3849c11"></a>`completed_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-80b81f4c83"></a>`canceled_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-e4cf59c186"></a>`failure` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-6f09699701"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |
| <a id="s-937cb9f3c0"></a>`foreign-key` | `—` | `FOREIGN KEY(plan_id) REFERENCES retrieval_plans (id)` |
| <a id="s-96318ec69c"></a>`unique` | `—` | `UNIQUE (id, plan_id)` |
| <a id="s-70e93ea9e9"></a>`check` | `ck_retrieval_jobs_state` | `CONSTRAINT ck_retrieval_jobs_state CHECK (state IN ('requested','ready','completed','canceled','expired','failed'))` |
| <a id="s-2009624b22"></a>`check` | `ck_retrieval_jobs_plan_etag` | `CONSTRAINT ck_retrieval_jobs_plan_etag CHECK (length(plan_etag) = 64)` |
| <a id="s-b57b139db2"></a>`check` | `ck_retrieval_jobs_lease` | `CONSTRAINT ck_retrieval_jobs_lease CHECK (lease_seconds > 0)` |
| <a id="s-ade3c9e587"></a>`unique` | `—` | `UNIQUE (plan_id)` |
| <a id="s-a3a4b823f9"></a>`check` | `ck_retrieval_jobs_plan_etag_hex` | `CONSTRAINT ck_retrieval_jobs_plan_etag_hex CHECK (length(plan_etag) = 64 AND lower(plan_etag) = plan_etag AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(plan_etag, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-701581a2a6"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/40`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 07fb96d36c7ef811dcd7415d23b00774c8d41729bca86181d83318912987b2e5 -->

```json
{
  "columns": [
    {
      "definition": "id VARCHAR NOT NULL",
      "name": "id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "plan_id VARCHAR NOT NULL",
      "name": "plan_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "principal_id VARCHAR NOT NULL",
      "name": "principal_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "initiated_by_key_id VARCHAR",
      "name": "initiated_by_key_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "event_context_json TEXT",
      "name": "event_context_json",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "plan_etag VARCHAR(64) NOT NULL",
      "name": "plan_etag",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "lease_seconds BIGINT NOT NULL",
      "name": "lease_seconds",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "created_at VARCHAR NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "requested_at VARCHAR",
      "name": "requested_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "restore_requested_at VARCHAR",
      "name": "restore_requested_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "ready_at VARCHAR",
      "name": "ready_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "expires_at VARCHAR",
      "name": "expires_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "next_poll_at VARCHAR",
      "name": "next_poll_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "completed_at VARCHAR",
      "name": "completed_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "canceled_at VARCHAR",
      "name": "canceled_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "failure TEXT",
      "name": "failure",
      "nullable": true,
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
      "columns": [
        "plan_id"
      ],
      "definition": "FOREIGN KEY(plan_id) REFERENCES retrieval_plans (id)",
      "kind": "foreign-key",
      "references": {
        "columns": [
          "id"
        ],
        "table": "retrieval_plans"
      }
    },
    {
      "columns": [
        "id",
        "plan_id"
      ],
      "definition": "UNIQUE (id, plan_id)",
      "kind": "unique"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_jobs_state CHECK (state IN ('requested','ready','completed','canceled','expired','failed'))",
      "expression": "(state IN ('requested','ready','completed','canceled','expired','failed'))",
      "kind": "check",
      "name": "ck_retrieval_jobs_state"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_jobs_plan_etag CHECK (length(plan_etag) = 64)",
      "expression": "(length(plan_etag) = 64)",
      "kind": "check",
      "name": "ck_retrieval_jobs_plan_etag"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_jobs_lease CHECK (lease_seconds > 0)",
      "expression": "(lease_seconds > 0)",
      "kind": "check",
      "name": "ck_retrieval_jobs_lease"
    },
    {
      "columns": [
        "plan_id"
      ],
      "definition": "UNIQUE (plan_id)",
      "kind": "unique"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_jobs_plan_etag_hex CHECK (length(plan_etag) = 64 AND lower(plan_etag) = plan_etag AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(plan_etag, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(plan_etag) = 64 AND lower(plan_etag) = plan_etag AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(plan_etag, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_retrieval_jobs_plan_etag_hex"
    }
  ],
  "name": "retrieval_jobs"
}
```

</details>
