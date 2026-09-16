# riverhog-catalog: retrieval_job_object_progress

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-job-object-progress:9d20405285 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-7369fcabeb"></a>

### Table: `retrieval_job_object_progress`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-cc82627a80"></a>`job_id` | `VARCHAR` | no | `—` | — |
| <a id="s-4ab8f3c1a7"></a>`object_order` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-f3243ab53a"></a>`plan_id` | `VARCHAR` | no | `—` | — |
| <a id="s-c1891cfedf"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-83c13196e8"></a>`prepare_requested_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-f965282383"></a>`next_poll_at` | `VARCHAR` | no | `—` | — |
| <a id="s-ab326bc70c"></a>`cache_store` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-b4e44dde84"></a>`primary-key` | `—` | `PRIMARY KEY (job_id, object_order)` |
| <a id="s-99ecd1b587"></a>`foreign-key` | `—` | `FOREIGN KEY(job_id, plan_id) REFERENCES retrieval_jobs (id, plan_id) ON DELETE CASCADE` |
| <a id="s-3c802dfcd1"></a>`foreign-key` | `—` | `FOREIGN KEY(plan_id, object_order) REFERENCES retrieval_plan_objects (plan_id, object_order)` |
| <a id="s-8c14194e23"></a>`check` | `ck_retrieval_job_object_progress_state` | `CONSTRAINT ck_retrieval_job_object_progress_state CHECK (state IN ('preparing','requested','ready'))` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-fdd29130b6"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/78`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 479cdf2f485159368add0b6b694682d568fbebcad61a7ec23aefd4aede96418d -->

```json
{
  "columns": [
    {
      "definition": "job_id VARCHAR NOT NULL",
      "name": "job_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "object_order VARCHAR(64) NOT NULL",
      "name": "object_order",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "plan_id VARCHAR NOT NULL",
      "name": "plan_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "prepare_requested_at VARCHAR",
      "name": "prepare_requested_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "next_poll_at VARCHAR NOT NULL",
      "name": "next_poll_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "cache_store VARCHAR",
      "name": "cache_store",
      "nullable": true,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "job_id",
        "object_order"
      ],
      "definition": "PRIMARY KEY (job_id, object_order)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "job_id",
        "plan_id"
      ],
      "definition": "FOREIGN KEY(job_id, plan_id) REFERENCES retrieval_jobs (id, plan_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "id",
          "plan_id"
        ],
        "table": "retrieval_jobs"
      }
    },
    {
      "columns": [
        "plan_id",
        "object_order"
      ],
      "definition": "FOREIGN KEY(plan_id, object_order) REFERENCES retrieval_plan_objects (plan_id, object_order)",
      "kind": "foreign-key",
      "references": {
        "columns": [
          "plan_id",
          "object_order"
        ],
        "table": "retrieval_plan_objects"
      }
    },
    {
      "definition": "CONSTRAINT ck_retrieval_job_object_progress_state CHECK (state IN ('preparing','requested','ready'))",
      "expression": "(state IN ('preparing','requested','ready'))",
      "kind": "check",
      "name": "ck_retrieval_job_object_progress_state"
    }
  ],
  "name": "retrieval_job_object_progress"
}
```

</details>
