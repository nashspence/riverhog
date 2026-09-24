# riverhog-catalog: retrieval_job_object_progress

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-job-object-progress:2f3c179864 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-f3f1ae97a4"></a>

### Table: `retrieval_job_object_progress`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-3955421a30"></a>`job_id` | `VARCHAR` | no | `—` | — |
| <a id="s-e028c8ca0a"></a>`object_order` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-b7163d56e6"></a>`plan_id` | `VARCHAR` | no | `—` | — |
| <a id="s-bb73a3a476"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-493aa58d39"></a>`prepare_requested_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-8475ff6ab4"></a>`next_poll_at` | `VARCHAR` | no | `—` | — |
| <a id="s-61fc175fcf"></a>`cache_store` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-eebf114466"></a>`primary-key` | `—` | `PRIMARY KEY (job_id, object_order)` |
| <a id="s-8fc9256e97"></a>`foreign-key` | `—` | `FOREIGN KEY(job_id, plan_id) REFERENCES retrieval_jobs (id, plan_id) ON DELETE CASCADE` |
| <a id="s-676149a270"></a>`foreign-key` | `—` | `FOREIGN KEY(plan_id, object_order) REFERENCES retrieval_plan_objects (plan_id, object_order)` |
| <a id="s-7017ebde56"></a>`check` | `ck_retrieval_job_object_progress_state` | `CONSTRAINT ck_retrieval_job_object_progress_state CHECK (state IN ('preparing','requested','ready'))` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-ecbfc71c51"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/79`

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
