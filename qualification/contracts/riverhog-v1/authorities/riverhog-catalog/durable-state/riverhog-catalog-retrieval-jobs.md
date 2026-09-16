# riverhog-catalog: retrieval_jobs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-jobs:e2afdbc705 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-c4b3febcb2"></a>

### Table: `retrieval_jobs`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1151e04935"></a>`id` | `VARCHAR` | no | `—` | — |
| <a id="s-d4aec50038"></a>`plan_id` | `VARCHAR` | no | `—` | — |
| <a id="s-06a2164c54"></a>`app` | `VARCHAR` | no | `—` | — |
| <a id="s-5f67d51e59"></a>`initiated_by_key_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-4fd1a6484e"></a>`event_context_json` | `TEXT` | yes | `—` | — |
| <a id="s-2eadc88986"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-8be9b06324"></a>`plan_etag` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-0e868aa534"></a>`lease_seconds` | `BIGINT` | no | `—` | — |
| <a id="s-794fc87403"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-6a017315b8"></a>`requested_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-97a0089309"></a>`restore_requested_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-60127830bb"></a>`ready_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-0a5f753aea"></a>`expires_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-c8e394bad8"></a>`next_poll_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-4f571fa5ab"></a>`completed_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-93c1bb8bc9"></a>`canceled_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-d2faf29d5e"></a>`failure` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-cc81e06afc"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |
| <a id="s-247213a175"></a>`foreign-key` | `—` | `FOREIGN KEY(plan_id) REFERENCES retrieval_plans (id)` |
| <a id="s-6a3e91e7e8"></a>`unique` | `—` | `UNIQUE (id, plan_id)` |
| <a id="s-ef35eeb543"></a>`check` | `ck_retrieval_jobs_state` | `CONSTRAINT ck_retrieval_jobs_state CHECK (state IN ('requested','ready','completed','canceled','expired','failed'))` |
| <a id="s-54425c0db5"></a>`check` | `ck_retrieval_jobs_plan_etag` | `CONSTRAINT ck_retrieval_jobs_plan_etag CHECK (length(plan_etag) = 64)` |
| <a id="s-7f34d34303"></a>`check` | `ck_retrieval_jobs_lease` | `CONSTRAINT ck_retrieval_jobs_lease CHECK (lease_seconds > 0)` |
| <a id="s-5d1aba64d7"></a>`unique` | `—` | `UNIQUE (plan_id)` |
| <a id="s-dea0761009"></a>`check` | `ck_retrieval_jobs_plan_etag_hex` | `CONSTRAINT ck_retrieval_jobs_plan_etag_hex CHECK (length(plan_etag) = 64 AND lower(plan_etag) = plan_etag AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(plan_etag, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-2dba426323"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/38`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0827b57d0462b8f5f3370bc1f6400fe0a63f0c2491664871a87f1f83f91737e -->

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
      "definition": "app VARCHAR NOT NULL",
      "name": "app",
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
