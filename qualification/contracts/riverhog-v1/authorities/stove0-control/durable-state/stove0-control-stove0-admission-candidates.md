# stove0-control: stove0_admission_candidates

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-admission-candidates:381e1d3333 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-d66df1f606"></a>

### Table: `stove0_admission_candidates`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-401349323c"></a>`admission_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-876aaa26f9"></a>`policy_id` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-af6de3522c"></a>`state` | `VARCHAR(16)` | no | `—` | — |
| <a id="s-db05a2519e"></a>`preview_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-760d4cc12b"></a>`work_id` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-848cf42864"></a>`document_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-a80932c513"></a>`document_json` | `TEXT` | no | `—` | — |
| <a id="s-586ad010d5"></a>`preview_bytes` | `BIGINT` | yes | `—` | — |
| <a id="s-19d7989692"></a>`preview_json` | `TEXT` | yes | `—` | — |
| <a id="s-e7d9cbd0e3"></a>`attempt_count` | `INTEGER` | no | `—` | — |
| <a id="s-8e2071a29a"></a>`next_attempt_at` | `VARCHAR(40)` | yes | `—` | — |
| <a id="s-70f2602822"></a>`failure` | `TEXT` | yes | `—` | — |
| <a id="s-b046cdbae5"></a>`created_at` | `VARCHAR(40)` | no | `—` | — |
| <a id="s-21ea7a0a61"></a>`updated_at` | `VARCHAR(40)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-09c0f1c27e"></a>`primary-key` | `—` | `PRIMARY KEY (admission_id)` |
| <a id="s-cc38a7d3ee"></a>`check` | `ck_stove0_admission_candidate_state` | `CONSTRAINT ck_stove0_admission_candidate_state CHECK (state IN ('intent','previewed','work_bound'))` |
| <a id="s-a3dbe86b6a"></a>`check` | `ck_stove0_admission_candidate_bytes` | `CONSTRAINT ck_stove0_admission_candidate_bytes CHECK (document_bytes >= 0)` |
| <a id="s-04e3ae137b"></a>`check` | `ck_stove0_admission_candidate_preview_bytes` | `CONSTRAINT ck_stove0_admission_candidate_preview_bytes CHECK (preview_bytes IS NULL OR preview_bytes >= 0)` |
| <a id="s-1724970620"></a>`check` | `ck_stove0_admission_candidate_attempt_count` | `CONSTRAINT ck_stove0_admission_candidate_attempt_count CHECK (attempt_count >= 0)` |
| <a id="s-fe67a3f53b"></a>`check` | `ck_stove0_admission_candidate_next_attempt` | `CONSTRAINT ck_stove0_admission_candidate_next_attempt CHECK (state = 'work_bound' AND next_attempt_at IS NULL OR state != 'work_bound' AND next_attempt_at IS NOT NULL)` |
| <a id="s-0aee41f500"></a>`check` | `ck_stove0_admission_candidates_admission_id_hex` | `CONSTRAINT ck_stove0_admission_candidates_admission_id_hex CHECK (length(admission_id) = 64 AND lower(admission_id) = admission_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(admission_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-4c272d1a2d"></a>`check` | `ck_stove0_admission_candidates_preview_sha256_hex` | `CONSTRAINT ck_stove0_admission_candidates_preview_sha256_hex CHECK (preview_sha256 IS NULL OR length(preview_sha256) = 64 AND lower(preview_sha256) = preview_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(preview_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-72607a1f93"></a>`check` | `ck_stove0_admission_candidates_work_id_hex` | `CONSTRAINT ck_stove0_admission_candidates_work_id_hex CHECK (work_id IS NULL OR length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-1ae0c288d0"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/5/structure/tables/6`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: acb36dd84a314c573c970d9be7652f3ea01741f24ad9cbc6d0233d3b3454c44e -->

```json
{
  "columns": [
    {
      "definition": "admission_id VARCHAR(64) NOT NULL",
      "name": "admission_id",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "policy_id VARCHAR(160) NOT NULL",
      "name": "policy_id",
      "nullable": false,
      "type": "VARCHAR(160)"
    },
    {
      "definition": "state VARCHAR(16) NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR(16)"
    },
    {
      "definition": "preview_sha256 VARCHAR(64)",
      "name": "preview_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "work_id VARCHAR(64)",
      "name": "work_id",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "document_bytes BIGINT NOT NULL",
      "name": "document_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "document_json TEXT NOT NULL",
      "name": "document_json",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "preview_bytes BIGINT",
      "name": "preview_bytes",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "preview_json TEXT",
      "name": "preview_json",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "attempt_count INTEGER NOT NULL",
      "name": "attempt_count",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "next_attempt_at VARCHAR(40)",
      "name": "next_attempt_at",
      "nullable": true,
      "type": "VARCHAR(40)"
    },
    {
      "definition": "failure TEXT",
      "name": "failure",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "created_at VARCHAR(40) NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR(40)"
    },
    {
      "definition": "updated_at VARCHAR(40) NOT NULL",
      "name": "updated_at",
      "nullable": false,
      "type": "VARCHAR(40)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "admission_id"
      ],
      "definition": "PRIMARY KEY (admission_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_candidate_state CHECK (state IN ('intent','previewed','work_bound'))",
      "expression": "(state IN ('intent','previewed','work_bound'))",
      "kind": "check",
      "name": "ck_stove0_admission_candidate_state"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_candidate_bytes CHECK (document_bytes >= 0)",
      "expression": "(document_bytes >= 0)",
      "kind": "check",
      "name": "ck_stove0_admission_candidate_bytes"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_candidate_preview_bytes CHECK (preview_bytes IS NULL OR preview_bytes >= 0)",
      "expression": "(preview_bytes IS NULL OR preview_bytes >= 0)",
      "kind": "check",
      "name": "ck_stove0_admission_candidate_preview_bytes"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_candidate_attempt_count CHECK (attempt_count >= 0)",
      "expression": "(attempt_count >= 0)",
      "kind": "check",
      "name": "ck_stove0_admission_candidate_attempt_count"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_candidate_next_attempt CHECK (state = 'work_bound' AND next_attempt_at IS NULL OR state != 'work_bound' AND next_attempt_at IS NOT NULL)",
      "expression": "(state = 'work_bound' AND next_attempt_at IS NULL OR state != 'work_bound' AND next_attempt_at IS NOT NULL)",
      "kind": "check",
      "name": "ck_stove0_admission_candidate_next_attempt"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_candidates_admission_id_hex CHECK (length(admission_id) = 64 AND lower(admission_id) = admission_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(admission_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(admission_id) = 64 AND lower(admission_id) = admission_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(admission_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_admission_candidates_admission_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_candidates_preview_sha256_hex CHECK (preview_sha256 IS NULL OR length(preview_sha256) = 64 AND lower(preview_sha256) = preview_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(preview_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(preview_sha256 IS NULL OR length(preview_sha256) = 64 AND lower(preview_sha256) = preview_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(preview_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_admission_candidates_preview_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_candidates_work_id_hex CHECK (work_id IS NULL OR length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(work_id IS NULL OR length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_admission_candidates_work_id_hex"
    }
  ],
  "name": "stove0_admission_candidates"
}
```

</details>
