# stove0-control: stove0_target_source_edges

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-target-source-edges:38b3c33983 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-095214b6e3"></a>
- Table: `stove0_target_source_edges`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-a5f3958eec"></a>`work_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-43fe26f6ef"></a>`job_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-2ab568a216"></a>`output_id` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-98d9f69eaa"></a>`input_id` | `VARCHAR(160)` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-f39527f937"></a>`primary-key` | `—` | `PRIMARY KEY (work_id, job_id, output_id, input_id)` |
| <a id="s-08fd411c7c"></a>`check` | `ck_stove0_target_source_edges_output` | `CONSTRAINT ck_stove0_target_source_edges_output CHECK (length(output_id) >= 1)` |
| <a id="s-01151a3646"></a>`check` | `ck_stove0_target_source_edges_input` | `CONSTRAINT ck_stove0_target_source_edges_input CHECK (length(input_id) >= 1)` |
| <a id="s-f75bbd382a"></a>`foreign-key` | `—` | `FOREIGN KEY(work_id) REFERENCES stove0_work_records (work_id) ON DELETE CASCADE` |
| <a id="s-ca540d957c"></a>`check` | `ck_stove0_target_source_edges_work_id_hex` | `CONSTRAINT ck_stove0_target_source_edges_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-fc1550b187"></a>`check` | `ck_stove0_target_source_edges_job_id_hex` | `CONSTRAINT ck_stove0_target_source_edges_job_id_hex CHECK (length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [stove0-control durable-state identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-99a365b99a"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:stove0-control](../../../evidence/sources.md#src-45e44b17fd) — `reference/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/3/structure/tables/13`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 50095304be6e3a137318e050030d191d73bb7d92fc249bc20a6bd1e48ece6339 -->

```json
{
  "columns": [
    {
      "definition": "work_id VARCHAR(64) NOT NULL",
      "name": "work_id",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "job_id VARCHAR(64) NOT NULL",
      "name": "job_id",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "output_id VARCHAR(160) NOT NULL",
      "name": "output_id",
      "nullable": false,
      "type": "VARCHAR(160)"
    },
    {
      "definition": "input_id VARCHAR(160) NOT NULL",
      "name": "input_id",
      "nullable": false,
      "type": "VARCHAR(160)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "work_id",
        "job_id",
        "output_id",
        "input_id"
      ],
      "definition": "PRIMARY KEY (work_id, job_id, output_id, input_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_stove0_target_source_edges_output CHECK (length(output_id) >= 1)",
      "expression": "(length(output_id) >= 1)",
      "kind": "check",
      "name": "ck_stove0_target_source_edges_output"
    },
    {
      "definition": "CONSTRAINT ck_stove0_target_source_edges_input CHECK (length(input_id) >= 1)",
      "expression": "(length(input_id) >= 1)",
      "kind": "check",
      "name": "ck_stove0_target_source_edges_input"
    },
    {
      "columns": [
        "work_id"
      ],
      "definition": "FOREIGN KEY(work_id) REFERENCES stove0_work_records (work_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "work_id"
        ],
        "table": "stove0_work_records"
      }
    },
    {
      "definition": "CONSTRAINT ck_stove0_target_source_edges_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_target_source_edges_work_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_target_source_edges_job_id_hex CHECK (length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_target_source_edges_job_id_hex"
    }
  ],
  "name": "stove0_target_source_edges"
}
```
