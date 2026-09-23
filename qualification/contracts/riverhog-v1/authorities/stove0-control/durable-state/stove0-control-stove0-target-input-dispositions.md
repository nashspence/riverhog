# stove0-control: stove0_target_input_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-target-input-dispositions:8c65c92f86 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-7c3747144a"></a>

### Table: `stove0_target_input_dispositions`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-5ceba2c9d9"></a>`work_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-2f8f4a1cca"></a>`job_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-6d9af8a930"></a>`input_id` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-a9086e8fba"></a>`status` | `VARCHAR(32)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-c1292ba776"></a>`primary-key` | `—` | `PRIMARY KEY (work_id, job_id, input_id)` |
| <a id="s-1ac5f87aff"></a>`check` | `ck_stove0_target_dispositions_id` | `CONSTRAINT ck_stove0_target_dispositions_id CHECK (length(input_id) >= 1)` |
| <a id="s-cee7be2bce"></a>`check` | `ck_stove0_target_dispositions_status` | `CONSTRAINT ck_stove0_target_dispositions_status CHECK (status IN ('omitted','preserved','rejected','transformed'))` |
| <a id="s-a9aa645989"></a>`foreign-key` | `—` | `FOREIGN KEY(work_id) REFERENCES stove0_work_records (work_id) ON DELETE CASCADE` |
| <a id="s-b1dd79e510"></a>`check` | `ck_stove0_target_input_dispositions_work_id_hex` | `CONSTRAINT ck_stove0_target_input_dispositions_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-ced3109970"></a>`check` | `ck_stove0_target_input_dispositions_job_id_hex` | `CONSTRAINT ck_stove0_target_input_dispositions_job_id_hex CHECK (length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-7e4a1b6bef"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/3/structure/tables/11`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: abc67561913ecac95c1065bdff687c5f758aeb2038c6dc25c1cb3998fd62352d -->

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
      "definition": "input_id VARCHAR(160) NOT NULL",
      "name": "input_id",
      "nullable": false,
      "type": "VARCHAR(160)"
    },
    {
      "definition": "status VARCHAR(32) NOT NULL",
      "name": "status",
      "nullable": false,
      "type": "VARCHAR(32)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "work_id",
        "job_id",
        "input_id"
      ],
      "definition": "PRIMARY KEY (work_id, job_id, input_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_stove0_target_dispositions_id CHECK (length(input_id) >= 1)",
      "expression": "(length(input_id) >= 1)",
      "kind": "check",
      "name": "ck_stove0_target_dispositions_id"
    },
    {
      "definition": "CONSTRAINT ck_stove0_target_dispositions_status CHECK (status IN ('omitted','preserved','rejected','transformed'))",
      "expression": "(status IN ('omitted','preserved','rejected','transformed'))",
      "kind": "check",
      "name": "ck_stove0_target_dispositions_status"
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
      "definition": "CONSTRAINT ck_stove0_target_input_dispositions_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_target_input_dispositions_work_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_target_input_dispositions_job_id_hex CHECK (length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_target_input_dispositions_job_id_hex"
    }
  ],
  "name": "stove0_target_input_dispositions"
}
```

</details>
