# stove0-control: stove0_target_source_edges

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-target-source-edges:f186eff2e3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-76b5dae209"></a>

### Table: `stove0_target_source_edges`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-efb2946600"></a>`work_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-8b95bf9324"></a>`job_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-71475f6b5b"></a>`output_id` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-3eb0467a9a"></a>`input_id` | `VARCHAR(160)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-1e533907a4"></a>`primary-key` | `—` | `PRIMARY KEY (work_id, job_id, output_id, input_id)` |
| <a id="s-4891afa888"></a>`check` | `ck_stove0_target_source_edges_output` | `CONSTRAINT ck_stove0_target_source_edges_output CHECK (length(output_id) >= 1)` |
| <a id="s-814f6267ff"></a>`check` | `ck_stove0_target_source_edges_input` | `CONSTRAINT ck_stove0_target_source_edges_input CHECK (length(input_id) >= 1)` |
| <a id="s-0dc12dd287"></a>`foreign-key` | `—` | `FOREIGN KEY(work_id) REFERENCES stove0_work_records (work_id) ON DELETE CASCADE` |
| <a id="s-422e2c90e1"></a>`check` | `ck_stove0_target_source_edges_work_id_hex` | `CONSTRAINT ck_stove0_target_source_edges_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-b906e226ab"></a>`check` | `ck_stove0_target_source_edges_job_id_hex` | `CONSTRAINT ck_stove0_target_source_edges_job_id_hex CHECK (length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-afdab02be1"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/5/structure/tables/16`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
