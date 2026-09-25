# stove0-control: stove0_work_records

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-work-records:2e487b8d3d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-2d9619cff1"></a>

### Table: `stove0_work_records`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-8ac67101a0"></a>`work_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-f6483e44f7"></a>`revision` | `INTEGER` | no | `—` | — |
| <a id="s-24cc30d105"></a>`phase` | `VARCHAR(32)` | no | `—` | — |
| <a id="s-8be77a8c80"></a>`updated_at` | `VARCHAR(40)` | no | `—` | — |
| <a id="s-57360247b5"></a>`document_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-fa20ab3040"></a>`document_json` | `TEXT` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-38dda1fe65"></a>`primary-key` | `—` | `PRIMARY KEY (work_id)` |
| <a id="s-9951cbb077"></a>`check` | `ck_stove0_work_records_revision` | `CONSTRAINT ck_stove0_work_records_revision CHECK (revision >= 1)` |
| <a id="s-d793471327"></a>`check` | `ck_stove0_work_records_phase` | `CONSTRAINT ck_stove0_work_records_phase CHECK (phase IN ('eligible','claimed','observing','planning','target_preflight','queued','executing','output_finalizing','verifying','settled','source_collection_retirement_pending','coordinating','abandon_pending','complete','inapplicable','failed','canceled'))` |
| <a id="s-a2fb59bbf2"></a>`check` | `ck_stove0_work_records_id` | `CONSTRAINT ck_stove0_work_records_id CHECK (length(work_id) = 64)` |
| <a id="s-d00555203b"></a>`check` | `ck_stove0_work_records_document_bytes` | `CONSTRAINT ck_stove0_work_records_document_bytes CHECK (document_bytes >= 0)` |
| <a id="s-f98dfd3458"></a>`check` | `ck_stove0_work_records_work_id_hex` | `CONSTRAINT ck_stove0_work_records_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-6598ce8015"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/5/structure/tables/11`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e85c431fa35579410a05bebc811031ec6cf574c03dfb6b07ab445f9994721702 -->

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
      "definition": "revision INTEGER NOT NULL",
      "name": "revision",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "phase VARCHAR(32) NOT NULL",
      "name": "phase",
      "nullable": false,
      "type": "VARCHAR(32)"
    },
    {
      "definition": "updated_at VARCHAR(40) NOT NULL",
      "name": "updated_at",
      "nullable": false,
      "type": "VARCHAR(40)"
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
    }
  ],
  "constraints": [
    {
      "columns": [
        "work_id"
      ],
      "definition": "PRIMARY KEY (work_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_stove0_work_records_revision CHECK (revision >= 1)",
      "expression": "(revision >= 1)",
      "kind": "check",
      "name": "ck_stove0_work_records_revision"
    },
    {
      "definition": "CONSTRAINT ck_stove0_work_records_phase CHECK (phase IN ('eligible','claimed','observing','planning','target_preflight','queued','executing','output_finalizing','verifying','settled','source_collection_retirement_pending','coordinating','abandon_pending','complete','inapplicable','failed','canceled'))",
      "expression": "(phase IN ('eligible','claimed','observing','planning','target_preflight','queued','executing','output_finalizing','verifying','settled','source_collection_retirement_pending','coordinating','abandon_pending','complete','inapplicable','failed','canceled'))",
      "kind": "check",
      "name": "ck_stove0_work_records_phase"
    },
    {
      "definition": "CONSTRAINT ck_stove0_work_records_id CHECK (length(work_id) = 64)",
      "expression": "(length(work_id) = 64)",
      "kind": "check",
      "name": "ck_stove0_work_records_id"
    },
    {
      "definition": "CONSTRAINT ck_stove0_work_records_document_bytes CHECK (document_bytes >= 0)",
      "expression": "(document_bytes >= 0)",
      "kind": "check",
      "name": "ck_stove0_work_records_document_bytes"
    },
    {
      "definition": "CONSTRAINT ck_stove0_work_records_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_work_records_work_id_hex"
    }
  ],
  "name": "stove0_work_records"
}
```

</details>
