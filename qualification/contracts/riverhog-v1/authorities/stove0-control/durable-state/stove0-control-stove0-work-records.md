# stove0-control: stove0_work_records

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-work-records:7e84fc55c9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-1d558a5c9e"></a>

### Table: `stove0_work_records`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-a68a8c0f1e"></a>`work_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-d43cdafbc0"></a>`revision` | `INTEGER` | no | `—` | — |
| <a id="s-04c53dd5b7"></a>`phase` | `VARCHAR(32)` | no | `—` | — |
| <a id="s-091063b1f4"></a>`updated_at` | `VARCHAR(40)` | no | `—` | — |
| <a id="s-2679702022"></a>`document_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-2516c93f7e"></a>`document_json` | `TEXT` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-fbd19d024b"></a>`primary-key` | `—` | `PRIMARY KEY (work_id)` |
| <a id="s-d6f7938e94"></a>`check` | `ck_stove0_work_records_revision` | `CONSTRAINT ck_stove0_work_records_revision CHECK (revision >= 1)` |
| <a id="s-ed995ae514"></a>`check` | `ck_stove0_work_records_phase` | `CONSTRAINT ck_stove0_work_records_phase CHECK (phase IN ('eligible','claimed','observing','planning','target_preflight','queued','executing','output_finalizing','verifying','settled','retirement_pending','coordinating','abandon_pending','complete','inapplicable','failed','canceled'))` |
| <a id="s-f490934355"></a>`check` | `ck_stove0_work_records_id` | `CONSTRAINT ck_stove0_work_records_id CHECK (length(work_id) = 64)` |
| <a id="s-c4575f784b"></a>`check` | `ck_stove0_work_records_document_bytes` | `CONSTRAINT ck_stove0_work_records_document_bytes CHECK (document_bytes >= 0)` |
| <a id="s-a8be8b2824"></a>`check` | `ck_stove0_work_records_work_id_hex` | `CONSTRAINT ck_stove0_work_records_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-fefb2bc6a3"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [reference/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../reference/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/3/structure/tables/8`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 88f68004fb250c8281e4f83bb0784790bc9b73a355af328af6a41f71a488f5f7 -->

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
      "definition": "CONSTRAINT ck_stove0_work_records_phase CHECK (phase IN ('eligible','claimed','observing','planning','target_preflight','queued','executing','output_finalizing','verifying','settled','retirement_pending','coordinating','abandon_pending','complete','inapplicable','failed','canceled'))",
      "expression": "(phase IN ('eligible','claimed','observing','planning','target_preflight','queued','executing','output_finalizing','verifying','settled','retirement_pending','coordinating','abandon_pending','complete','inapplicable','failed','canceled'))",
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
