# stove0-control: stove0_evaluation_records

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-evaluation-records:dfc17e8eb3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-2815e2a68e"></a>
- Table: `stove0_evaluation_records`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-29ddb78c38"></a>`evaluation_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-44a1a4de48"></a>`revision` | `INTEGER` | no | `—` | — |
| <a id="s-2ab402d7bf"></a>`phase` | `VARCHAR(32)` | no | `—` | — |
| <a id="s-046771c8a6"></a>`updated_at` | `VARCHAR(40)` | no | `—` | — |
| <a id="s-8edce34fcf"></a>`document_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-bf157de57b"></a>`document_json` | `TEXT` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-cc8dddb416"></a>`primary-key` | `—` | `PRIMARY KEY (evaluation_id)` |
| <a id="s-e4a5ec9b5a"></a>`check` | `ck_stove0_evaluation_records_revision` | `CONSTRAINT ck_stove0_evaluation_records_revision CHECK (revision >= 1)` |
| <a id="s-2b3a2ed99c"></a>`check` | `ck_stove0_evaluation_records_phase` | `CONSTRAINT ck_stove0_evaluation_records_phase CHECK (phase IN ('planning','running','partially_complete','complete','failed','canceled'))` |
| <a id="s-136ff96364"></a>`check` | `ck_stove0_evaluation_records_id` | `CONSTRAINT ck_stove0_evaluation_records_id CHECK (length(evaluation_id) = 64)` |
| <a id="s-d8433e13a6"></a>`check` | `ck_stove0_evaluation_records_document_bytes` | `CONSTRAINT ck_stove0_evaluation_records_document_bytes CHECK (document_bytes >= 0)` |
| <a id="s-c72842df65"></a>`check` | `ck_stove0_evaluation_records_evaluation_id_hex` | `CONSTRAINT ck_stove0_evaluation_records_evaluation_id_hex CHECK (length(evaluation_id) = 64 AND lower(evaluation_id) = evaluation_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(evaluation_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-91a6451d40"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:stove0-control](../../../evidence/sources.md#src-45e44b17fd) — `reference/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/3/structure/tables/5`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2f2e192cd2a075ba74c90b8f0cc67eef7342714f61e1e955f41c7baa68849f89 -->

```json
{
  "columns": [
    {
      "definition": "evaluation_id VARCHAR(64) NOT NULL",
      "name": "evaluation_id",
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
        "evaluation_id"
      ],
      "definition": "PRIMARY KEY (evaluation_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_stove0_evaluation_records_revision CHECK (revision >= 1)",
      "expression": "(revision >= 1)",
      "kind": "check",
      "name": "ck_stove0_evaluation_records_revision"
    },
    {
      "definition": "CONSTRAINT ck_stove0_evaluation_records_phase CHECK (phase IN ('planning','running','partially_complete','complete','failed','canceled'))",
      "expression": "(phase IN ('planning','running','partially_complete','complete','failed','canceled'))",
      "kind": "check",
      "name": "ck_stove0_evaluation_records_phase"
    },
    {
      "definition": "CONSTRAINT ck_stove0_evaluation_records_id CHECK (length(evaluation_id) = 64)",
      "expression": "(length(evaluation_id) = 64)",
      "kind": "check",
      "name": "ck_stove0_evaluation_records_id"
    },
    {
      "definition": "CONSTRAINT ck_stove0_evaluation_records_document_bytes CHECK (document_bytes >= 0)",
      "expression": "(document_bytes >= 0)",
      "kind": "check",
      "name": "ck_stove0_evaluation_records_document_bytes"
    },
    {
      "definition": "CONSTRAINT ck_stove0_evaluation_records_evaluation_id_hex CHECK (length(evaluation_id) = 64 AND lower(evaluation_id) = evaluation_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(evaluation_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(evaluation_id) = 64 AND lower(evaluation_id) = evaluation_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(evaluation_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_evaluation_records_evaluation_id_hex"
    }
  ],
  "name": "stove0_evaluation_records"
}
```
