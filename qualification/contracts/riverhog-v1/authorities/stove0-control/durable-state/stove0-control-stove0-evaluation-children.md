# stove0-control: stove0_evaluation_children

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-evaluation-children:5c05ff705f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-d5b5eea9cd"></a>

### Table: `stove0_evaluation_children`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-29726f335f"></a>`evaluation_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-2b5151653b"></a>`work_id` | `VARCHAR(64)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-d4bdab0d8f"></a>`primary-key` | `—` | `PRIMARY KEY (evaluation_id, work_id)` |
| <a id="s-5895417179"></a>`foreign-key` | `—` | `FOREIGN KEY(evaluation_id) REFERENCES stove0_evaluation_records (evaluation_id) ON DELETE CASCADE` |
| <a id="s-221aab1112"></a>`check` | `ck_stove0_evaluation_children_evaluation_id_hex` | `CONSTRAINT ck_stove0_evaluation_children_evaluation_id_hex CHECK (length(evaluation_id) = 64 AND lower(evaluation_id) = evaluation_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(evaluation_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-11f669d5a1"></a>`check` | `ck_stove0_evaluation_children_work_id_hex` | `CONSTRAINT ck_stove0_evaluation_children_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-a8c4cc146b"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/3/structure/tables/10`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3b9c114f0a03267246a1016dfa8c997fbeca167728bac94c7b53b38fa6ac59d2 -->

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
      "definition": "work_id VARCHAR(64) NOT NULL",
      "name": "work_id",
      "nullable": false,
      "type": "VARCHAR(64)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "evaluation_id",
        "work_id"
      ],
      "definition": "PRIMARY KEY (evaluation_id, work_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "evaluation_id"
      ],
      "definition": "FOREIGN KEY(evaluation_id) REFERENCES stove0_evaluation_records (evaluation_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "evaluation_id"
        ],
        "table": "stove0_evaluation_records"
      }
    },
    {
      "definition": "CONSTRAINT ck_stove0_evaluation_children_evaluation_id_hex CHECK (length(evaluation_id) = 64 AND lower(evaluation_id) = evaluation_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(evaluation_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(evaluation_id) = 64 AND lower(evaluation_id) = evaluation_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(evaluation_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_evaluation_children_evaluation_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_evaluation_children_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_evaluation_children_work_id_hex"
    }
  ],
  "name": "stove0_evaluation_children"
}
```

</details>
