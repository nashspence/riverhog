# stove0-control: stove0_work_relations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-work-relations:b9ca28dc05 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-9371305b6f"></a>

### Table: `stove0_work_relations`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-4890ea14d9"></a>`work_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-08c6e2de43"></a>`related_work_id` | `VARCHAR(64)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-09d1f0d61a"></a>`primary-key` | `—` | `PRIMARY KEY (work_id, related_work_id)` |
| <a id="s-cca31c0cd5"></a>`check` | `ck_stove0_work_relations_distinct` | `CONSTRAINT ck_stove0_work_relations_distinct CHECK (work_id <> related_work_id)` |
| <a id="s-d33ae787f1"></a>`foreign-key` | `—` | `FOREIGN KEY(work_id) REFERENCES stove0_work_records (work_id) ON DELETE CASCADE` |
| <a id="s-2d0bb86689"></a>`check` | `ck_stove0_work_relations_work_id_hex` | `CONSTRAINT ck_stove0_work_relations_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-112d6ac625"></a>`check` | `ck_stove0_work_relations_related_work_id_hex` | `CONSTRAINT ck_stove0_work_relations_related_work_id_hex CHECK (length(related_work_id) = 64 AND lower(related_work_id) = related_work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(related_work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-f9a72ebfbe"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/3/structure/tables/17`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af8949e3872f540c4c4c4e220f1ec708d1ab235fd2d182f8b81abc1c38e3e559 -->

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
      "definition": "related_work_id VARCHAR(64) NOT NULL",
      "name": "related_work_id",
      "nullable": false,
      "type": "VARCHAR(64)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "work_id",
        "related_work_id"
      ],
      "definition": "PRIMARY KEY (work_id, related_work_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_stove0_work_relations_distinct CHECK (work_id <> related_work_id)",
      "expression": "(work_id <> related_work_id)",
      "kind": "check",
      "name": "ck_stove0_work_relations_distinct"
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
      "definition": "CONSTRAINT ck_stove0_work_relations_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_work_relations_work_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_work_relations_related_work_id_hex CHECK (length(related_work_id) = 64 AND lower(related_work_id) = related_work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(related_work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(related_work_id) = 64 AND lower(related_work_id) = related_work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(related_work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_work_relations_related_work_id_hex"
    }
  ],
  "name": "stove0_work_relations"
}
```

</details>
