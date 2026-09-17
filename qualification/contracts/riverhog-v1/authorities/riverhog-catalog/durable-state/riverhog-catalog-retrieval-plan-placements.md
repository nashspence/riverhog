# riverhog-catalog: retrieval_plan_placements

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-plan-placements:8c5bdc4832 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-f3f1ae97a4"></a>

### Table: `retrieval_plan_placements`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-3955421a30"></a>`plan_id` | `VARCHAR` | no | `—` | — |
| <a id="s-e028c8ca0a"></a>`file_order` | `INTEGER` | no | `—` | — |
| <a id="s-b7163d56e6"></a>`sequence` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-bb73a3a476"></a>`object_order` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-493aa58d39"></a>`file_offset` | `BIGINT` | no | `—` | — |
| <a id="s-8475ff6ab4"></a>`object_offset` | `BIGINT` | no | `—` | — |
| <a id="s-61fc175fcf"></a>`bytes` | `BIGINT` | no | `—` | — |
| <a id="s-55e5a19ae4"></a>`member` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-eebf114466"></a>`primary-key` | `—` | `PRIMARY KEY (plan_id, file_order, sequence)` |
| <a id="s-8fc9256e97"></a>`foreign-key` | `—` | `FOREIGN KEY(plan_id, file_order) REFERENCES retrieval_plan_files (plan_id, file_order) ON DELETE CASCADE` |
| <a id="s-676149a270"></a>`foreign-key` | `—` | `FOREIGN KEY(plan_id, object_order) REFERENCES retrieval_plan_objects (plan_id, object_order)` |
| <a id="s-7017ebde56"></a>`check` | `ck_retrieval_plan_placements_file_offset` | `CONSTRAINT ck_retrieval_plan_placements_file_offset CHECK (file_offset >= 0)` |
| <a id="s-dd24576a7b"></a>`check` | `ck_retrieval_plan_placements_object_offset` | `CONSTRAINT ck_retrieval_plan_placements_object_offset CHECK (object_offset >= 0)` |
| <a id="s-133507adbc"></a>`check` | `ck_retrieval_plan_placements_bytes` | `CONSTRAINT ck_retrieval_plan_placements_bytes CHECK (bytes >= 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-9e0ab1aaf7"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/79`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 514ae2286f1a6f9b05744d8ad7309a5459c9991f64611ce2b0974d6476a60923 -->

```json
{
  "columns": [
    {
      "definition": "plan_id VARCHAR NOT NULL",
      "name": "plan_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "file_order INTEGER NOT NULL",
      "name": "file_order",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "sequence VARCHAR(64) NOT NULL",
      "name": "sequence",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "object_order VARCHAR(64) NOT NULL",
      "name": "object_order",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "file_offset BIGINT NOT NULL",
      "name": "file_offset",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "object_offset BIGINT NOT NULL",
      "name": "object_offset",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "bytes BIGINT NOT NULL",
      "name": "bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "member VARCHAR",
      "name": "member",
      "nullable": true,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "plan_id",
        "file_order",
        "sequence"
      ],
      "definition": "PRIMARY KEY (plan_id, file_order, sequence)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "plan_id",
        "file_order"
      ],
      "definition": "FOREIGN KEY(plan_id, file_order) REFERENCES retrieval_plan_files (plan_id, file_order) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "plan_id",
          "file_order"
        ],
        "table": "retrieval_plan_files"
      }
    },
    {
      "columns": [
        "plan_id",
        "object_order"
      ],
      "definition": "FOREIGN KEY(plan_id, object_order) REFERENCES retrieval_plan_objects (plan_id, object_order)",
      "kind": "foreign-key",
      "references": {
        "columns": [
          "plan_id",
          "object_order"
        ],
        "table": "retrieval_plan_objects"
      }
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plan_placements_file_offset CHECK (file_offset >= 0)",
      "expression": "(file_offset >= 0)",
      "kind": "check",
      "name": "ck_retrieval_plan_placements_file_offset"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plan_placements_object_offset CHECK (object_offset >= 0)",
      "expression": "(object_offset >= 0)",
      "kind": "check",
      "name": "ck_retrieval_plan_placements_object_offset"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plan_placements_bytes CHECK (bytes >= 0)",
      "expression": "(bytes >= 0)",
      "kind": "check",
      "name": "ck_retrieval_plan_placements_bytes"
    }
  ],
  "name": "retrieval_plan_placements"
}
```

</details>
