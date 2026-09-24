# stove0-control: stove0_target_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-target-outputs:018719321a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-9ec47388f3"></a>

### Table: `stove0_target_outputs`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-2ac7f1aba0"></a>`work_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-8ef5694fac"></a>`job_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-d80299117e"></a>`output_id` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-3bdf8a9594"></a>`output_path` | `VARCHAR(4096)` | no | `—` | — |
| <a id="s-d0b039f149"></a>`document_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-f5511c4612"></a>`document_json` | `TEXT` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-21e34812a6"></a>`primary-key` | `—` | `PRIMARY KEY (work_id, job_id, output_id)` |
| <a id="s-097e7bab76"></a>`check` | `ck_stove0_target_outputs_id` | `CONSTRAINT ck_stove0_target_outputs_id CHECK (length(output_id) >= 1)` |
| <a id="s-eba4593c79"></a>`check` | `ck_stove0_target_outputs_path` | `CONSTRAINT ck_stove0_target_outputs_path CHECK (length(output_path) >= 1)` |
| <a id="s-95b2c9b814"></a>`check` | `ck_stove0_target_outputs_document_bytes` | `CONSTRAINT ck_stove0_target_outputs_document_bytes CHECK (document_bytes >= 0)` |
| <a id="s-0e75b037e7"></a>`foreign-key` | `—` | `FOREIGN KEY(work_id) REFERENCES stove0_work_records (work_id) ON DELETE CASCADE` |
| <a id="s-c49e3b256b"></a>`check` | `ck_stove0_target_outputs_work_id_hex` | `CONSTRAINT ck_stove0_target_outputs_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-44c8aff73f"></a>`check` | `ck_stove0_target_outputs_job_id_hex` | `CONSTRAINT ck_stove0_target_outputs_job_id_hex CHECK (length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-1a43153497"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/3/structure/tables/15`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 90d30e45be0866f985d5d808d0b74f8e9bb552ad72bc32201d5744c1726961eb -->

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
      "definition": "output_path VARCHAR(4096) NOT NULL",
      "name": "output_path",
      "nullable": false,
      "type": "VARCHAR(4096)"
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
        "work_id",
        "job_id",
        "output_id"
      ],
      "definition": "PRIMARY KEY (work_id, job_id, output_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_stove0_target_outputs_id CHECK (length(output_id) >= 1)",
      "expression": "(length(output_id) >= 1)",
      "kind": "check",
      "name": "ck_stove0_target_outputs_id"
    },
    {
      "definition": "CONSTRAINT ck_stove0_target_outputs_path CHECK (length(output_path) >= 1)",
      "expression": "(length(output_path) >= 1)",
      "kind": "check",
      "name": "ck_stove0_target_outputs_path"
    },
    {
      "definition": "CONSTRAINT ck_stove0_target_outputs_document_bytes CHECK (document_bytes >= 0)",
      "expression": "(document_bytes >= 0)",
      "kind": "check",
      "name": "ck_stove0_target_outputs_document_bytes"
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
      "definition": "CONSTRAINT ck_stove0_target_outputs_work_id_hex CHECK (length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(work_id) = 64 AND lower(work_id) = work_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(work_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_target_outputs_work_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_target_outputs_job_id_hex CHECK (length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(job_id) = 64 AND lower(job_id) = job_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(job_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_target_outputs_job_id_hex"
    }
  ],
  "name": "stove0_target_outputs"
}
```

</details>
