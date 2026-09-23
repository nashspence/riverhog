# stove0-control: stove0_artifact_selection_members

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-artifact-selection-members:0bf73925aa -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-e19eab67c2"></a>

### Table: `stove0_artifact_selection_members`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-5d302e9f99"></a>`selection_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-95d75d7f56"></a>`artifact_id` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-8a06f51ef2"></a>`artifact_order` | `INTEGER` | no | `—` | — |
| <a id="s-e56cb3ffad"></a>`continuation_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-24859e4f23"></a>`document_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-b0ec09b361"></a>`document_json` | `TEXT` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-4dfa1a6aaa"></a>`primary-key` | `—` | `PRIMARY KEY (selection_sha256, artifact_id)` |
| <a id="s-6e58358dbc"></a>`check` | `ck_stove0_selection_members_artifact_id` | `CONSTRAINT ck_stove0_selection_members_artifact_id CHECK (length(artifact_id) >= 1)` |
| <a id="s-c9b8ae1505"></a>`check` | `ck_stove0_selection_members_order` | `CONSTRAINT ck_stove0_selection_members_order CHECK (artifact_order >= 0)` |
| <a id="s-72b90bab8e"></a>`check` | `ck_stove0_selection_members_continuation` | `CONSTRAINT ck_stove0_selection_members_continuation CHECK (length(continuation_sha256) = 64)` |
| <a id="s-16ba36bd4e"></a>`check` | `ck_stove0_selection_members_document_bytes` | `CONSTRAINT ck_stove0_selection_members_document_bytes CHECK (document_bytes >= 0)` |
| <a id="s-6fa272a43a"></a>`foreign-key` | `—` | `FOREIGN KEY(selection_sha256) REFERENCES stove0_artifact_selections (selection_sha256) ON DELETE CASCADE` |
| <a id="s-4a8c0441cf"></a>`check` | `ck_stove0_artifact_selection_members_selection_sha256_hex` | `CONSTRAINT ck_stove0_artifact_selection_members_selection_sha256_hex CHECK (length(selection_sha256) = 64 AND lower(selection_sha256) = selection_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(selection_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-6b0f4ffce0"></a>`check` | `ck_stove0_artifact_selection_members_continuation_sha256_hex` | `CONSTRAINT ck_stove0_artifact_selection_members_continuation_sha256_hex CHECK (length(continuation_sha256) = 64 AND lower(continuation_sha256) = continuation_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(continuation_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-af77c72436"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/3/structure/tables/9`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 636afebfe918197a71d779499b5d45d7a4b5548ad4c48bf9c71b1d2d89b83a53 -->

```json
{
  "columns": [
    {
      "definition": "selection_sha256 VARCHAR(64) NOT NULL",
      "name": "selection_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "artifact_id VARCHAR(160) NOT NULL",
      "name": "artifact_id",
      "nullable": false,
      "type": "VARCHAR(160)"
    },
    {
      "definition": "artifact_order INTEGER NOT NULL",
      "name": "artifact_order",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "continuation_sha256 VARCHAR(64) NOT NULL",
      "name": "continuation_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
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
        "selection_sha256",
        "artifact_id"
      ],
      "definition": "PRIMARY KEY (selection_sha256, artifact_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_stove0_selection_members_artifact_id CHECK (length(artifact_id) >= 1)",
      "expression": "(length(artifact_id) >= 1)",
      "kind": "check",
      "name": "ck_stove0_selection_members_artifact_id"
    },
    {
      "definition": "CONSTRAINT ck_stove0_selection_members_order CHECK (artifact_order >= 0)",
      "expression": "(artifact_order >= 0)",
      "kind": "check",
      "name": "ck_stove0_selection_members_order"
    },
    {
      "definition": "CONSTRAINT ck_stove0_selection_members_continuation CHECK (length(continuation_sha256) = 64)",
      "expression": "(length(continuation_sha256) = 64)",
      "kind": "check",
      "name": "ck_stove0_selection_members_continuation"
    },
    {
      "definition": "CONSTRAINT ck_stove0_selection_members_document_bytes CHECK (document_bytes >= 0)",
      "expression": "(document_bytes >= 0)",
      "kind": "check",
      "name": "ck_stove0_selection_members_document_bytes"
    },
    {
      "columns": [
        "selection_sha256"
      ],
      "definition": "FOREIGN KEY(selection_sha256) REFERENCES stove0_artifact_selections (selection_sha256) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "selection_sha256"
        ],
        "table": "stove0_artifact_selections"
      }
    },
    {
      "definition": "CONSTRAINT ck_stove0_artifact_selection_members_selection_sha256_hex CHECK (length(selection_sha256) = 64 AND lower(selection_sha256) = selection_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(selection_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(selection_sha256) = 64 AND lower(selection_sha256) = selection_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(selection_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_artifact_selection_members_selection_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_artifact_selection_members_continuation_sha256_hex CHECK (length(continuation_sha256) = 64 AND lower(continuation_sha256) = continuation_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(continuation_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(continuation_sha256) = 64 AND lower(continuation_sha256) = continuation_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(continuation_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_artifact_selection_members_continuation_sha256_hex"
    }
  ],
  "name": "stove0_artifact_selection_members"
}
```

</details>
