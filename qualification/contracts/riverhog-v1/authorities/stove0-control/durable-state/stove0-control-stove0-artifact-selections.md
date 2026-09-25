# stove0-control: stove0_artifact_selections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-artifact-selections:097926f237 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-832a5b9429"></a>

### Table: `stove0_artifact_selections`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-ff791530e7"></a>`selection_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-41cfaf192d"></a>`artifact_count` | `INTEGER` | no | `—` | — |
| <a id="s-3d6a9f170f"></a>`total_bytes` | `BIGINT` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-fab135a0d4"></a>`primary-key` | `—` | `PRIMARY KEY (selection_sha256)` |
| <a id="s-8c53bec223"></a>`check` | `ck_stove0_selections_id` | `CONSTRAINT ck_stove0_selections_id CHECK (length(selection_sha256) = 64)` |
| <a id="s-ed960edbed"></a>`check` | `ck_stove0_selections_count` | `CONSTRAINT ck_stove0_selections_count CHECK (artifact_count >= 0)` |
| <a id="s-92450efd39"></a>`check` | `ck_stove0_selections_bytes` | `CONSTRAINT ck_stove0_selections_bytes CHECK (total_bytes >= 0)` |
| <a id="s-931d820a9a"></a>`check` | `ck_stove0_artifact_selections_selection_sha256_hex` | `CONSTRAINT ck_stove0_artifact_selections_selection_sha256_hex CHECK (length(selection_sha256) = 64 AND lower(selection_sha256) = selection_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(selection_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-f57dfdcd61"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/5/structure/tables/7`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a9bea8ee271a6e38e5b6fac54543a5705ba239ed826c2ffdb59d77697bef728 -->

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
      "definition": "artifact_count INTEGER NOT NULL",
      "name": "artifact_count",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "total_bytes BIGINT NOT NULL",
      "name": "total_bytes",
      "nullable": false,
      "type": "BIGINT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "selection_sha256"
      ],
      "definition": "PRIMARY KEY (selection_sha256)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_stove0_selections_id CHECK (length(selection_sha256) = 64)",
      "expression": "(length(selection_sha256) = 64)",
      "kind": "check",
      "name": "ck_stove0_selections_id"
    },
    {
      "definition": "CONSTRAINT ck_stove0_selections_count CHECK (artifact_count >= 0)",
      "expression": "(artifact_count >= 0)",
      "kind": "check",
      "name": "ck_stove0_selections_count"
    },
    {
      "definition": "CONSTRAINT ck_stove0_selections_bytes CHECK (total_bytes >= 0)",
      "expression": "(total_bytes >= 0)",
      "kind": "check",
      "name": "ck_stove0_selections_bytes"
    },
    {
      "definition": "CONSTRAINT ck_stove0_artifact_selections_selection_sha256_hex CHECK (length(selection_sha256) = 64 AND lower(selection_sha256) = selection_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(selection_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(selection_sha256) = 64 AND lower(selection_sha256) = selection_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(selection_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_artifact_selections_selection_sha256_hex"
    }
  ],
  "name": "stove0_artifact_selections"
}
```

</details>
