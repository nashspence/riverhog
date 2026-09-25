# riverhog-catalog: storage_incarnations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-storage-incarnations:d9382aecb5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-578253b2fe"></a>

### Table: `storage_incarnations`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-e44fb490ed"></a>`id` | `VARCHAR(36)` | no | `—` | — |
| <a id="s-783c2ae605"></a>`kind` | `VARCHAR` | no | `—` | — |
| <a id="s-22ee02e7b4"></a>`name` | `VARCHAR` | no | `—` | — |
| <a id="s-9e3258cae2"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-1d3e2eea6e"></a>`binding_generation` | `BIGINT` | no | `—` | — |
| <a id="s-f528281c4f"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-0bea5f8cba"></a>`last_bound_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-078b54c5df"></a>`last_read_mode` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-3250a87917"></a>`primary-key` | `—` | `PRIMARY KEY (kind, name)` |
| <a id="s-8d6e1f6ff9"></a>`unique` | `uq_storage_incarnations_id_name` | `CONSTRAINT uq_storage_incarnations_id_name UNIQUE (id, name)` |
| <a id="s-0c0b741b50"></a>`check` | `ck_storage_incarnations_kind` | `CONSTRAINT ck_storage_incarnations_kind CHECK (kind IN ('archive','cache'))` |
| <a id="s-9ac08b25f4"></a>`check` | `ck_storage_incarnations_state` | `CONSTRAINT ck_storage_incarnations_state CHECK (state IN ('bound','disabled','retired'))` |
| <a id="s-10c8373969"></a>`check` | `ck_storage_incarnations_generation` | `CONSTRAINT ck_storage_incarnations_generation CHECK (binding_generation >= 1)` |
| <a id="s-18579fadba"></a>`check` | `ck_storage_incarnations_read_mode` | `CONSTRAINT ck_storage_incarnations_read_mode CHECK (last_read_mode IS NULL OR last_read_mode IN ('immediate','restore_required'))` |
| <a id="s-ebb9833020"></a>`check` | `ck_storage_incarnations_uuid4` | `CONSTRAINT ck_storage_incarnations_uuid4 CHECK (length(id) = 36 AND substr(id, 9, 1) = '-' AND substr(id, 14, 1) = '-' AND substr(id, 19, 1) = '-' AND substr(id, 24, 1) = '-' AND substr(id, 15, 1) = '4' AND substr(id, 20, 1) >= '8' AND substr(id, 20, 1) <= 'b' AND length(replace(id, '-', '')) = 32 AND lower(replace(id, '-', '')) = replace(id, '-', '') AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(id, '-', ''), '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-790f3ab1ae"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9469dc574f3452c5e4d38220fb3fa787949833f9d5965cf2434200ce6680deeb -->

```json
{
  "columns": [
    {
      "definition": "id VARCHAR(36) NOT NULL",
      "name": "id",
      "nullable": false,
      "type": "VARCHAR(36)"
    },
    {
      "definition": "kind VARCHAR NOT NULL",
      "name": "kind",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "name VARCHAR NOT NULL",
      "name": "name",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "binding_generation BIGINT NOT NULL",
      "name": "binding_generation",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "created_at VARCHAR NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "last_bound_at VARCHAR",
      "name": "last_bound_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "last_read_mode VARCHAR",
      "name": "last_read_mode",
      "nullable": true,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "kind",
        "name"
      ],
      "definition": "PRIMARY KEY (kind, name)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "id",
        "name"
      ],
      "definition": "CONSTRAINT uq_storage_incarnations_id_name UNIQUE (id, name)",
      "kind": "unique",
      "name": "uq_storage_incarnations_id_name"
    },
    {
      "definition": "CONSTRAINT ck_storage_incarnations_kind CHECK (kind IN ('archive','cache'))",
      "expression": "(kind IN ('archive','cache'))",
      "kind": "check",
      "name": "ck_storage_incarnations_kind"
    },
    {
      "definition": "CONSTRAINT ck_storage_incarnations_state CHECK (state IN ('bound','disabled','retired'))",
      "expression": "(state IN ('bound','disabled','retired'))",
      "kind": "check",
      "name": "ck_storage_incarnations_state"
    },
    {
      "definition": "CONSTRAINT ck_storage_incarnations_generation CHECK (binding_generation >= 1)",
      "expression": "(binding_generation >= 1)",
      "kind": "check",
      "name": "ck_storage_incarnations_generation"
    },
    {
      "definition": "CONSTRAINT ck_storage_incarnations_read_mode CHECK (last_read_mode IS NULL OR last_read_mode IN ('immediate','restore_required'))",
      "expression": "(last_read_mode IS NULL OR last_read_mode IN ('immediate','restore_required'))",
      "kind": "check",
      "name": "ck_storage_incarnations_read_mode"
    },
    {
      "definition": "CONSTRAINT ck_storage_incarnations_uuid4 CHECK (length(id) = 36 AND substr(id, 9, 1) = '-' AND substr(id, 14, 1) = '-' AND substr(id, 19, 1) = '-' AND substr(id, 24, 1) = '-' AND substr(id, 15, 1) = '4' AND substr(id, 20, 1) >= '8' AND substr(id, 20, 1) <= 'b' AND length(replace(id, '-', '')) = 32 AND lower(replace(id, '-', '')) = replace(id, '-', '') AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(id, '-', ''), '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(id) = 36 AND substr(id, 9, 1) = '-' AND substr(id, 14, 1) = '-' AND substr(id, 19, 1) = '-' AND substr(id, 24, 1) = '-' AND substr(id, 15, 1) = '4' AND substr(id, 20, 1) >= '8' AND substr(id, 20, 1) <= 'b' AND length(replace(id, '-', '')) = 32 AND lower(replace(id, '-', '')) = replace(id, '-', '') AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(id, '-', ''), '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_storage_incarnations_uuid4"
    }
  ],
  "name": "storage_incarnations"
}
```

</details>
