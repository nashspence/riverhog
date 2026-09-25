# stove0-control: stove0_departure_policies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-departure-policies:dbf586e1b7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-341d1a25d4"></a>

### Table: `stove0_departure_policies`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-d83fb5dad7"></a>`policy_id` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-b006814d17"></a>`policy_revision` | `INTEGER` | no | `—` | — |
| <a id="s-c10566affd"></a>`policy_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-f4b77e738a"></a>`phase` | `VARCHAR(32)` | no | `—` | — |
| <a id="s-0989f49cac"></a>`generation` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-5b28cad67e"></a>`source_identity` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-c4cc1aab40"></a>`authorization_view_identity` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-02a3cae96b"></a>`cursor` | `VARCHAR(4096)` | yes | `—` | — |
| <a id="s-51e55e09ff"></a>`through_revision` | `VARCHAR(19)` | no | `—` | — |
| <a id="s-100b64c558"></a>`updated_at` | `VARCHAR(40)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-cf5ae6c67a"></a>`primary-key` | `—` | `PRIMARY KEY (policy_id)` |
| <a id="s-1cdd334778"></a>`check` | `ck_stove0_departure_policy_revision` | `CONSTRAINT ck_stove0_departure_policy_revision CHECK (policy_revision >= 1)` |
| <a id="s-6b21b044b9"></a>`check` | `ck_stove0_departure_policy_phase` | `CONSTRAINT ck_stove0_departure_policy_phase CHECK (phase IN ('new','baseline','following','reset_required'))` |
| <a id="s-d718bdc902"></a>`check` | `ck_stove0_departure_policies_policy_sha256_hex` | `CONSTRAINT ck_stove0_departure_policies_policy_sha256_hex CHECK (length(policy_sha256) = 64 AND lower(policy_sha256) = policy_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(policy_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-9e4c6428e5"></a>`check` | `ck_stove0_departure_policies_generation_hex` | `CONSTRAINT ck_stove0_departure_policies_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-04902e0a52"></a>`check` | `ck_stove0_departure_policies_source_identity_hex` | `CONSTRAINT ck_stove0_departure_policies_source_identity_hex CHECK (source_identity IS NULL OR length(source_identity) = 64 AND lower(source_identity) = source_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-60d57c3138"></a>`check` | `ck_stove0_departure_policies_authorization_view_identity_hex` | `CONSTRAINT ck_stove0_departure_policies_authorization_view_identity_hex CHECK (authorization_view_identity IS NULL OR length(authorization_view_identity) = 64 AND lower(authorization_view_identity) = authorization_view_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authorization_view_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-3323b7ed6f"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/5/structure/tables/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb334ba2f7bfcf49a2dd76b217431ed703004be3096a4de965fc2cae820592bd -->

```json
{
  "columns": [
    {
      "definition": "policy_id VARCHAR(160) NOT NULL",
      "name": "policy_id",
      "nullable": false,
      "type": "VARCHAR(160)"
    },
    {
      "definition": "policy_revision INTEGER NOT NULL",
      "name": "policy_revision",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "policy_sha256 VARCHAR(64) NOT NULL",
      "name": "policy_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "phase VARCHAR(32) NOT NULL",
      "name": "phase",
      "nullable": false,
      "type": "VARCHAR(32)"
    },
    {
      "definition": "generation VARCHAR(64) NOT NULL",
      "name": "generation",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "source_identity VARCHAR(64)",
      "name": "source_identity",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "authorization_view_identity VARCHAR(64)",
      "name": "authorization_view_identity",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "cursor VARCHAR(4096)",
      "name": "cursor",
      "nullable": true,
      "type": "VARCHAR(4096)"
    },
    {
      "definition": "through_revision VARCHAR(19) NOT NULL",
      "name": "through_revision",
      "nullable": false,
      "type": "VARCHAR(19)"
    },
    {
      "definition": "updated_at VARCHAR(40) NOT NULL",
      "name": "updated_at",
      "nullable": false,
      "type": "VARCHAR(40)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "policy_id"
      ],
      "definition": "PRIMARY KEY (policy_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_policy_revision CHECK (policy_revision >= 1)",
      "expression": "(policy_revision >= 1)",
      "kind": "check",
      "name": "ck_stove0_departure_policy_revision"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_policy_phase CHECK (phase IN ('new','baseline','following','reset_required'))",
      "expression": "(phase IN ('new','baseline','following','reset_required'))",
      "kind": "check",
      "name": "ck_stove0_departure_policy_phase"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_policies_policy_sha256_hex CHECK (length(policy_sha256) = 64 AND lower(policy_sha256) = policy_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(policy_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(policy_sha256) = 64 AND lower(policy_sha256) = policy_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(policy_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_departure_policies_policy_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_policies_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_departure_policies_generation_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_policies_source_identity_hex CHECK (source_identity IS NULL OR length(source_identity) = 64 AND lower(source_identity) = source_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(source_identity IS NULL OR length(source_identity) = 64 AND lower(source_identity) = source_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_departure_policies_source_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_departure_policies_authorization_view_identity_hex CHECK (authorization_view_identity IS NULL OR length(authorization_view_identity) = 64 AND lower(authorization_view_identity) = authorization_view_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authorization_view_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(authorization_view_identity IS NULL OR length(authorization_view_identity) = 64 AND lower(authorization_view_identity) = authorization_view_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authorization_view_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_departure_policies_authorization_view_identity_hex"
    }
  ],
  "name": "stove0_departure_policies"
}
```

</details>
