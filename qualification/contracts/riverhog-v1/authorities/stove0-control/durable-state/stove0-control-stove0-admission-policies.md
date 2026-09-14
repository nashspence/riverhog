# stove0-control: stove0_admission_policies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-admission-policies:488f0eafc8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-9fcc47afa8"></a>
- Table: `stove0_admission_policies`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-0eb2e1316e"></a>`policy_id` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-e994025205"></a>`policy_revision` | `INTEGER` | no | `—` | — |
| <a id="s-942b0af2fd"></a>`policy_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-059199b5d0"></a>`phase` | `VARCHAR(32)` | no | `—` | — |
| <a id="s-27a3ad2ff9"></a>`generation` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-02c49023f8"></a>`source_identity` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-58b23f9aa7"></a>`authorization_view_identity` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-90c7b06c9e"></a>`cursor` | `VARCHAR(4096)` | yes | `—` | — |
| <a id="s-caf9796f81"></a>`baseline_mode` | `VARCHAR(16)` | no | `—` | — |
| <a id="s-776499b623"></a>`through_revision` | `VARCHAR(19)` | no | `—` | — |
| <a id="s-4438b43506"></a>`updated_at` | `VARCHAR(40)` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-255e4b2132"></a>`primary-key` | `—` | `PRIMARY KEY (policy_id)` |
| <a id="s-eb0d74082c"></a>`check` | `ck_stove0_admission_policy_revision` | `CONSTRAINT ck_stove0_admission_policy_revision CHECK (policy_revision >= 1)` |
| <a id="s-f9574ba51c"></a>`check` | `ck_stove0_admission_policy_id` | `CONSTRAINT ck_stove0_admission_policy_id CHECK (length(policy_id) >= 1)` |
| <a id="s-491d0ac14d"></a>`check` | `ck_stove0_admission_policy_phase` | `CONSTRAINT ck_stove0_admission_policy_phase CHECK (phase IN ('new','baseline','following','reset_required'))` |
| <a id="s-4cd89a59ad"></a>`check` | `ck_stove0_admission_policy_baseline_mode` | `CONSTRAINT ck_stove0_admission_policy_baseline_mode CHECK (baseline_mode IN ('observe','backfill'))` |
| <a id="s-418322b3cc"></a>`check` | `ck_stove0_admission_policies_policy_sha256_hex` | `CONSTRAINT ck_stove0_admission_policies_policy_sha256_hex CHECK (length(policy_sha256) = 64 AND lower(policy_sha256) = policy_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(policy_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-5921963be0"></a>`check` | `ck_stove0_admission_policies_generation_hex` | `CONSTRAINT ck_stove0_admission_policies_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-79a94c69b6"></a>`check` | `ck_stove0_admission_policies_source_identity_hex` | `CONSTRAINT ck_stove0_admission_policies_source_identity_hex CHECK (source_identity IS NULL OR length(source_identity) = 64 AND lower(source_identity) = source_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-8d2f6024df"></a>`check` | `ck_stove0_admission_policies_authorization_view_identity_hex` | `CONSTRAINT ck_stove0_admission_policies_authorization_view_identity_hex CHECK (authorization_view_identity IS NULL OR length(authorization_view_identity) = 64 AND lower(authorization_view_identity) = authorization_view_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authorization_view_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [stove0-control durable-state identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-a18ee73de2"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:stove0-control](../../../evidence/sources.md#src-45e44b17fd) — `reference/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/3/structure/tables/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd51832058d27152fb26e634c827e1cc2862a29a6d79963ca1d462264455be4f -->

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
      "definition": "baseline_mode VARCHAR(16) NOT NULL",
      "name": "baseline_mode",
      "nullable": false,
      "type": "VARCHAR(16)"
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
      "definition": "CONSTRAINT ck_stove0_admission_policy_revision CHECK (policy_revision >= 1)",
      "expression": "(policy_revision >= 1)",
      "kind": "check",
      "name": "ck_stove0_admission_policy_revision"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_policy_id CHECK (length(policy_id) >= 1)",
      "expression": "(length(policy_id) >= 1)",
      "kind": "check",
      "name": "ck_stove0_admission_policy_id"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_policy_phase CHECK (phase IN ('new','baseline','following','reset_required'))",
      "expression": "(phase IN ('new','baseline','following','reset_required'))",
      "kind": "check",
      "name": "ck_stove0_admission_policy_phase"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_policy_baseline_mode CHECK (baseline_mode IN ('observe','backfill'))",
      "expression": "(baseline_mode IN ('observe','backfill'))",
      "kind": "check",
      "name": "ck_stove0_admission_policy_baseline_mode"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_policies_policy_sha256_hex CHECK (length(policy_sha256) = 64 AND lower(policy_sha256) = policy_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(policy_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(policy_sha256) = 64 AND lower(policy_sha256) = policy_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(policy_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_admission_policies_policy_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_policies_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_admission_policies_generation_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_policies_source_identity_hex CHECK (source_identity IS NULL OR length(source_identity) = 64 AND lower(source_identity) = source_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(source_identity IS NULL OR length(source_identity) = 64 AND lower(source_identity) = source_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_admission_policies_source_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_policies_authorization_view_identity_hex CHECK (authorization_view_identity IS NULL OR length(authorization_view_identity) = 64 AND lower(authorization_view_identity) = authorization_view_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authorization_view_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(authorization_view_identity IS NULL OR length(authorization_view_identity) = 64 AND lower(authorization_view_identity) = authorization_view_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authorization_view_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_admission_policies_authorization_view_identity_hex"
    }
  ],
  "name": "stove0_admission_policies"
}
```
