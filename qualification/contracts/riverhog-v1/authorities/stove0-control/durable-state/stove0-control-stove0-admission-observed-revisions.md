# stove0-control: stove0_admission_observed_revisions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-admission-observed-revisions:78d234491c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-8896d26a74"></a>

### Table: `stove0_admission_observed_revisions`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-f90539a670"></a>`policy_id` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-e734579d57"></a>`generation` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-1bd6cdb6ee"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-f93b65b455"></a>`descriptor_revision` | `VARCHAR(19)` | no | `—` | — |
| <a id="s-1b5bd822ff"></a>`operation` | `VARCHAR(8)` | no | `—` | — |
| <a id="s-be6b6a135e"></a>`authority_sha256` | `VARCHAR(64)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-4ab1932ad4"></a>`primary-key` | `—` | `PRIMARY KEY (policy_id, generation, collection_id)` |
| <a id="s-7ed2444305"></a>`check` | `ck_stove0_admission_observed_revision_collection` | `CONSTRAINT ck_stove0_admission_observed_revision_collection CHECK (collection_id >= 1)` |
| <a id="s-b25f7c68eb"></a>`check` | `ck_stove0_admission_observed_revision_operation` | `CONSTRAINT ck_stove0_admission_observed_revision_operation CHECK (operation IN ('upsert','delete'))` |
| <a id="s-ea8b5724ef"></a>`check` | `ck_stove0_admission_observed_revisions_generation_hex` | `CONSTRAINT ck_stove0_admission_observed_revisions_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-6f84756d76"></a>`check` | `ck_stove0_admission_observed_revisions_authority_sha256_hex` | `CONSTRAINT ck_stove0_admission_observed_revisions_authority_sha256_hex CHECK (length(authority_sha256) = 64 AND lower(authority_sha256) = authority_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authority_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-b90ca96784"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [reference/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../reference/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/3/structure/tables/2`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f81663902f45fb9543349b1dc3eb788cb09ed5ebbcd70e414125bc53846c370 -->

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
      "definition": "generation VARCHAR(64) NOT NULL",
      "name": "generation",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "collection_id BIGINT NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "descriptor_revision VARCHAR(19) NOT NULL",
      "name": "descriptor_revision",
      "nullable": false,
      "type": "VARCHAR(19)"
    },
    {
      "definition": "operation VARCHAR(8) NOT NULL",
      "name": "operation",
      "nullable": false,
      "type": "VARCHAR(8)"
    },
    {
      "definition": "authority_sha256 VARCHAR(64) NOT NULL",
      "name": "authority_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "policy_id",
        "generation",
        "collection_id"
      ],
      "definition": "PRIMARY KEY (policy_id, generation, collection_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_observed_revision_collection CHECK (collection_id >= 1)",
      "expression": "(collection_id >= 1)",
      "kind": "check",
      "name": "ck_stove0_admission_observed_revision_collection"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_observed_revision_operation CHECK (operation IN ('upsert','delete'))",
      "expression": "(operation IN ('upsert','delete'))",
      "kind": "check",
      "name": "ck_stove0_admission_observed_revision_operation"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_observed_revisions_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_admission_observed_revisions_generation_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_observed_revisions_authority_sha256_hex CHECK (length(authority_sha256) = 64 AND lower(authority_sha256) = authority_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authority_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(authority_sha256) = 64 AND lower(authority_sha256) = authority_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(authority_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_admission_observed_revisions_authority_sha256_hex"
    }
  ],
  "name": "stove0_admission_observed_revisions"
}
```

</details>
