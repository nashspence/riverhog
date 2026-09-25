# stove0-control: stove0_admission_matches

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-admission-matches:6797ddcda4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-fd5602c5b6"></a>

### Table: `stove0_admission_matches`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-6c88353fc3"></a>`policy_id` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-f1f083007b"></a>`generation` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-dce147fd70"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-d630f56923"></a>`matched` | `BOOLEAN` | no | `—` | — |
| <a id="s-bb2cb8fe28"></a>`descriptor_revision` | `VARCHAR(19)` | no | `—` | — |
| <a id="s-4e5f341522"></a>`tag_revision` | `BIGINT` | no | `—` | — |
| <a id="s-8977dce01e"></a>`tag_set_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-2716440084"></a>`document_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-f7f27bc862"></a>`document_json` | `TEXT` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-80e6b7a809"></a>`primary-key` | `—` | `PRIMARY KEY (policy_id, generation, collection_id)` |
| <a id="s-eca51385f8"></a>`check` | `ck_stove0_admission_match_collection` | `CONSTRAINT ck_stove0_admission_match_collection CHECK (collection_id >= 1)` |
| <a id="s-a103db3377"></a>`check` | `ck_stove0_admission_match_tag_revision` | `CONSTRAINT ck_stove0_admission_match_tag_revision CHECK (tag_revision >= 1)` |
| <a id="s-b27c89f807"></a>`check` | `ck_stove0_admission_match_bytes` | `CONSTRAINT ck_stove0_admission_match_bytes CHECK (document_bytes >= 0)` |
| <a id="s-88dad2677c"></a>`check` | `ck_stove0_admission_matches_generation_hex` | `CONSTRAINT ck_stove0_admission_matches_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-2e49235e0b"></a>`check` | `ck_stove0_admission_matches_tag_set_identity_hex` | `CONSTRAINT ck_stove0_admission_matches_tag_set_identity_hex CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-0605ef8e2d"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/5/structure/tables/4`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 943418120af43f9745b922bdaee6afe3c84284d32c70dd0ce8ba19f41bc032b6 -->

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
      "definition": "matched BOOLEAN NOT NULL",
      "name": "matched",
      "nullable": false,
      "type": "BOOLEAN"
    },
    {
      "definition": "descriptor_revision VARCHAR(19) NOT NULL",
      "name": "descriptor_revision",
      "nullable": false,
      "type": "VARCHAR(19)"
    },
    {
      "definition": "tag_revision BIGINT NOT NULL",
      "name": "tag_revision",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "tag_set_identity VARCHAR(64) NOT NULL",
      "name": "tag_set_identity",
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
        "policy_id",
        "generation",
        "collection_id"
      ],
      "definition": "PRIMARY KEY (policy_id, generation, collection_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_match_collection CHECK (collection_id >= 1)",
      "expression": "(collection_id >= 1)",
      "kind": "check",
      "name": "ck_stove0_admission_match_collection"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_match_tag_revision CHECK (tag_revision >= 1)",
      "expression": "(tag_revision >= 1)",
      "kind": "check",
      "name": "ck_stove0_admission_match_tag_revision"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_match_bytes CHECK (document_bytes >= 0)",
      "expression": "(document_bytes >= 0)",
      "kind": "check",
      "name": "ck_stove0_admission_match_bytes"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_matches_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_admission_matches_generation_hex"
    },
    {
      "definition": "CONSTRAINT ck_stove0_admission_matches_tag_set_identity_hex CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_stove0_admission_matches_tag_set_identity_hex"
    }
  ],
  "name": "stove0_admission_matches"
}
```

</details>
