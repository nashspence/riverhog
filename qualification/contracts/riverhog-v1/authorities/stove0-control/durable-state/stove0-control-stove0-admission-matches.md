# stove0-control: stove0_admission_matches

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-stove0-admission-matches:91c9ddebe3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-e58bef7e49"></a>
- Table: `stove0_admission_matches`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-14cfe6c26b"></a>`policy_id` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-3c62a5ea32"></a>`generation` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-0fcbee6a32"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-9d801259b9"></a>`matched` | `BOOLEAN` | no | `—` | — |
| <a id="s-d2ca25b581"></a>`descriptor_revision` | `VARCHAR(19)` | no | `—` | — |
| <a id="s-a7549c86bf"></a>`tag_revision` | `BIGINT` | no | `—` | — |
| <a id="s-863698b0d2"></a>`tag_set_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-9e18cd2ef1"></a>`document_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-2fe5dbb4b6"></a>`document_json` | `TEXT` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-c6d0508442"></a>`primary-key` | `—` | `PRIMARY KEY (policy_id, generation, collection_id)` |
| <a id="s-c6dc358e18"></a>`check` | `ck_stove0_admission_match_collection` | `CONSTRAINT ck_stove0_admission_match_collection CHECK (collection_id >= 1)` |
| <a id="s-39b80fd0d2"></a>`check` | `ck_stove0_admission_match_tag_revision` | `CONSTRAINT ck_stove0_admission_match_tag_revision CHECK (tag_revision >= 1)` |
| <a id="s-e7473b9e05"></a>`check` | `ck_stove0_admission_match_bytes` | `CONSTRAINT ck_stove0_admission_match_bytes CHECK (document_bytes >= 0)` |
| <a id="s-5021d18387"></a>`check` | `ck_stove0_admission_matches_generation_hex` | `CONSTRAINT ck_stove0_admission_matches_generation_hex CHECK (length(generation) = 64 AND lower(generation) = generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-16667e052f"></a>`check` | `ck_stove0_admission_matches_tag_set_identity_hex` | `CONSTRAINT ck_stove0_admission_matches_tag_set_identity_hex CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-1f1d8b8afe"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:stove0-control](../../../evidence/sources.md#src-45e44b17fd) — `reference/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/3/structure/tables/1`

### Exact owned JSON

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
