# riverhog-catalog: collection_tag_visibility

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-visibility:6e1d37abf0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-836867470f"></a>
- Table: `collection_tag_visibility`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-543f1173bd"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-8cf50124da"></a>`tag_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-b581846f01"></a>`start_revision` | `BIGINT` | no | `—` | — |
| <a id="s-c015ab3577"></a>`end_revision` | `BIGINT` | yes | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-c046d2085b"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, tag_sha256, start_revision)` |
| <a id="s-48169f3bfb"></a>`check` | `ck_collection_tag_visibility_sha256` | `CONSTRAINT ck_collection_tag_visibility_sha256 CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-e35b6107ac"></a>`check` | `ck_collection_tag_visibility_start` | `CONSTRAINT ck_collection_tag_visibility_start CHECK (start_revision >= 1 AND start_revision <= 9007199254740991)` |
| <a id="s-57df76227e"></a>`check` | `ck_collection_tag_visibility_end` | `CONSTRAINT ck_collection_tag_visibility_end CHECK (end_revision IS NULL OR end_revision > start_revision AND end_revision <= 9007199254740991)` |
| <a id="s-f704d0b8d6"></a>`check` | `ck_collection_tag_visibility_tag_sha256_hex` | `CONSTRAINT ck_collection_tag_visibility_tag_sha256_hex CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-e069ecb4ba"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/8`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4fb719258eda13fb248fbb6311e48ce93dddea71b6b28e13e7d1942e9bfe46fd -->

```json
{
  "columns": [
    {
      "definition": "collection_id BIGINT NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "tag_sha256 VARCHAR(64) NOT NULL",
      "name": "tag_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "start_revision BIGINT NOT NULL",
      "name": "start_revision",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "end_revision BIGINT",
      "name": "end_revision",
      "nullable": true,
      "type": "BIGINT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "tag_sha256",
        "start_revision"
      ],
      "definition": "PRIMARY KEY (collection_id, tag_sha256, start_revision)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_visibility_sha256 CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_visibility_sha256"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_visibility_start CHECK (start_revision >= 1 AND start_revision <= 9007199254740991)",
      "expression": "(start_revision >= 1 AND start_revision <= 9007199254740991)",
      "kind": "check",
      "name": "ck_collection_tag_visibility_start"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_visibility_end CHECK (end_revision IS NULL OR end_revision > start_revision AND end_revision <= 9007199254740991)",
      "expression": "(end_revision IS NULL OR end_revision > start_revision AND end_revision <= 9007199254740991)",
      "kind": "check",
      "name": "ck_collection_tag_visibility_end"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_visibility_tag_sha256_hex CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_visibility_tag_sha256_hex"
    }
  ],
  "name": "collection_tag_visibility"
}
```
