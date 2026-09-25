# riverhog-catalog: collection_tag_visibility

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-visibility:313bbcba09 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-2941fcb793"></a>

### Table: `collection_tag_visibility`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-e6a3102ca4"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-45a26679f2"></a>`tag_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-f7e0e7026b"></a>`start_revision` | `BIGINT` | no | `—` | — |
| <a id="s-c4cdabcd66"></a>`end_revision` | `BIGINT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-4530713694"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, tag_sha256, start_revision)` |
| <a id="s-2730127e5c"></a>`check` | `ck_collection_tag_visibility_sha256` | `CONSTRAINT ck_collection_tag_visibility_sha256 CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-945a2aeec6"></a>`check` | `ck_collection_tag_visibility_start` | `CONSTRAINT ck_collection_tag_visibility_start CHECK (start_revision >= 1 AND start_revision <= 9007199254740991)` |
| <a id="s-2c57e5524e"></a>`check` | `ck_collection_tag_visibility_end` | `CONSTRAINT ck_collection_tag_visibility_end CHECK (end_revision IS NULL OR end_revision > start_revision AND end_revision <= 9007199254740991)` |
| <a id="s-1411e97bbc"></a>`check` | `ck_collection_tag_visibility_tag_sha256_hex` | `CONSTRAINT ck_collection_tag_visibility_tag_sha256_hex CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-ff39690edf"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/9`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
