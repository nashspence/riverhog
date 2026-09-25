# riverhog-catalog: collection_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tags:9701aebbd7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-0dcdf28a9e"></a>

### Table: `collection_tags`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-d601b16a2d"></a>`tag_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-8d00d34a79"></a>`tag` | `TEXT` | no | `—` | — |
| <a id="s-bff7e96752"></a>`search_text` | `TEXT` | no | `—` | — |
| <a id="s-5fc246700a"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-a48ee9c9b4"></a>`updated_at` | `VARCHAR` | no | `—` | — |
| <a id="s-384daf9286"></a>`collection_count` | `BIGINT` | no | `0` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-2bda372a88"></a>`primary-key` | `—` | `PRIMARY KEY (tag_sha256)` |
| <a id="s-0c435c0da4"></a>`check` | `ck_collection_tags_collection_count` | `CONSTRAINT ck_collection_tags_collection_count CHECK (collection_count >= 0)` |
| <a id="s-c6d82b9c25"></a>`check` | `ck_collection_tags_bytes` | `CONSTRAINT ck_collection_tags_bytes CHECK (octet_length(tag) > 0 AND octet_length(tag) <= 65536)` |
| <a id="s-c2645773a2"></a>`check` | `ck_collection_tags_sha256` | `CONSTRAINT ck_collection_tags_sha256 CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-64010cac99"></a>`unique` | `—` | `UNIQUE (tag)` |
| <a id="s-afd23cf666"></a>`check` | `ck_collection_tags_tag_sha256_hex` | `CONSTRAINT ck_collection_tags_tag_sha256_hex CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-ef7a0252cf"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/10`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b09e69b3f73b7c2ad0595ec7508265eb3e61b3cc5fa6ad3536d12a12ec474207 -->

```json
{
  "columns": [
    {
      "definition": "tag_sha256 VARCHAR(64) NOT NULL",
      "name": "tag_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "tag TEXT NOT NULL",
      "name": "tag",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "search_text TEXT NOT NULL",
      "name": "search_text",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "created_at VARCHAR NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "updated_at VARCHAR NOT NULL",
      "name": "updated_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "default": "0",
      "definition": "collection_count BIGINT DEFAULT 0 NOT NULL",
      "name": "collection_count",
      "nullable": false,
      "type": "BIGINT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "tag_sha256"
      ],
      "definition": "PRIMARY KEY (tag_sha256)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_collection_tags_collection_count CHECK (collection_count >= 0)",
      "expression": "(collection_count >= 0)",
      "kind": "check",
      "name": "ck_collection_tags_collection_count"
    },
    {
      "definition": "CONSTRAINT ck_collection_tags_bytes CHECK (octet_length(tag) > 0 AND octet_length(tag) <= 65536)",
      "expression": "(octet_length(tag) > 0 AND octet_length(tag) <= 65536)",
      "kind": "check",
      "name": "ck_collection_tags_bytes"
    },
    {
      "definition": "CONSTRAINT ck_collection_tags_sha256 CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tags_sha256"
    },
    {
      "columns": [
        "tag"
      ],
      "definition": "UNIQUE (tag)",
      "kind": "unique"
    },
    {
      "definition": "CONSTRAINT ck_collection_tags_tag_sha256_hex CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tags_tag_sha256_hex"
    }
  ],
  "name": "collection_tags"
}
```

</details>
