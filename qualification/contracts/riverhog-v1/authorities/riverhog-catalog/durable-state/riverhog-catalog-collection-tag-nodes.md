# riverhog-catalog: collection_tag_nodes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-nodes:2a8c388d13 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-4d3d34b691"></a>

### Table: `collection_tag_nodes`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-763105ac20"></a>`digest` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-f0101b7481"></a>`encoded` | `BYTEA` | no | `—` | — |
| <a id="s-f9bf45a961"></a>`created_at` | `VARCHAR` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-e0b3aac648"></a>`primary-key` | `—` | `PRIMARY KEY (digest)` |
| <a id="s-1ddad5ce22"></a>`check` | `ck_collection_tag_nodes_digest` | `CONSTRAINT ck_collection_tag_nodes_digest CHECK (length(digest) = 64 AND lower(digest) = digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-477ebc2d37"></a>`check` | `ck_collection_tag_nodes_bytes` | `CONSTRAINT ck_collection_tag_nodes_bytes CHECK (length(encoded) > 0 AND length(encoded) <= 131072)` |
| <a id="s-99b620a604"></a>`check` | `ck_collection_tag_nodes_digest_hex` | `CONSTRAINT ck_collection_tag_nodes_digest_hex CHECK (length(digest) = 64 AND lower(digest) = digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-704ee7204a"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/6`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5d09c9c9e32c7c30b7aa5f382a9512008219cf332f0fee02e64d41786934d55d -->

```json
{
  "columns": [
    {
      "definition": "digest VARCHAR(64) NOT NULL",
      "name": "digest",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "encoded BYTEA NOT NULL",
      "name": "encoded",
      "nullable": false,
      "type": "BYTEA"
    },
    {
      "definition": "created_at VARCHAR NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "digest"
      ],
      "definition": "PRIMARY KEY (digest)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_nodes_digest CHECK (length(digest) = 64 AND lower(digest) = digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(digest) = 64 AND lower(digest) = digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_nodes_digest"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_nodes_bytes CHECK (length(encoded) > 0 AND length(encoded) <= 131072)",
      "expression": "(length(encoded) > 0 AND length(encoded) <= 131072)",
      "kind": "check",
      "name": "ck_collection_tag_nodes_bytes"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_nodes_digest_hex CHECK (length(digest) = 64 AND lower(digest) = digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(digest) = 64 AND lower(digest) = digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_nodes_digest_hex"
    }
  ],
  "name": "collection_tag_nodes"
}
```

</details>
