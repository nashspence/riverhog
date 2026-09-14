# riverhog-catalog: collection_tag_nodes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-nodes:e1f7c64925 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-6f68f9e318"></a>
- Table: `collection_tag_nodes`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-a8f138f787"></a>`digest` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-a799814f1d"></a>`encoded` | `BYTEA` | no | `—` | — |
| <a id="s-eca992f774"></a>`created_at` | `VARCHAR` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-30364acc57"></a>`primary-key` | `—` | `PRIMARY KEY (digest)` |
| <a id="s-23d0e4cee7"></a>`check` | `ck_collection_tag_nodes_digest` | `CONSTRAINT ck_collection_tag_nodes_digest CHECK (length(digest) = 64 AND lower(digest) = digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-f908bf6951"></a>`check` | `ck_collection_tag_nodes_bytes` | `CONSTRAINT ck_collection_tag_nodes_bytes CHECK (length(encoded) > 0 AND length(encoded) <= 131072)` |
| <a id="s-82d846d0a9"></a>`check` | `ck_collection_tag_nodes_digest_hex` | `CONSTRAINT ck_collection_tag_nodes_digest_hex CHECK (length(digest) = 64 AND lower(digest) = digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [riverhog-catalog durable-state identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-cb9c33f408"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/5`

### Exact owned JSON

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
