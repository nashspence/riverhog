# riverhog-catalog: collection_tag_publication_frontier

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-publication-frontier:7d90eee97c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-88bf9910ea"></a>

### Table: `collection_tag_publication_frontier`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-8e986a5d99"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-052416dafd"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-2cf457d11d"></a>`head_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-a5aa6b2d02"></a>`node_digest` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-59c0bfa9ac"></a>`expanded` | `BOOLEAN` | no | `false` | — |
| <a id="s-11181a55cc"></a>`published` | `BOOLEAN` | no | `false` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-4ab13d9a44"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store, head_identity, node_digest)` |
| <a id="s-3d92348c64"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, store) REFERENCES collection_tag_publications (collection_id, store) ON DELETE CASCADE` |
| <a id="s-986e99e0a7"></a>`check` | `ck_collection_tag_publication_frontier_head` | `CONSTRAINT ck_collection_tag_publication_frontier_head CHECK (length(head_identity) = 64 AND lower(head_identity) = head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-551bd4ca17"></a>`check` | `ck_collection_tag_publication_frontier_node` | `CONSTRAINT ck_collection_tag_publication_frontier_node CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-81e5d2d380"></a>`check` | `ck_collection_tag_publication_frontier_head_identity_hex` | `CONSTRAINT ck_collection_tag_publication_frontier_head_identity_hex CHECK (length(head_identity) = 64 AND lower(head_identity) = head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-3e9933a4e0"></a>`check` | `ck_collection_tag_publication_frontier_node_digest_hex` | `CONSTRAINT ck_collection_tag_publication_frontier_node_digest_hex CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-e47651ae6a"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/73`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 659bcb448bf796e3b02ea37bf2610cf80b063c2ded2ebdfbba290d9539de5896 -->

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
      "definition": "store VARCHAR NOT NULL",
      "name": "store",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "head_identity VARCHAR(64) NOT NULL",
      "name": "head_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "node_digest VARCHAR(64) NOT NULL",
      "name": "node_digest",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "default": "false",
      "definition": "expanded BOOLEAN DEFAULT false NOT NULL",
      "name": "expanded",
      "nullable": false,
      "type": "BOOLEAN"
    },
    {
      "default": "false",
      "definition": "published BOOLEAN DEFAULT false NOT NULL",
      "name": "published",
      "nullable": false,
      "type": "BOOLEAN"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "store",
        "head_identity",
        "node_digest"
      ],
      "definition": "PRIMARY KEY (collection_id, store, head_identity, node_digest)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id",
        "store"
      ],
      "definition": "FOREIGN KEY(collection_id, store) REFERENCES collection_tag_publications (collection_id, store) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id",
          "store"
        ],
        "table": "collection_tag_publications"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_publication_frontier_head CHECK (length(head_identity) = 64 AND lower(head_identity) = head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(head_identity) = 64 AND lower(head_identity) = head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_publication_frontier_head"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_publication_frontier_node CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_publication_frontier_node"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_publication_frontier_head_identity_hex CHECK (length(head_identity) = 64 AND lower(head_identity) = head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(head_identity) = 64 AND lower(head_identity) = head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_publication_frontier_head_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_publication_frontier_node_digest_hex CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_publication_frontier_node_digest_hex"
    }
  ],
  "name": "collection_tag_publication_frontier"
}
```

</details>
