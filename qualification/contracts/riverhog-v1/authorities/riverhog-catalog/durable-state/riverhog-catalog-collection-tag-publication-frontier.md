# riverhog-catalog: collection_tag_publication_frontier

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-publication-frontier:4472b02d1f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-5e15c17a39"></a>
- Table: `collection_tag_publication_frontier`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-e372eb26e2"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-4b768c7212"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-5371ae3ac6"></a>`head_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-60e49570cd"></a>`node_digest` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-2c48cc5719"></a>`expanded` | `BOOLEAN` | no | `false` | — |
| <a id="s-c7b45d8f32"></a>`published` | `BOOLEAN` | no | `false` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-8e05875b86"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store, head_identity, node_digest)` |
| <a id="s-ac6db663d5"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, store) REFERENCES collection_tag_publications (collection_id, store) ON DELETE CASCADE` |
| <a id="s-1c5bd8767a"></a>`check` | `ck_collection_tag_publication_frontier_head` | `CONSTRAINT ck_collection_tag_publication_frontier_head CHECK (length(head_identity) = 64 AND lower(head_identity) = head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-007f57e27f"></a>`check` | `ck_collection_tag_publication_frontier_node` | `CONSTRAINT ck_collection_tag_publication_frontier_node CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-783a406675"></a>`check` | `ck_collection_tag_publication_frontier_head_identity_hex` | `CONSTRAINT ck_collection_tag_publication_frontier_head_identity_hex CHECK (length(head_identity) = 64 AND lower(head_identity) = head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-629d80da66"></a>`check` | `ck_collection_tag_publication_frontier_node_digest_hex` | `CONSTRAINT ck_collection_tag_publication_frontier_node_digest_hex CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [riverhog-catalog durable-state identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-c40f2dfef1"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/72`

### Exact owned JSON

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
