# riverhog-catalog: collection_tag_node_edges

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-node-edges:656d0bbcdb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-ad68eae1ff"></a>
- Table: `collection_tag_node_edges`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-772cb5f04d"></a>`parent_digest` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-599dab012a"></a>`child_digest` | `VARCHAR(64)` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-9d1a4fe4bf"></a>`primary-key` | `—` | `PRIMARY KEY (parent_digest, child_digest)` |
| <a id="s-a0d85149e2"></a>`foreign-key` | `—` | `FOREIGN KEY(parent_digest) REFERENCES collection_tag_nodes (digest)` |
| <a id="s-adfbb39d2a"></a>`foreign-key` | `—` | `FOREIGN KEY(child_digest) REFERENCES collection_tag_nodes (digest)` |
| <a id="s-53ea7e434f"></a>`check` | `ck_collection_tag_node_edges_parent_digest` | `CONSTRAINT ck_collection_tag_node_edges_parent_digest CHECK (length(parent_digest) = 64 AND lower(parent_digest) = parent_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(parent_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-899a8c9a94"></a>`check` | `ck_collection_tag_node_edges_child_digest` | `CONSTRAINT ck_collection_tag_node_edges_child_digest CHECK (length(child_digest) = 64 AND lower(child_digest) = child_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(child_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-fd126391c1"></a>`check` | `ck_collection_tag_node_edges_parent_digest_hex` | `CONSTRAINT ck_collection_tag_node_edges_parent_digest_hex CHECK (length(parent_digest) = 64 AND lower(parent_digest) = parent_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(parent_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-7e5a930328"></a>`check` | `ck_collection_tag_node_edges_child_digest_hex` | `CONSTRAINT ck_collection_tag_node_edges_child_digest_hex CHECK (length(child_digest) = 64 AND lower(child_digest) = child_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(child_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-9f3ff220c3"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/7`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a589391d65fd83c81577ac930cf0da35105568c9af9089d45fe1309b17e1d043 -->

```json
{
  "columns": [
    {
      "definition": "parent_digest VARCHAR(64) NOT NULL",
      "name": "parent_digest",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "child_digest VARCHAR(64) NOT NULL",
      "name": "child_digest",
      "nullable": false,
      "type": "VARCHAR(64)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "parent_digest",
        "child_digest"
      ],
      "definition": "PRIMARY KEY (parent_digest, child_digest)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "parent_digest"
      ],
      "definition": "FOREIGN KEY(parent_digest) REFERENCES collection_tag_nodes (digest)",
      "kind": "foreign-key",
      "references": {
        "columns": [
          "digest"
        ],
        "table": "collection_tag_nodes"
      }
    },
    {
      "columns": [
        "child_digest"
      ],
      "definition": "FOREIGN KEY(child_digest) REFERENCES collection_tag_nodes (digest)",
      "kind": "foreign-key",
      "references": {
        "columns": [
          "digest"
        ],
        "table": "collection_tag_nodes"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_node_edges_parent_digest CHECK (length(parent_digest) = 64 AND lower(parent_digest) = parent_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(parent_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(parent_digest) = 64 AND lower(parent_digest) = parent_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(parent_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_node_edges_parent_digest"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_node_edges_child_digest CHECK (length(child_digest) = 64 AND lower(child_digest) = child_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(child_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(child_digest) = 64 AND lower(child_digest) = child_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(child_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_node_edges_child_digest"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_node_edges_parent_digest_hex CHECK (length(parent_digest) = 64 AND lower(parent_digest) = parent_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(parent_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(parent_digest) = 64 AND lower(parent_digest) = parent_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(parent_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_node_edges_parent_digest_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_node_edges_child_digest_hex CHECK (length(child_digest) = 64 AND lower(child_digest) = child_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(child_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(child_digest) = 64 AND lower(child_digest) = child_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(child_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_node_edges_child_digest_hex"
    }
  ],
  "name": "collection_tag_node_edges"
}
```
