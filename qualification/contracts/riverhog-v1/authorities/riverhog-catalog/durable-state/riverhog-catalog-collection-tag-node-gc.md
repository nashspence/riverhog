# riverhog-catalog: collection_tag_node_gc

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-node-gc:5a15c885cc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-88bf9910ea"></a>

### Table: `collection_tag_node_gc`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-8e986a5d99"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-052416dafd"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-2cf457d11d"></a>`incarnation_id` | `VARCHAR(36)` | no | `—` | — |
| <a id="s-a5aa6b2d02"></a>`node_digest` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-59c0bfa9ac"></a>`expected_head_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-11181a55cc"></a>`object_path` | `VARCHAR` | no | `—` | — |
| <a id="s-9cee2de888"></a>`provider_revision` | `VARCHAR` | yes | `—` | — |
| <a id="s-63b9cc30dd"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-6f1ed77644"></a>`next_attempt_at` | `VARCHAR` | no | `—` | — |
| <a id="s-1ab7a7facc"></a>`failure` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-4ab13d9a44"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store, node_digest)` |
| <a id="s-3d92348c64"></a>`foreign-key` | `—` | `FOREIGN KEY(incarnation_id, store) REFERENCES storage_incarnations (id, name)` |
| <a id="s-986e99e0a7"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, store, node_digest) REFERENCES collection_tag_published_nodes (collection_id, store, node_digest) ON DELETE CASCADE` |
| <a id="s-551bd4ca17"></a>`check` | `ck_collection_tag_node_gc_digest` | `CONSTRAINT ck_collection_tag_node_gc_digest CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-81e5d2d380"></a>`check` | `ck_collection_tag_node_gc_head` | `CONSTRAINT ck_collection_tag_node_gc_head CHECK (length(expected_head_identity) = 64 AND lower(expected_head_identity) = expected_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-3e9933a4e0"></a>`check` | `ck_collection_tag_node_gc_state` | `CONSTRAINT ck_collection_tag_node_gc_state CHECK (state IN ('pending','deleting','retry_wait'))` |
| <a id="s-4b997eddb7"></a>`check` | `ck_collection_tag_node_gc_node_digest_hex` | `CONSTRAINT ck_collection_tag_node_gc_node_digest_hex CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-9d9489bc53"></a>`check` | `ck_collection_tag_node_gc_expected_head_identity_hex` | `CONSTRAINT ck_collection_tag_node_gc_expected_head_identity_hex CHECK (length(expected_head_identity) = 64 AND lower(expected_head_identity) = expected_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-b668a658e3"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

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

<!-- exact-contract-value: c6db942821aae6d44ef02f7bd565655ec8d4e9aa120d7b4e61cef3a66c85b206 -->

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
      "definition": "incarnation_id VARCHAR(36) NOT NULL",
      "name": "incarnation_id",
      "nullable": false,
      "type": "VARCHAR(36)"
    },
    {
      "definition": "node_digest VARCHAR(64) NOT NULL",
      "name": "node_digest",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "expected_head_identity VARCHAR(64) NOT NULL",
      "name": "expected_head_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "object_path VARCHAR NOT NULL",
      "name": "object_path",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "provider_revision VARCHAR",
      "name": "provider_revision",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "next_attempt_at VARCHAR NOT NULL",
      "name": "next_attempt_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "failure TEXT",
      "name": "failure",
      "nullable": true,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "store",
        "node_digest"
      ],
      "definition": "PRIMARY KEY (collection_id, store, node_digest)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "incarnation_id",
        "store"
      ],
      "definition": "FOREIGN KEY(incarnation_id, store) REFERENCES storage_incarnations (id, name)",
      "kind": "foreign-key",
      "references": {
        "columns": [
          "id",
          "name"
        ],
        "table": "storage_incarnations"
      }
    },
    {
      "columns": [
        "collection_id",
        "store",
        "node_digest"
      ],
      "definition": "FOREIGN KEY(collection_id, store, node_digest) REFERENCES collection_tag_published_nodes (collection_id, store, node_digest) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id",
          "store",
          "node_digest"
        ],
        "table": "collection_tag_published_nodes"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_node_gc_digest CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_node_gc_digest"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_node_gc_head CHECK (length(expected_head_identity) = 64 AND lower(expected_head_identity) = expected_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(expected_head_identity) = 64 AND lower(expected_head_identity) = expected_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_node_gc_head"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_node_gc_state CHECK (state IN ('pending','deleting','retry_wait'))",
      "expression": "(state IN ('pending','deleting','retry_wait'))",
      "kind": "check",
      "name": "ck_collection_tag_node_gc_state"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_node_gc_node_digest_hex CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_node_gc_node_digest_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_node_gc_expected_head_identity_hex CHECK (length(expected_head_identity) = 64 AND lower(expected_head_identity) = expected_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(expected_head_identity) = 64 AND lower(expected_head_identity) = expected_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_node_gc_expected_head_identity_hex"
    }
  ],
  "name": "collection_tag_node_gc"
}
```

</details>
