# riverhog-catalog: collection_tag_node_gc

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-node-gc:bbe55cb086 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-d73c4686f2"></a>

### Table: `collection_tag_node_gc`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1250dfe579"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-4c71906f53"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-ca4491da34"></a>`node_digest` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-dc20a39a2f"></a>`expected_head_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-d26c896d55"></a>`object_path` | `VARCHAR` | no | `—` | — |
| <a id="s-a2bfca2a40"></a>`provider_revision` | `VARCHAR` | yes | `—` | — |
| <a id="s-ce71694687"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-aa47ae46c1"></a>`next_attempt_at` | `VARCHAR` | no | `—` | — |
| <a id="s-f0bd2c217b"></a>`failure` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-6144105291"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store, node_digest)` |
| <a id="s-7c2cdc0223"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, store, node_digest) REFERENCES collection_tag_published_nodes (collection_id, store, node_digest) ON DELETE CASCADE` |
| <a id="s-69b2b169cb"></a>`check` | `ck_collection_tag_node_gc_digest` | `CONSTRAINT ck_collection_tag_node_gc_digest CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-d8aace839a"></a>`check` | `ck_collection_tag_node_gc_head` | `CONSTRAINT ck_collection_tag_node_gc_head CHECK (length(expected_head_identity) = 64 AND lower(expected_head_identity) = expected_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-008dede6d0"></a>`check` | `ck_collection_tag_node_gc_state` | `CONSTRAINT ck_collection_tag_node_gc_state CHECK (state IN ('pending','deleting','retry_wait'))` |
| <a id="s-11c5deb2c5"></a>`check` | `ck_collection_tag_node_gc_node_digest_hex` | `CONSTRAINT ck_collection_tag_node_gc_node_digest_hex CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-10a2c0646f"></a>`check` | `ck_collection_tag_node_gc_expected_head_identity_hex` | `CONSTRAINT ck_collection_tag_node_gc_expected_head_identity_hex CHECK (length(expected_head_identity) = 64 AND lower(expected_head_identity) = expected_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-cc92a53adf"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/71`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4b55819f2fb08c976e65a89af34e391e78f1fba5ec9cc62a45d32852667b62a2 -->

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
