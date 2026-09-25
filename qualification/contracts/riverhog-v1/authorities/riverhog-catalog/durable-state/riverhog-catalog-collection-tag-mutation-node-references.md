# riverhog-catalog: collection_tag_mutation_node_references

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-mutation-9ec54ff9df:259b55d7dd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-5c534a7cd1"></a>

### Table: `collection_tag_mutation_node_references`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-4e8db8a2bd"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-ee0619fdfd"></a>`operation_id` | `VARCHAR` | no | `—` | — |
| <a id="s-cf60d564c7"></a>`node_digest` | `VARCHAR(64)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-ed3b17ea02"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, operation_id, node_digest)` |
| <a id="s-f653cbd2a0"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, operation_id) REFERENCES collection_tag_mutations (collection_id, operation_id) ON DELETE CASCADE` |
| <a id="s-0e2db9fec1"></a>`foreign-key` | `—` | `FOREIGN KEY(node_digest) REFERENCES collection_tag_nodes (digest)` |
| <a id="s-2f9d7ae2a2"></a>`check` | `ck_collection_tag_mutation_node_references_digest` | `CONSTRAINT ck_collection_tag_mutation_node_references_digest CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-35502dfd43"></a>`check` | `ck_collection_tag_mutation_node_references_node_digest_hex` | `CONSTRAINT ck_collection_tag_mutation_node_references_node_digest_hex CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-fb42252843"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/28`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59dcef033f7c97f8e831c211faf1080487224c3f36feda3ee0c47f93c4bfc030 -->

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
      "definition": "operation_id VARCHAR NOT NULL",
      "name": "operation_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "node_digest VARCHAR(64) NOT NULL",
      "name": "node_digest",
      "nullable": false,
      "type": "VARCHAR(64)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "operation_id",
        "node_digest"
      ],
      "definition": "PRIMARY KEY (collection_id, operation_id, node_digest)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id",
        "operation_id"
      ],
      "definition": "FOREIGN KEY(collection_id, operation_id) REFERENCES collection_tag_mutations (collection_id, operation_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id",
          "operation_id"
        ],
        "table": "collection_tag_mutations"
      }
    },
    {
      "columns": [
        "node_digest"
      ],
      "definition": "FOREIGN KEY(node_digest) REFERENCES collection_tag_nodes (digest)",
      "kind": "foreign-key",
      "references": {
        "columns": [
          "digest"
        ],
        "table": "collection_tag_nodes"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_mutation_node_references_digest CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_mutation_node_references_digest"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_mutation_node_references_node_digest_hex CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_mutation_node_references_node_digest_hex"
    }
  ],
  "name": "collection_tag_mutation_node_references"
}
```

</details>
