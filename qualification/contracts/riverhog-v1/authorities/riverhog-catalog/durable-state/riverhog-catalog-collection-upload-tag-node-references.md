# riverhog-catalog: collection_upload_tag_node_references

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-tag-no-6f8aaa3bc6:afd2cf95e6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-a2db780a45"></a>

### Table: `collection_upload_tag_node_references`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1bb399350c"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-2e2de983dc"></a>`node_digest` | `VARCHAR(64)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-00a009e43e"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, node_digest)` |
| <a id="s-ed22eff2e3"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_uploads (collection_id) ON DELETE CASCADE` |
| <a id="s-a9dedea45a"></a>`foreign-key` | `—` | `FOREIGN KEY(node_digest) REFERENCES collection_tag_nodes (digest) ON DELETE RESTRICT` |
| <a id="s-be50a8c351"></a>`check` | `ck_collection_upload_tag_node_references_digest` | `CONSTRAINT ck_collection_upload_tag_node_references_digest CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-0ba57b918c"></a>`check` | `ck_collection_upload_tag_node_references_node_digest_hex` | `CONSTRAINT ck_collection_upload_tag_node_references_node_digest_hex CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-3357e19581"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/32`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 78a56a450075bada74e9b25eb386a67160ef03716ddd37a93ee75f68e3c2f84d -->

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
        "node_digest"
      ],
      "definition": "PRIMARY KEY (collection_id, node_digest)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id"
      ],
      "definition": "FOREIGN KEY(collection_id) REFERENCES collection_uploads (collection_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id"
        ],
        "table": "collection_uploads"
      }
    },
    {
      "columns": [
        "node_digest"
      ],
      "definition": "FOREIGN KEY(node_digest) REFERENCES collection_tag_nodes (digest) ON DELETE RESTRICT",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE RESTRICT",
        "columns": [
          "digest"
        ],
        "table": "collection_tag_nodes"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_tag_node_references_digest CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_upload_tag_node_references_digest"
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_tag_node_references_node_digest_hex CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_upload_tag_node_references_node_digest_hex"
    }
  ],
  "name": "collection_upload_tag_node_references"
}
```

</details>
