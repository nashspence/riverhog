# riverhog-catalog: collection_upload_tag_publication_frontier

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-tag-pu-d83bf3a033:80a6054897 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-a2db780a45"></a>

### Table: `collection_upload_tag_publication_frontier`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1bb399350c"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-2e2de983dc"></a>`node_digest` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-f01985f377"></a>`expanded` | `BOOLEAN` | no | `false` | — |
| <a id="s-89181597fc"></a>`published` | `BOOLEAN` | no | `false` | — |
| <a id="s-4c4d33766e"></a>`object_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-4e074482ca"></a>`provider_revision` | `VARCHAR` | yes | `—` | — |
| <a id="s-6774180e24"></a>`stored_bytes` | `BIGINT` | yes | `—` | — |
| <a id="s-1f0e96365d"></a>`stored_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-fb9cb459d6"></a>`published_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-00a009e43e"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, node_digest)` |
| <a id="s-ed22eff2e3"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_uploads (collection_id) ON DELETE CASCADE` |
| <a id="s-a9dedea45a"></a>`check` | `ck_collection_upload_tag_frontier_digest` | `CONSTRAINT ck_collection_upload_tag_frontier_digest CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-be50a8c351"></a>`check` | `ck_collection_upload_tag_frontier_receipt` | `CONSTRAINT ck_collection_upload_tag_frontier_receipt CHECK (published = false AND object_path IS NULL AND stored_bytes IS NULL AND stored_sha256 IS NULL AND published_at IS NULL OR published = true AND object_path IS NOT NULL AND stored_bytes > 0 AND stored_sha256 IS NOT NULL AND published_at IS NOT NULL)` |
| <a id="s-0ba57b918c"></a>`check` | `ck_sha256_a9a6767f54e4c1ef` | `CONSTRAINT ck_sha256_a9a6767f54e4c1ef CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-2d54d01f5e"></a>`check` | `ck_sha256_6acac9b414fb5e15` | `CONSTRAINT ck_sha256_6acac9b414fb5e15 CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-614981445d"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/32`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 05f94645d264366ee5e5e9c3b2b84ccaf247b4a88e3609fcd15688c2618e2f54 -->

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
    },
    {
      "definition": "object_path VARCHAR",
      "name": "object_path",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "provider_revision VARCHAR",
      "name": "provider_revision",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "stored_bytes BIGINT",
      "name": "stored_bytes",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "stored_sha256 VARCHAR(64)",
      "name": "stored_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "published_at VARCHAR",
      "name": "published_at",
      "nullable": true,
      "type": "VARCHAR"
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
      "definition": "CONSTRAINT ck_collection_upload_tag_frontier_digest CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_upload_tag_frontier_digest"
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_tag_frontier_receipt CHECK (published = false AND object_path IS NULL AND stored_bytes IS NULL AND stored_sha256 IS NULL AND published_at IS NULL OR published = true AND object_path IS NOT NULL AND stored_bytes > 0 AND stored_sha256 IS NOT NULL AND published_at IS NOT NULL)",
      "expression": "(published = false AND object_path IS NULL AND stored_bytes IS NULL AND stored_sha256 IS NULL AND published_at IS NULL OR published = true AND object_path IS NOT NULL AND stored_bytes > 0 AND stored_sha256 IS NOT NULL AND published_at IS NOT NULL)",
      "kind": "check",
      "name": "ck_collection_upload_tag_frontier_receipt"
    },
    {
      "definition": "CONSTRAINT ck_sha256_a9a6767f54e4c1ef CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_a9a6767f54e4c1ef"
    },
    {
      "definition": "CONSTRAINT ck_sha256_6acac9b414fb5e15 CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_6acac9b414fb5e15"
    }
  ],
  "name": "collection_upload_tag_publication_frontier"
}
```

</details>
