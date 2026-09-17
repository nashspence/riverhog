# riverhog-catalog: collection_upload_tag_publication_frontier

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-tag-pu-d83bf3a033:593302df51 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-ccf86d0d1d"></a>

### Table: `collection_upload_tag_publication_frontier`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-381cd9629a"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-4ea96d1eff"></a>`node_digest` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-fe12289701"></a>`expanded` | `BOOLEAN` | no | `false` | — |
| <a id="s-6391d15c05"></a>`published` | `BOOLEAN` | no | `false` | — |
| <a id="s-131f9fd4d2"></a>`object_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-261eebe213"></a>`provider_revision` | `VARCHAR` | yes | `—` | — |
| <a id="s-28921ff745"></a>`stored_bytes` | `BIGINT` | yes | `—` | — |
| <a id="s-d21225e6b5"></a>`stored_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-be6820f9c3"></a>`published_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-5b95236dcf"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, node_digest)` |
| <a id="s-f6d3e6c966"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_uploads (collection_id) ON DELETE CASCADE` |
| <a id="s-b777884233"></a>`check` | `ck_collection_upload_tag_frontier_digest` | `CONSTRAINT ck_collection_upload_tag_frontier_digest CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-ccbacf0434"></a>`check` | `ck_collection_upload_tag_frontier_receipt` | `CONSTRAINT ck_collection_upload_tag_frontier_receipt CHECK (published = false AND object_path IS NULL AND stored_bytes IS NULL AND stored_sha256 IS NULL AND published_at IS NULL OR published = true AND object_path IS NOT NULL AND stored_bytes > 0 AND stored_sha256 IS NOT NULL AND published_at IS NOT NULL)` |
| <a id="s-6fd47b72c8"></a>`check` | `ck_sha256_a9a6767f54e4c1ef` | `CONSTRAINT ck_sha256_a9a6767f54e4c1ef CHECK (length(node_digest) = 64 AND lower(node_digest) = node_digest AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(node_digest, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-8c8789c65a"></a>`check` | `ck_sha256_6acac9b414fb5e15` | `CONSTRAINT ck_sha256_6acac9b414fb5e15 CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-dd84f652bb"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/31`

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
