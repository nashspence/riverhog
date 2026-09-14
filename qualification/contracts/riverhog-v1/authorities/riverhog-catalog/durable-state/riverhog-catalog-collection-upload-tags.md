# riverhog-catalog: collection_upload_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-tags:acd1a6e2ac -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-f06902722f"></a>
- Table: `collection_upload_tags`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-e042748c52"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-7908bd2ac1"></a>`tag_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-cc950e582e"></a>`tag` | `TEXT` | no | `—` | — |
| <a id="s-aac24cd421"></a>`added_at` | `VARCHAR` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-e6fdbc79b0"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, tag_sha256)` |
| <a id="s-7f3de5ebe4"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_uploads (collection_id) ON DELETE CASCADE` |
| <a id="s-ff8105ac0b"></a>`check` | `ck_collection_upload_tags_bytes` | `CONSTRAINT ck_collection_upload_tags_bytes CHECK (octet_length(tag) > 0 AND octet_length(tag) <= 65536)` |
| <a id="s-e6d6330067"></a>`check` | `ck_collection_upload_tags_sha256` | `CONSTRAINT ck_collection_upload_tags_sha256 CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-b17d650acc"></a>`unique` | `uq_collection_upload_tags_value` | `CONSTRAINT uq_collection_upload_tags_value UNIQUE (collection_id, tag)` |
| <a id="s-23de958f64"></a>`check` | `ck_collection_upload_tags_tag_sha256_hex` | `CONSTRAINT ck_collection_upload_tags_tag_sha256_hex CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-974531ca57"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/33`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 933c8a8e63644fc564f7f25ac1417d80dd876a4af087905c9d0d11ef13ac6113 -->

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
      "definition": "tag_sha256 VARCHAR(64) NOT NULL",
      "name": "tag_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "tag TEXT NOT NULL",
      "name": "tag",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "added_at VARCHAR NOT NULL",
      "name": "added_at",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "tag_sha256"
      ],
      "definition": "PRIMARY KEY (collection_id, tag_sha256)",
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
      "definition": "CONSTRAINT ck_collection_upload_tags_bytes CHECK (octet_length(tag) > 0 AND octet_length(tag) <= 65536)",
      "expression": "(octet_length(tag) > 0 AND octet_length(tag) <= 65536)",
      "kind": "check",
      "name": "ck_collection_upload_tags_bytes"
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_tags_sha256 CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_upload_tags_sha256"
    },
    {
      "columns": [
        "collection_id",
        "tag"
      ],
      "definition": "CONSTRAINT uq_collection_upload_tags_value UNIQUE (collection_id, tag)",
      "kind": "unique",
      "name": "uq_collection_upload_tags_value"
    },
    {
      "definition": "CONSTRAINT ck_collection_upload_tags_tag_sha256_hex CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_upload_tags_tag_sha256_hex"
    }
  ],
  "name": "collection_upload_tags"
}
```
