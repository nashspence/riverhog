# riverhog-catalog: collection_tag_revisions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-revisions:be1d6570d0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-fde380e33e"></a>

### Table: `collection_tag_revisions`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-11f8ed6d88"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-4aab0b8f00"></a>`revision` | `BIGINT` | no | `—` | — |
| <a id="s-f4491a8309"></a>`root_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-7aff0d860b"></a>`tag_set_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-988ad3697c"></a>`head_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-78952758ca"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-d50b4e617c"></a>`cleanup_started_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-827632f5b7"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, revision)` |
| <a id="s-590de972d1"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE` |
| <a id="s-3b41812152"></a>`check` | `ck_collection_tag_revisions_revision` | `CONSTRAINT ck_collection_tag_revisions_revision CHECK (revision >= 1 AND revision <= 9007199254740991)` |
| <a id="s-94ae3aef5d"></a>`check` | `ck_collection_tag_revisions_root` | `CONSTRAINT ck_collection_tag_revisions_root CHECK (root_sha256 IS NULL OR length(root_sha256) = 64 AND lower(root_sha256) = root_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-454beba88c"></a>`check` | `ck_collection_tag_revisions_set_identity` | `CONSTRAINT ck_collection_tag_revisions_set_identity CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-a3c3896a90"></a>`check` | `ck_collection_tag_revisions_head_identity` | `CONSTRAINT ck_collection_tag_revisions_head_identity CHECK (length(head_identity) = 64 AND lower(head_identity) = head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-a9e3eb6dba"></a>`check` | `ck_collection_tag_revisions_root_sha256_hex` | `CONSTRAINT ck_collection_tag_revisions_root_sha256_hex CHECK (root_sha256 IS NULL OR length(root_sha256) = 64 AND lower(root_sha256) = root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-d3c4a722f2"></a>`check` | `ck_collection_tag_revisions_tag_set_identity_hex` | `CONSTRAINT ck_collection_tag_revisions_tag_set_identity_hex CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-3ad132abad"></a>`check` | `ck_collection_tag_revisions_head_identity_hex` | `CONSTRAINT ck_collection_tag_revisions_head_identity_hex CHECK (length(head_identity) = 64 AND lower(head_identity) = head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-383ee92707"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/27`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dd170ab463c8ede33391a8d02695ece9cf1d284fee7156e9a4fa8dfc2ddb90b5 -->

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
      "definition": "revision BIGINT NOT NULL",
      "name": "revision",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "root_sha256 VARCHAR(64)",
      "name": "root_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "tag_set_identity VARCHAR(64) NOT NULL",
      "name": "tag_set_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "head_identity VARCHAR(64) NOT NULL",
      "name": "head_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "created_at VARCHAR NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "cleanup_started_at VARCHAR",
      "name": "cleanup_started_at",
      "nullable": true,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "revision"
      ],
      "definition": "PRIMARY KEY (collection_id, revision)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id"
      ],
      "definition": "FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "id"
        ],
        "table": "collections"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_revisions_revision CHECK (revision >= 1 AND revision <= 9007199254740991)",
      "expression": "(revision >= 1 AND revision <= 9007199254740991)",
      "kind": "check",
      "name": "ck_collection_tag_revisions_revision"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_revisions_root CHECK (root_sha256 IS NULL OR length(root_sha256) = 64 AND lower(root_sha256) = root_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(root_sha256 IS NULL OR length(root_sha256) = 64 AND lower(root_sha256) = root_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_revisions_root"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_revisions_set_identity CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_revisions_set_identity"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_revisions_head_identity CHECK (length(head_identity) = 64 AND lower(head_identity) = head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(head_identity) = 64 AND lower(head_identity) = head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_revisions_head_identity"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_revisions_root_sha256_hex CHECK (root_sha256 IS NULL OR length(root_sha256) = 64 AND lower(root_sha256) = root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(root_sha256 IS NULL OR length(root_sha256) = 64 AND lower(root_sha256) = root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_revisions_root_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_revisions_tag_set_identity_hex CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_revisions_tag_set_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_revisions_head_identity_hex CHECK (length(head_identity) = 64 AND lower(head_identity) = head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(head_identity) = 64 AND lower(head_identity) = head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_revisions_head_identity_hex"
    }
  ],
  "name": "collection_tag_revisions"
}
```

</details>
