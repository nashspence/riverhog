# riverhog-catalog: collection_tag_revisions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-revisions:59548e3b55 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-5c534a7cd1"></a>

### Table: `collection_tag_revisions`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-4e8db8a2bd"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-ee0619fdfd"></a>`revision` | `BIGINT` | no | `—` | — |
| <a id="s-cf60d564c7"></a>`root_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-cadad7863c"></a>`tag_set_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-728dded389"></a>`head_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-264662b57d"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-d576685a7a"></a>`cleanup_started_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-ed3b17ea02"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, revision)` |
| <a id="s-f653cbd2a0"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE` |
| <a id="s-0e2db9fec1"></a>`check` | `ck_collection_tag_revisions_revision` | `CONSTRAINT ck_collection_tag_revisions_revision CHECK (revision >= 1 AND revision <= 9007199254740991)` |
| <a id="s-2f9d7ae2a2"></a>`check` | `ck_collection_tag_revisions_root` | `CONSTRAINT ck_collection_tag_revisions_root CHECK (root_sha256 IS NULL OR length(root_sha256) = 64 AND lower(root_sha256) = root_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-35502dfd43"></a>`check` | `ck_collection_tag_revisions_set_identity` | `CONSTRAINT ck_collection_tag_revisions_set_identity CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-0992175ad1"></a>`check` | `ck_collection_tag_revisions_head_identity` | `CONSTRAINT ck_collection_tag_revisions_head_identity CHECK (length(head_identity) = 64 AND lower(head_identity) = head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-789a57e5c1"></a>`check` | `ck_collection_tag_revisions_root_sha256_hex` | `CONSTRAINT ck_collection_tag_revisions_root_sha256_hex CHECK (root_sha256 IS NULL OR length(root_sha256) = 64 AND lower(root_sha256) = root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-ce83ac2f2f"></a>`check` | `ck_collection_tag_revisions_tag_set_identity_hex` | `CONSTRAINT ck_collection_tag_revisions_tag_set_identity_hex CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-df4c771bbc"></a>`check` | `ck_collection_tag_revisions_head_identity_hex` | `CONSTRAINT ck_collection_tag_revisions_head_identity_hex CHECK (length(head_identity) = 64 AND lower(head_identity) = head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-5766cef978"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

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
