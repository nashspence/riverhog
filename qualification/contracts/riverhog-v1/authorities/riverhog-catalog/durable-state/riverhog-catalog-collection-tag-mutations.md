# riverhog-catalog: collection_tag_mutations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-mutations:15f9485e04 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-458329dbd9"></a>

### Table: `collection_tag_mutations`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-7b6d8af07d"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-fc9a3dce28"></a>`operation_id` | `VARCHAR` | no | `—` | — |
| <a id="s-6c19550b0f"></a>`action` | `VARCHAR` | no | `—` | — |
| <a id="s-80df7fa5c0"></a>`tag` | `TEXT` | no | `—` | — |
| <a id="s-22d7c7a8c1"></a>`tag_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-3f1cb21773"></a>`expected_revision` | `BIGINT` | no | `—` | — |
| <a id="s-9c47c29160"></a>`expected_tag_set_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-ff0c939bec"></a>`result_revision` | `BIGINT` | no | `—` | — |
| <a id="s-836e80e56f"></a>`result_root_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-0da512b71d"></a>`result_tag_set_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-17e39084a4"></a>`result_head_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-7cdb07c69a"></a>`changed` | `BOOLEAN` | no | `—` | — |
| <a id="s-f3a735e98b"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-239a391689"></a>`initiated_by_app` | `VARCHAR` | no | `—` | — |
| <a id="s-4ceccbf775"></a>`initiated_by_key_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-6c7a153199"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-7a4f76107e"></a>`updated_at` | `VARCHAR` | no | `—` | — |
| <a id="s-49406c8440"></a>`failure` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-18c8dd01ce"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, operation_id)` |
| <a id="s-e845e376ac"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE` |
| <a id="s-e6c64907d1"></a>`check` | `ck_collection_tag_mutations_action` | `CONSTRAINT ck_collection_tag_mutations_action CHECK (action IN ('add','remove'))` |
| <a id="s-41c7ffa546"></a>`check` | `ck_collection_tag_mutations_state` | `CONSTRAINT ck_collection_tag_mutations_state CHECK (state IN ('pending','retry_wait','succeeded'))` |
| <a id="s-1de05260ae"></a>`check` | `ck_collection_tag_mutations_tag_bytes` | `CONSTRAINT ck_collection_tag_mutations_tag_bytes CHECK (octet_length(tag) > 0 AND octet_length(tag) <= 65536)` |
| <a id="s-cfe2e87f84"></a>`check` | `ck_collection_tag_mutations_expected_revision` | `CONSTRAINT ck_collection_tag_mutations_expected_revision CHECK (expected_revision >= 1 AND expected_revision < 9007199254740991)` |
| <a id="s-5a3384acd9"></a>`check` | `ck_collection_tag_mutations_result_revision` | `CONSTRAINT ck_collection_tag_mutations_result_revision CHECK (changed AND result_revision = expected_revision + 1 OR NOT changed AND result_revision = expected_revision)` |
| <a id="s-a6335d4960"></a>`check` | `ck_collection_tag_mutations_tag_sha256` | `CONSTRAINT ck_collection_tag_mutations_tag_sha256 CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-d416e216f3"></a>`check` | `ck_collection_tag_mutations_expected_identity` | `CONSTRAINT ck_collection_tag_mutations_expected_identity CHECK (length(expected_tag_set_identity) = 64 AND lower(expected_tag_set_identity) = expected_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-adfa1fd787"></a>`check` | `ck_collection_tag_mutations_result_identity` | `CONSTRAINT ck_collection_tag_mutations_result_identity CHECK (length(result_tag_set_identity) = 64 AND lower(result_tag_set_identity) = result_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-ed6e635d30"></a>`check` | `ck_collection_tag_mutations_head_identity` | `CONSTRAINT ck_collection_tag_mutations_head_identity CHECK (length(result_head_identity) = 64 AND lower(result_head_identity) = result_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-27e5601b29"></a>`check` | `ck_collection_tag_mutations_tag_sha256_hex` | `CONSTRAINT ck_collection_tag_mutations_tag_sha256_hex CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-03bc2f22d0"></a>`check` | `ck_collection_tag_mutations_expected_tag_set_identity_hex` | `CONSTRAINT ck_collection_tag_mutations_expected_tag_set_identity_hex CHECK (length(expected_tag_set_identity) = 64 AND lower(expected_tag_set_identity) = expected_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-d93c042db5"></a>`check` | `ck_collection_tag_mutations_result_root_sha256_hex` | `CONSTRAINT ck_collection_tag_mutations_result_root_sha256_hex CHECK (result_root_sha256 IS NULL OR length(result_root_sha256) = 64 AND lower(result_root_sha256) = result_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-d3375bde9e"></a>`check` | `ck_collection_tag_mutations_result_tag_set_identity_hex` | `CONSTRAINT ck_collection_tag_mutations_result_tag_set_identity_hex CHECK (length(result_tag_set_identity) = 64 AND lower(result_tag_set_identity) = result_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-3b4cbd6058"></a>`check` | `ck_collection_tag_mutations_result_head_identity_hex` | `CONSTRAINT ck_collection_tag_mutations_result_head_identity_hex CHECK (length(result_head_identity) = 64 AND lower(result_head_identity) = result_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-72244c953d"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/26`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f5cd07d5cc94f830dddf61beeeb33dbb63ae34dcfed50c3439d4d84877fb8dd -->

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
      "definition": "action VARCHAR NOT NULL",
      "name": "action",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "tag TEXT NOT NULL",
      "name": "tag",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "tag_sha256 VARCHAR(64) NOT NULL",
      "name": "tag_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "expected_revision BIGINT NOT NULL",
      "name": "expected_revision",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "expected_tag_set_identity VARCHAR(64) NOT NULL",
      "name": "expected_tag_set_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "result_revision BIGINT NOT NULL",
      "name": "result_revision",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "result_root_sha256 VARCHAR(64)",
      "name": "result_root_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "result_tag_set_identity VARCHAR(64) NOT NULL",
      "name": "result_tag_set_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "result_head_identity VARCHAR(64) NOT NULL",
      "name": "result_head_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "changed BOOLEAN NOT NULL",
      "name": "changed",
      "nullable": false,
      "type": "BOOLEAN"
    },
    {
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "initiated_by_app VARCHAR NOT NULL",
      "name": "initiated_by_app",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "initiated_by_key_id VARCHAR",
      "name": "initiated_by_key_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "created_at VARCHAR NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "updated_at VARCHAR NOT NULL",
      "name": "updated_at",
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
        "operation_id"
      ],
      "definition": "PRIMARY KEY (collection_id, operation_id)",
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
      "definition": "CONSTRAINT ck_collection_tag_mutations_action CHECK (action IN ('add','remove'))",
      "expression": "(action IN ('add','remove'))",
      "kind": "check",
      "name": "ck_collection_tag_mutations_action"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_mutations_state CHECK (state IN ('pending','retry_wait','succeeded'))",
      "expression": "(state IN ('pending','retry_wait','succeeded'))",
      "kind": "check",
      "name": "ck_collection_tag_mutations_state"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_mutations_tag_bytes CHECK (octet_length(tag) > 0 AND octet_length(tag) <= 65536)",
      "expression": "(octet_length(tag) > 0 AND octet_length(tag) <= 65536)",
      "kind": "check",
      "name": "ck_collection_tag_mutations_tag_bytes"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_mutations_expected_revision CHECK (expected_revision >= 1 AND expected_revision < 9007199254740991)",
      "expression": "(expected_revision >= 1 AND expected_revision < 9007199254740991)",
      "kind": "check",
      "name": "ck_collection_tag_mutations_expected_revision"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_mutations_result_revision CHECK (changed AND result_revision = expected_revision + 1 OR NOT changed AND result_revision = expected_revision)",
      "expression": "(changed AND result_revision = expected_revision + 1 OR NOT changed AND result_revision = expected_revision)",
      "kind": "check",
      "name": "ck_collection_tag_mutations_result_revision"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_mutations_tag_sha256 CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_mutations_tag_sha256"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_mutations_expected_identity CHECK (length(expected_tag_set_identity) = 64 AND lower(expected_tag_set_identity) = expected_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(expected_tag_set_identity) = 64 AND lower(expected_tag_set_identity) = expected_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_mutations_expected_identity"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_mutations_result_identity CHECK (length(result_tag_set_identity) = 64 AND lower(result_tag_set_identity) = result_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(result_tag_set_identity) = 64 AND lower(result_tag_set_identity) = result_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_mutations_result_identity"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_mutations_head_identity CHECK (length(result_head_identity) = 64 AND lower(result_head_identity) = result_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(result_head_identity) = 64 AND lower(result_head_identity) = result_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_mutations_head_identity"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_mutations_tag_sha256_hex CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_mutations_tag_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_mutations_expected_tag_set_identity_hex CHECK (length(expected_tag_set_identity) = 64 AND lower(expected_tag_set_identity) = expected_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(expected_tag_set_identity) = 64 AND lower(expected_tag_set_identity) = expected_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_mutations_expected_tag_set_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_mutations_result_root_sha256_hex CHECK (result_root_sha256 IS NULL OR length(result_root_sha256) = 64 AND lower(result_root_sha256) = result_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(result_root_sha256 IS NULL OR length(result_root_sha256) = 64 AND lower(result_root_sha256) = result_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_mutations_result_root_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_mutations_result_tag_set_identity_hex CHECK (length(result_tag_set_identity) = 64 AND lower(result_tag_set_identity) = result_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(result_tag_set_identity) = 64 AND lower(result_tag_set_identity) = result_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_mutations_result_tag_set_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_mutations_result_head_identity_hex CHECK (length(result_head_identity) = 64 AND lower(result_head_identity) = result_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(result_head_identity) = 64 AND lower(result_head_identity) = result_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_mutations_result_head_identity_hex"
    }
  ],
  "name": "collection_tag_mutations"
}
```

</details>
