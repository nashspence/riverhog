# riverhog-catalog: collection_tag_mutations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-mutations:218f686b4e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-fde380e33e"></a>

### Table: `collection_tag_mutations`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-11f8ed6d88"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-4aab0b8f00"></a>`operation_id` | `VARCHAR` | no | `—` | — |
| <a id="s-f4491a8309"></a>`action` | `VARCHAR` | no | `—` | — |
| <a id="s-7aff0d860b"></a>`tag` | `TEXT` | no | `—` | — |
| <a id="s-988ad3697c"></a>`tag_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-78952758ca"></a>`expected_revision` | `BIGINT` | no | `—` | — |
| <a id="s-d50b4e617c"></a>`expected_tag_set_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-18547b6b81"></a>`result_revision` | `BIGINT` | no | `—` | — |
| <a id="s-462fd16148"></a>`result_root_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-b3b2d69c94"></a>`result_tag_set_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-1213cf50f7"></a>`result_head_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-c027cb6c26"></a>`changed` | `BOOLEAN` | no | `—` | — |
| <a id="s-0450faac09"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-2bc7fa07d4"></a>`initiated_by_app` | `VARCHAR` | no | `—` | — |
| <a id="s-5aecf275e8"></a>`initiated_by_key_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-0f0ea57020"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-cd1386738d"></a>`updated_at` | `VARCHAR` | no | `—` | — |
| <a id="s-0fd4ec6b4d"></a>`failure` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-827632f5b7"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, operation_id)` |
| <a id="s-590de972d1"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE` |
| <a id="s-3b41812152"></a>`check` | `ck_collection_tag_mutations_action` | `CONSTRAINT ck_collection_tag_mutations_action CHECK (action IN ('add','remove'))` |
| <a id="s-94ae3aef5d"></a>`check` | `ck_collection_tag_mutations_state` | `CONSTRAINT ck_collection_tag_mutations_state CHECK (state IN ('pending','retry_wait','succeeded'))` |
| <a id="s-454beba88c"></a>`check` | `ck_collection_tag_mutations_tag_bytes` | `CONSTRAINT ck_collection_tag_mutations_tag_bytes CHECK (octet_length(tag) > 0 AND octet_length(tag) <= 65536)` |
| <a id="s-a3c3896a90"></a>`check` | `ck_collection_tag_mutations_expected_revision` | `CONSTRAINT ck_collection_tag_mutations_expected_revision CHECK (expected_revision >= 1 AND expected_revision < 9007199254740991)` |
| <a id="s-a9e3eb6dba"></a>`check` | `ck_collection_tag_mutations_result_revision` | `CONSTRAINT ck_collection_tag_mutations_result_revision CHECK (changed AND result_revision = expected_revision + 1 OR NOT changed AND result_revision = expected_revision)` |
| <a id="s-d3c4a722f2"></a>`check` | `ck_collection_tag_mutations_tag_sha256` | `CONSTRAINT ck_collection_tag_mutations_tag_sha256 CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-3ad132abad"></a>`check` | `ck_collection_tag_mutations_expected_identity` | `CONSTRAINT ck_collection_tag_mutations_expected_identity CHECK (length(expected_tag_set_identity) = 64 AND lower(expected_tag_set_identity) = expected_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-5ef7203b44"></a>`check` | `ck_collection_tag_mutations_result_identity` | `CONSTRAINT ck_collection_tag_mutations_result_identity CHECK (length(result_tag_set_identity) = 64 AND lower(result_tag_set_identity) = result_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-20bfe4bded"></a>`check` | `ck_collection_tag_mutations_head_identity` | `CONSTRAINT ck_collection_tag_mutations_head_identity CHECK (length(result_head_identity) = 64 AND lower(result_head_identity) = result_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-8a5accf1c7"></a>`check` | `ck_collection_tag_mutations_tag_sha256_hex` | `CONSTRAINT ck_collection_tag_mutations_tag_sha256_hex CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-9942e6d183"></a>`check` | `ck_collection_tag_mutations_expected_tag_set_identity_hex` | `CONSTRAINT ck_collection_tag_mutations_expected_tag_set_identity_hex CHECK (length(expected_tag_set_identity) = 64 AND lower(expected_tag_set_identity) = expected_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-1c369b042d"></a>`check` | `ck_collection_tag_mutations_result_root_sha256_hex` | `CONSTRAINT ck_collection_tag_mutations_result_root_sha256_hex CHECK (result_root_sha256 IS NULL OR length(result_root_sha256) = 64 AND lower(result_root_sha256) = result_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-bd21edd385"></a>`check` | `ck_collection_tag_mutations_result_tag_set_identity_hex` | `CONSTRAINT ck_collection_tag_mutations_result_tag_set_identity_hex CHECK (length(result_tag_set_identity) = 64 AND lower(result_tag_set_identity) = result_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-b6f2378c5e"></a>`check` | `ck_collection_tag_mutations_result_head_identity_hex` | `CONSTRAINT ck_collection_tag_mutations_result_head_identity_hex CHECK (length(result_head_identity) = 64 AND lower(result_head_identity) = result_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-7d555378c5"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/27`

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
