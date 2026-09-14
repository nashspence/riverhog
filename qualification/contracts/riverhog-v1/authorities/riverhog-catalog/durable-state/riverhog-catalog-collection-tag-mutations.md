# riverhog-catalog: collection_tag_mutations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-mutations:7b625dee0d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-f8df4c117f"></a>
- Table: `collection_tag_mutations`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-9932270c54"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-2e7b6ebbb8"></a>`operation_id` | `VARCHAR` | no | `—` | — |
| <a id="s-c55acc3fd7"></a>`action` | `VARCHAR` | no | `—` | — |
| <a id="s-0e44175f25"></a>`tag` | `TEXT` | no | `—` | — |
| <a id="s-a1fe7085a7"></a>`tag_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-63c188e927"></a>`expected_revision` | `BIGINT` | no | `—` | — |
| <a id="s-2d26de7851"></a>`expected_tag_set_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-8c80809714"></a>`result_revision` | `BIGINT` | no | `—` | — |
| <a id="s-fdda0d2250"></a>`result_root_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-ebe848d365"></a>`result_tag_set_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-c147718322"></a>`result_head_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-94f64413f2"></a>`changed` | `BOOLEAN` | no | `—` | — |
| <a id="s-9fadb41d15"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-8b6f9442cd"></a>`initiated_by_app` | `VARCHAR` | no | `—` | — |
| <a id="s-daf67e277a"></a>`initiated_by_key_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-6c5dd0c6de"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-2248802671"></a>`updated_at` | `VARCHAR` | no | `—` | — |
| <a id="s-51edcc7b4b"></a>`failure` | `TEXT` | yes | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-823d9f3cd6"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, operation_id)` |
| <a id="s-e8b482321b"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE` |
| <a id="s-a782980d78"></a>`check` | `ck_collection_tag_mutations_action` | `CONSTRAINT ck_collection_tag_mutations_action CHECK (action IN ('add','remove'))` |
| <a id="s-af6b431813"></a>`check` | `ck_collection_tag_mutations_state` | `CONSTRAINT ck_collection_tag_mutations_state CHECK (state IN ('pending','retry_wait','succeeded'))` |
| <a id="s-2df6940e8c"></a>`check` | `ck_collection_tag_mutations_tag_bytes` | `CONSTRAINT ck_collection_tag_mutations_tag_bytes CHECK (octet_length(tag) > 0 AND octet_length(tag) <= 65536)` |
| <a id="s-b52d0279a2"></a>`check` | `ck_collection_tag_mutations_expected_revision` | `CONSTRAINT ck_collection_tag_mutations_expected_revision CHECK (expected_revision >= 1 AND expected_revision < 9007199254740991)` |
| <a id="s-742ea126ee"></a>`check` | `ck_collection_tag_mutations_result_revision` | `CONSTRAINT ck_collection_tag_mutations_result_revision CHECK (changed AND result_revision = expected_revision + 1 OR NOT changed AND result_revision = expected_revision)` |
| <a id="s-2181ee01c0"></a>`check` | `ck_collection_tag_mutations_tag_sha256` | `CONSTRAINT ck_collection_tag_mutations_tag_sha256 CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-2793e05e58"></a>`check` | `ck_collection_tag_mutations_expected_identity` | `CONSTRAINT ck_collection_tag_mutations_expected_identity CHECK (length(expected_tag_set_identity) = 64 AND lower(expected_tag_set_identity) = expected_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-eb0738484a"></a>`check` | `ck_collection_tag_mutations_result_identity` | `CONSTRAINT ck_collection_tag_mutations_result_identity CHECK (length(result_tag_set_identity) = 64 AND lower(result_tag_set_identity) = result_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-b9bc82136c"></a>`check` | `ck_collection_tag_mutations_head_identity` | `CONSTRAINT ck_collection_tag_mutations_head_identity CHECK (length(result_head_identity) = 64 AND lower(result_head_identity) = result_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-fa8f5ceda6"></a>`check` | `ck_collection_tag_mutations_tag_sha256_hex` | `CONSTRAINT ck_collection_tag_mutations_tag_sha256_hex CHECK (length(tag_sha256) = 64 AND lower(tag_sha256) = tag_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-ff1bb5c8d1"></a>`check` | `ck_collection_tag_mutations_expected_tag_set_identity_hex` | `CONSTRAINT ck_collection_tag_mutations_expected_tag_set_identity_hex CHECK (length(expected_tag_set_identity) = 64 AND lower(expected_tag_set_identity) = expected_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(expected_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-611aeab5af"></a>`check` | `ck_collection_tag_mutations_result_root_sha256_hex` | `CONSTRAINT ck_collection_tag_mutations_result_root_sha256_hex CHECK (result_root_sha256 IS NULL OR length(result_root_sha256) = 64 AND lower(result_root_sha256) = result_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-e9d984bef8"></a>`check` | `ck_collection_tag_mutations_result_tag_set_identity_hex` | `CONSTRAINT ck_collection_tag_mutations_result_tag_set_identity_hex CHECK (length(result_tag_set_identity) = 64 AND lower(result_tag_set_identity) = result_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-544d9f5e68"></a>`check` | `ck_collection_tag_mutations_result_head_identity_hex` | `CONSTRAINT ck_collection_tag_mutations_result_head_identity_hex CHECK (length(result_head_identity) = 64 AND lower(result_head_identity) = result_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(result_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [riverhog-catalog durable-state identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-3f87d118c7"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/25`

### Exact owned JSON

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
