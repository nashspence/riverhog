# riverhog-catalog: collection_mutable_document_reclamations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-mutable-docum-e81a9d1020:277208a4f3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-d8e2cc9a0e"></a>

### Table: `collection_mutable_document_reclamations`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-5d77180643"></a>`receipt_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-bd66413ccc"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-e8bc9bc109"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-7a336bd933"></a>`document_kind` | `VARCHAR` | no | `—` | — |
| <a id="s-99a03b594f"></a>`object_path` | `VARCHAR` | no | `—` | — |
| <a id="s-9bff7e14d5"></a>`provider_revision` | `VARCHAR` | no | `—` | — |
| <a id="s-7706fa3f74"></a>`stored_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-cb4f0241c3"></a>`stored_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-32d503c792"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-b94d0e4940"></a>`next_attempt_at` | `VARCHAR` | no | `—` | — |
| <a id="s-21ddb59a23"></a>`failure` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-8d9384f18c"></a>`primary-key` | `—` | `PRIMARY KEY (receipt_identity)` |
| <a id="s-3159d74f9c"></a>`check` | `ck_mutable_document_reclamations_identity` | `CONSTRAINT ck_mutable_document_reclamations_identity CHECK (length(receipt_identity) = 64 AND lower(receipt_identity) = receipt_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(receipt_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-f9f80d0430"></a>`check` | `ck_mutable_document_reclamations_kind` | `CONSTRAINT ck_mutable_document_reclamations_kind CHECK (document_kind IN ('description','tag_head'))` |
| <a id="s-545d9c713f"></a>`check` | `ck_mutable_document_reclamations_bytes` | `CONSTRAINT ck_mutable_document_reclamations_bytes CHECK (stored_bytes > 0)` |
| <a id="s-6d38c2a732"></a>`check` | `ck_mutable_document_reclamations_stored_sha256` | `CONSTRAINT ck_mutable_document_reclamations_stored_sha256 CHECK (length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-dc1e3b2e65"></a>`check` | `ck_mutable_document_reclamations_state` | `CONSTRAINT ck_mutable_document_reclamations_state CHECK (state IN ('pending','deleting','retry_wait'))` |
| <a id="s-1e470c0bbe"></a>`check` | `ck_sha256_22e6e1bdfada4730` | `CONSTRAINT ck_sha256_22e6e1bdfada4730 CHECK (length(receipt_identity) = 64 AND lower(receipt_identity) = receipt_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(receipt_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-7c88cdbf32"></a>`check` | `ck_sha256_c2ced24a99b9db78` | `CONSTRAINT ck_sha256_c2ced24a99b9db78 CHECK (length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-77ed941070"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/59`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f8516e8ea9a06d25a7a026a1078ff0e492b31bbda3c78a2adf946f14fd1bb3c4 -->

```json
{
  "columns": [
    {
      "definition": "receipt_identity VARCHAR(64) NOT NULL",
      "name": "receipt_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "collection_id BIGINT NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "store VARCHAR NOT NULL",
      "name": "store",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "document_kind VARCHAR NOT NULL",
      "name": "document_kind",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "object_path VARCHAR NOT NULL",
      "name": "object_path",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "provider_revision VARCHAR NOT NULL",
      "name": "provider_revision",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "stored_bytes BIGINT NOT NULL",
      "name": "stored_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "stored_sha256 VARCHAR(64) NOT NULL",
      "name": "stored_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "next_attempt_at VARCHAR NOT NULL",
      "name": "next_attempt_at",
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
        "receipt_identity"
      ],
      "definition": "PRIMARY KEY (receipt_identity)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_mutable_document_reclamations_identity CHECK (length(receipt_identity) = 64 AND lower(receipt_identity) = receipt_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(receipt_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(receipt_identity) = 64 AND lower(receipt_identity) = receipt_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(receipt_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_mutable_document_reclamations_identity"
    },
    {
      "definition": "CONSTRAINT ck_mutable_document_reclamations_kind CHECK (document_kind IN ('description','tag_head'))",
      "expression": "(document_kind IN ('description','tag_head'))",
      "kind": "check",
      "name": "ck_mutable_document_reclamations_kind"
    },
    {
      "definition": "CONSTRAINT ck_mutable_document_reclamations_bytes CHECK (stored_bytes > 0)",
      "expression": "(stored_bytes > 0)",
      "kind": "check",
      "name": "ck_mutable_document_reclamations_bytes"
    },
    {
      "definition": "CONSTRAINT ck_mutable_document_reclamations_stored_sha256 CHECK (length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_mutable_document_reclamations_stored_sha256"
    },
    {
      "definition": "CONSTRAINT ck_mutable_document_reclamations_state CHECK (state IN ('pending','deleting','retry_wait'))",
      "expression": "(state IN ('pending','deleting','retry_wait'))",
      "kind": "check",
      "name": "ck_mutable_document_reclamations_state"
    },
    {
      "definition": "CONSTRAINT ck_sha256_22e6e1bdfada4730 CHECK (length(receipt_identity) = 64 AND lower(receipt_identity) = receipt_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(receipt_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(receipt_identity) = 64 AND lower(receipt_identity) = receipt_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(receipt_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_22e6e1bdfada4730"
    },
    {
      "definition": "CONSTRAINT ck_sha256_c2ced24a99b9db78 CHECK (length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_c2ced24a99b9db78"
    }
  ],
  "name": "collection_mutable_document_reclamations"
}
```

</details>
