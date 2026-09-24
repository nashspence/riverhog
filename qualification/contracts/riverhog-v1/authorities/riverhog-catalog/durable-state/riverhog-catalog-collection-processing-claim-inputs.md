# riverhog-catalog: collection_processing_claim_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-processing-claim-inputs:6e93d61cd9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-cf59823348"></a>

### Table: `collection_processing_claim_inputs`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-4fe8090c32"></a>`claim_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-0f291cd4e2"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-0f961b500a"></a>`collection_order` | `INTEGER` | no | `—` | — |
| <a id="s-23d2a36754"></a>`archive_root_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-c787b2f974"></a>`content_identity` | `VARCHAR(64)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-9927dc56fe"></a>`primary-key` | `—` | `PRIMARY KEY (claim_id, collection_id)` |
| <a id="s-850043dd24"></a>`unique` | `uq_collection_processing_claim_inputs_order` | `CONSTRAINT uq_collection_processing_claim_inputs_order UNIQUE (claim_id, collection_order)` |
| <a id="s-68e5038c1f"></a>`check` | `ck_processing_claim_inputs_order` | `CONSTRAINT ck_processing_claim_inputs_order CHECK (collection_order >= 0)` |
| <a id="s-d610feb878"></a>`check` | `ck_claim_inputs_archive_root` | `CONSTRAINT ck_claim_inputs_archive_root CHECK (length(archive_root_sha256) = 64)` |
| <a id="s-612721833c"></a>`check` | `ck_claim_inputs_content_identity` | `CONSTRAINT ck_claim_inputs_content_identity CHECK (length(content_identity) = 64)` |
| <a id="s-2c6a5f9de3"></a>`foreign-key` | `—` | `FOREIGN KEY(claim_id) REFERENCES collection_processing_claims (id) ON DELETE CASCADE` |
| <a id="s-82d4157845"></a>`check` | `ck_collection_processing_claim_inputs_claim_id_hex` | `CONSTRAINT ck_collection_processing_claim_inputs_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-68fb3472d8"></a>`check` | `ck_sha256_0bcbb66e83231f7f` | `CONSTRAINT ck_sha256_0bcbb66e83231f7f CHECK (length(archive_root_sha256) = 64 AND lower(archive_root_sha256) = archive_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-ea982db0dd"></a>`check` | `ck_collection_processing_claim_inputs_content_identity_hex` | `CONSTRAINT ck_collection_processing_claim_inputs_content_identity_hex CHECK (length(content_identity) = 64 AND lower(content_identity) = content_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(content_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-728cec5368"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/46`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 83da4473fe685b6da4db39bcacc30648ee374aa61482d8bed0b953505d974be6 -->

```json
{
  "columns": [
    {
      "definition": "claim_id VARCHAR(64) NOT NULL",
      "name": "claim_id",
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
      "definition": "collection_order INTEGER NOT NULL",
      "name": "collection_order",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "archive_root_sha256 VARCHAR(64) NOT NULL",
      "name": "archive_root_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "content_identity VARCHAR(64) NOT NULL",
      "name": "content_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "claim_id",
        "collection_id"
      ],
      "definition": "PRIMARY KEY (claim_id, collection_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "claim_id",
        "collection_order"
      ],
      "definition": "CONSTRAINT uq_collection_processing_claim_inputs_order UNIQUE (claim_id, collection_order)",
      "kind": "unique",
      "name": "uq_collection_processing_claim_inputs_order"
    },
    {
      "definition": "CONSTRAINT ck_processing_claim_inputs_order CHECK (collection_order >= 0)",
      "expression": "(collection_order >= 0)",
      "kind": "check",
      "name": "ck_processing_claim_inputs_order"
    },
    {
      "definition": "CONSTRAINT ck_claim_inputs_archive_root CHECK (length(archive_root_sha256) = 64)",
      "expression": "(length(archive_root_sha256) = 64)",
      "kind": "check",
      "name": "ck_claim_inputs_archive_root"
    },
    {
      "definition": "CONSTRAINT ck_claim_inputs_content_identity CHECK (length(content_identity) = 64)",
      "expression": "(length(content_identity) = 64)",
      "kind": "check",
      "name": "ck_claim_inputs_content_identity"
    },
    {
      "columns": [
        "claim_id"
      ],
      "definition": "FOREIGN KEY(claim_id) REFERENCES collection_processing_claims (id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "id"
        ],
        "table": "collection_processing_claims"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claim_inputs_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_claim_inputs_claim_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_sha256_0bcbb66e83231f7f CHECK (length(archive_root_sha256) = 64 AND lower(archive_root_sha256) = archive_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(archive_root_sha256) = 64 AND lower(archive_root_sha256) = archive_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_0bcbb66e83231f7f"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_claim_inputs_content_identity_hex CHECK (length(content_identity) = 64 AND lower(content_identity) = content_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(content_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(content_identity) = 64 AND lower(content_identity) = content_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(content_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_claim_inputs_content_identity_hex"
    }
  ],
  "name": "collection_processing_claim_inputs"
}
```

</details>
