# riverhog-catalog: collection_processing_outcomes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-processing-outcomes:68acc06e8b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-752449f3f7"></a>

### Table: `collection_processing_outcomes`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1844fea9ca"></a>`claim_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-a25c903146"></a>`outcome_id` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-7deb583524"></a>`source_claim_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-d9e917b22f"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-6163934f46"></a>`archive_root_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-0cfbc54f10"></a>`content_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-bc4326af82"></a>`derivation_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-b520f1dc18"></a>`outcome_order` | `BIGINT` | yes | `—` | — |
| <a id="s-849032c210"></a>`created_at` | `VARCHAR` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-b5b1e0ce88"></a>`primary-key` | `—` | `PRIMARY KEY (claim_id, outcome_id)` |
| <a id="s-1b54efaa44"></a>`unique` | `uq_collection_processing_outcomes_source_claim` | `CONSTRAINT uq_collection_processing_outcomes_source_claim UNIQUE (claim_id, source_claim_id)` |
| <a id="s-e4f433b235"></a>`unique` | `uq_collection_processing_outcomes_output` | `CONSTRAINT uq_collection_processing_outcomes_output UNIQUE (claim_id, collection_id)` |
| <a id="s-898f22405f"></a>`check` | `ck_collection_processing_outcomes_order` | `CONSTRAINT ck_collection_processing_outcomes_order CHECK (outcome_order IS NULL OR outcome_order >= 0)` |
| <a id="s-ffc342a574"></a>`foreign-key` | `—` | `FOREIGN KEY(claim_id) REFERENCES collection_processing_claims (id) ON DELETE CASCADE` |
| <a id="s-92f05227ce"></a>`foreign-key` | `—` | `FOREIGN KEY(source_claim_id) REFERENCES collection_processing_claims (id) ON DELETE RESTRICT` |
| <a id="s-058438dd05"></a>`check` | `ck_collection_processing_outcomes_claim_id_hex` | `CONSTRAINT ck_collection_processing_outcomes_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-99e9c3861e"></a>`check` | `ck_collection_processing_outcomes_source_claim_id_hex` | `CONSTRAINT ck_collection_processing_outcomes_source_claim_id_hex CHECK (length(source_claim_id) = 64 AND lower(source_claim_id) = source_claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-05ef151826"></a>`check` | `ck_collection_processing_outcomes_archive_root_sha256_hex` | `CONSTRAINT ck_collection_processing_outcomes_archive_root_sha256_hex CHECK (length(archive_root_sha256) = 64 AND lower(archive_root_sha256) = archive_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-d707c30d4b"></a>`check` | `ck_collection_processing_outcomes_content_identity_hex` | `CONSTRAINT ck_collection_processing_outcomes_content_identity_hex CHECK (length(content_identity) = 64 AND lower(content_identity) = content_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(content_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-a188f255ad"></a>`check` | `ck_collection_processing_outcomes_derivation_sha256_hex` | `CONSTRAINT ck_collection_processing_outcomes_derivation_sha256_hex CHECK (length(derivation_sha256) = 64 AND lower(derivation_sha256) = derivation_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(derivation_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-7628c2e295"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/48`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bcb1ea5c4d1fb5da3d25a4877f1287f31aad331566d28fa67a6d659022080577 -->

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
      "definition": "outcome_id VARCHAR(160) NOT NULL",
      "name": "outcome_id",
      "nullable": false,
      "type": "VARCHAR(160)"
    },
    {
      "definition": "source_claim_id VARCHAR(64) NOT NULL",
      "name": "source_claim_id",
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
    },
    {
      "definition": "derivation_sha256 VARCHAR(64) NOT NULL",
      "name": "derivation_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "outcome_order BIGINT",
      "name": "outcome_order",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "created_at VARCHAR NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "claim_id",
        "outcome_id"
      ],
      "definition": "PRIMARY KEY (claim_id, outcome_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "claim_id",
        "source_claim_id"
      ],
      "definition": "CONSTRAINT uq_collection_processing_outcomes_source_claim UNIQUE (claim_id, source_claim_id)",
      "kind": "unique",
      "name": "uq_collection_processing_outcomes_source_claim"
    },
    {
      "columns": [
        "claim_id",
        "collection_id"
      ],
      "definition": "CONSTRAINT uq_collection_processing_outcomes_output UNIQUE (claim_id, collection_id)",
      "kind": "unique",
      "name": "uq_collection_processing_outcomes_output"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_outcomes_order CHECK (outcome_order IS NULL OR outcome_order >= 0)",
      "expression": "(outcome_order IS NULL OR outcome_order >= 0)",
      "kind": "check",
      "name": "ck_collection_processing_outcomes_order"
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
      "columns": [
        "source_claim_id"
      ],
      "definition": "FOREIGN KEY(source_claim_id) REFERENCES collection_processing_claims (id) ON DELETE RESTRICT",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE RESTRICT",
        "columns": [
          "id"
        ],
        "table": "collection_processing_claims"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_outcomes_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_outcomes_claim_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_outcomes_source_claim_id_hex CHECK (length(source_claim_id) = 64 AND lower(source_claim_id) = source_claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(source_claim_id) = 64 AND lower(source_claim_id) = source_claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_outcomes_source_claim_id_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_outcomes_archive_root_sha256_hex CHECK (length(archive_root_sha256) = 64 AND lower(archive_root_sha256) = archive_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(archive_root_sha256) = 64 AND lower(archive_root_sha256) = archive_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_outcomes_archive_root_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_outcomes_content_identity_hex CHECK (length(content_identity) = 64 AND lower(content_identity) = content_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(content_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(content_identity) = 64 AND lower(content_identity) = content_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(content_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_outcomes_content_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_processing_outcomes_derivation_sha256_hex CHECK (length(derivation_sha256) = 64 AND lower(derivation_sha256) = derivation_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(derivation_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(derivation_sha256) = 64 AND lower(derivation_sha256) = derivation_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(derivation_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_processing_outcomes_derivation_sha256_hex"
    }
  ],
  "name": "collection_processing_outcomes"
}
```

</details>
