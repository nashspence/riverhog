# riverhog-catalog: collection_processing_outcomes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-processing-outcomes:4fe89c3265 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-1d977b4003"></a>

### Table: `collection_processing_outcomes`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-d74300e3b0"></a>`claim_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-863018529b"></a>`outcome_id` | `VARCHAR(160)` | no | `—` | — |
| <a id="s-7527d8fd88"></a>`source_claim_id` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-bf9e18802a"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-c10a8e8fa3"></a>`archive_root_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-6a3e4c9479"></a>`content_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-db395d2c15"></a>`derivation_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-54af50ffb7"></a>`outcome_order` | `BIGINT` | yes | `—` | — |
| <a id="s-d271cef684"></a>`created_at` | `VARCHAR` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-c4569520c9"></a>`primary-key` | `—` | `PRIMARY KEY (claim_id, outcome_id)` |
| <a id="s-98337ecde5"></a>`unique` | `uq_collection_processing_outcomes_source_claim` | `CONSTRAINT uq_collection_processing_outcomes_source_claim UNIQUE (claim_id, source_claim_id)` |
| <a id="s-a446391540"></a>`unique` | `uq_collection_processing_outcomes_output` | `CONSTRAINT uq_collection_processing_outcomes_output UNIQUE (claim_id, collection_id)` |
| <a id="s-b61896961a"></a>`check` | `ck_collection_processing_outcomes_order` | `CONSTRAINT ck_collection_processing_outcomes_order CHECK (outcome_order IS NULL OR outcome_order >= 0)` |
| <a id="s-86afc15123"></a>`foreign-key` | `—` | `FOREIGN KEY(claim_id) REFERENCES collection_processing_claims (id) ON DELETE CASCADE` |
| <a id="s-a68209c836"></a>`foreign-key` | `—` | `FOREIGN KEY(source_claim_id) REFERENCES collection_processing_claims (id) ON DELETE RESTRICT` |
| <a id="s-1115004f43"></a>`check` | `ck_collection_processing_outcomes_claim_id_hex` | `CONSTRAINT ck_collection_processing_outcomes_claim_id_hex CHECK (length(claim_id) = 64 AND lower(claim_id) = claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-2525b9114c"></a>`check` | `ck_collection_processing_outcomes_source_claim_id_hex` | `CONSTRAINT ck_collection_processing_outcomes_source_claim_id_hex CHECK (length(source_claim_id) = 64 AND lower(source_claim_id) = source_claim_id AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(source_claim_id, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-646ea013e0"></a>`check` | `ck_collection_processing_outcomes_archive_root_sha256_hex` | `CONSTRAINT ck_collection_processing_outcomes_archive_root_sha256_hex CHECK (length(archive_root_sha256) = 64 AND lower(archive_root_sha256) = archive_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-c4aa2a8132"></a>`check` | `ck_collection_processing_outcomes_content_identity_hex` | `CONSTRAINT ck_collection_processing_outcomes_content_identity_hex CHECK (length(content_identity) = 64 AND lower(content_identity) = content_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(content_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-089fee33eb"></a>`check` | `ck_collection_processing_outcomes_derivation_sha256_hex` | `CONSTRAINT ck_collection_processing_outcomes_derivation_sha256_hex CHECK (length(derivation_sha256) = 64 AND lower(derivation_sha256) = derivation_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(derivation_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-b097e0ac22"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/49`

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
