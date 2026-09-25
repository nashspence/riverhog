# riverhog-catalog: collection_mutable_document_publication_attempts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-mutable-docum-5d22d086b5:6dc654e9f1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-d8e2cc9a0e"></a>

### Table: `collection_mutable_document_publication_attempts`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-5d77180643"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-bd66413ccc"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-e8bc9bc109"></a>`incarnation_id` | `VARCHAR(36)` | no | `—` | — |
| <a id="s-7a336bd933"></a>`document_kind` | `VARCHAR` | no | `—` | — |
| <a id="s-99a03b594f"></a>`attempt_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-9bff7e14d5"></a>`document_revision` | `BIGINT` | no | `—` | — |
| <a id="s-7706fa3f74"></a>`document_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-cb4f0241c3"></a>`document_bytes` | `BYTEA` | no | `—` | — |
| <a id="s-32d503c792"></a>`archive_storage_prefix` | `VARCHAR` | no | `—` | — |
| <a id="s-b94d0e4940"></a>`passphrase_id` | `VARCHAR` | no | `—` | — |
| <a id="s-21ddb59a23"></a>`prior_object_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-319de813c6"></a>`prior_provider_revision` | `VARCHAR` | yes | `—` | — |
| <a id="s-c4227362d2"></a>`prior_stored_bytes` | `BIGINT` | yes | `—` | — |
| <a id="s-e76d6a8db7"></a>`prior_stored_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-a1d9f17783"></a>`created_at` | `VARCHAR` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-8d9384f18c"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store, document_kind)` |
| <a id="s-3159d74f9c"></a>`foreign-key` | `—` | `FOREIGN KEY(incarnation_id, store) REFERENCES storage_incarnations (id, name)` |
| <a id="s-f9f80d0430"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, store) REFERENCES collection_archive_copies (collection_id, store) ON DELETE CASCADE` |
| <a id="s-545d9c713f"></a>`check` | `ck_mutable_document_publication_attempts_kind` | `CONSTRAINT ck_mutable_document_publication_attempts_kind CHECK (document_kind IN ('description','tag_head'))` |
| <a id="s-6d38c2a732"></a>`check` | `ck_mutable_document_publication_attempts_identity` | `CONSTRAINT ck_mutable_document_publication_attempts_identity CHECK (length(attempt_identity) = 64 AND lower(attempt_identity) = attempt_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(attempt_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-dc1e3b2e65"></a>`check` | `ck_mutable_document_publication_attempts_revision` | `CONSTRAINT ck_mutable_document_publication_attempts_revision CHECK (document_revision >= 1 AND document_revision <= 9007199254740991)` |
| <a id="s-1e470c0bbe"></a>`check` | `ck_mutable_document_publication_attempts_document_identity` | `CONSTRAINT ck_mutable_document_publication_attempts_document_identity CHECK (length(document_identity) = 64 AND lower(document_identity) = document_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(document_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-7c88cdbf32"></a>`check` | `ck_mutable_document_publication_attempts_bytes` | `CONSTRAINT ck_mutable_document_publication_attempts_bytes CHECK (octet_length(document_bytes) > 0 AND octet_length(document_bytes) <= 65807)` |
| <a id="s-7ac6640e4b"></a>`check` | `ck_mutable_document_publication_attempts_prior_receipt` | `CONSTRAINT ck_mutable_document_publication_attempts_prior_receipt CHECK (prior_object_path IS NULL AND prior_provider_revision IS NULL AND prior_stored_bytes IS NULL AND prior_stored_sha256 IS NULL OR prior_object_path IS NOT NULL AND prior_stored_bytes IS NOT NULL AND prior_stored_bytes > 0 AND prior_stored_sha256 IS NOT NULL)` |
| <a id="s-99833bf2ab"></a>`check` | `ck_mutable_document_publication_attempts_prior_sha256` | `CONSTRAINT ck_mutable_document_publication_attempts_prior_sha256 CHECK (prior_stored_sha256 IS NULL OR length(prior_stored_sha256) = 64 AND lower(prior_stored_sha256) = prior_stored_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(prior_stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-890c16c9a6"></a>`unique` | `—` | `UNIQUE (attempt_identity)` |
| <a id="s-f68bf1a9d9"></a>`check` | `ck_sha256_0f9f415e5b2f4112` | `CONSTRAINT ck_sha256_0f9f415e5b2f4112 CHECK (length(attempt_identity) = 64 AND lower(attempt_identity) = attempt_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(attempt_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-65e079f58c"></a>`check` | `ck_sha256_2b9f56df047af7b5` | `CONSTRAINT ck_sha256_2b9f56df047af7b5 CHECK (length(document_identity) = 64 AND lower(document_identity) = document_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(document_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-75d6b619ab"></a>`check` | `ck_sha256_a1e9d33ca9fd7958` | `CONSTRAINT ck_sha256_a1e9d33ca9fd7958 CHECK (prior_stored_sha256 IS NULL OR length(prior_stored_sha256) = 64 AND lower(prior_stored_sha256) = prior_stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(prior_stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-b977371ca5"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

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

<!-- exact-contract-value: f790e65a7e7ccba21c73df8925816f6063f9630fcdd433410f52e7b8ec0e0fe8 -->

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
      "definition": "store VARCHAR NOT NULL",
      "name": "store",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "incarnation_id VARCHAR(36) NOT NULL",
      "name": "incarnation_id",
      "nullable": false,
      "type": "VARCHAR(36)"
    },
    {
      "definition": "document_kind VARCHAR NOT NULL",
      "name": "document_kind",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "attempt_identity VARCHAR(64) NOT NULL",
      "name": "attempt_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "document_revision BIGINT NOT NULL",
      "name": "document_revision",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "document_identity VARCHAR(64) NOT NULL",
      "name": "document_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "document_bytes BYTEA NOT NULL",
      "name": "document_bytes",
      "nullable": false,
      "type": "BYTEA"
    },
    {
      "definition": "archive_storage_prefix VARCHAR NOT NULL",
      "name": "archive_storage_prefix",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "passphrase_id VARCHAR NOT NULL",
      "name": "passphrase_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "prior_object_path VARCHAR",
      "name": "prior_object_path",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "prior_provider_revision VARCHAR",
      "name": "prior_provider_revision",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "prior_stored_bytes BIGINT",
      "name": "prior_stored_bytes",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "prior_stored_sha256 VARCHAR(64)",
      "name": "prior_stored_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
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
        "collection_id",
        "store",
        "document_kind"
      ],
      "definition": "PRIMARY KEY (collection_id, store, document_kind)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "incarnation_id",
        "store"
      ],
      "definition": "FOREIGN KEY(incarnation_id, store) REFERENCES storage_incarnations (id, name)",
      "kind": "foreign-key",
      "references": {
        "columns": [
          "id",
          "name"
        ],
        "table": "storage_incarnations"
      }
    },
    {
      "columns": [
        "collection_id",
        "store"
      ],
      "definition": "FOREIGN KEY(collection_id, store) REFERENCES collection_archive_copies (collection_id, store) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id",
          "store"
        ],
        "table": "collection_archive_copies"
      }
    },
    {
      "definition": "CONSTRAINT ck_mutable_document_publication_attempts_kind CHECK (document_kind IN ('description','tag_head'))",
      "expression": "(document_kind IN ('description','tag_head'))",
      "kind": "check",
      "name": "ck_mutable_document_publication_attempts_kind"
    },
    {
      "definition": "CONSTRAINT ck_mutable_document_publication_attempts_identity CHECK (length(attempt_identity) = 64 AND lower(attempt_identity) = attempt_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(attempt_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(attempt_identity) = 64 AND lower(attempt_identity) = attempt_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(attempt_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_mutable_document_publication_attempts_identity"
    },
    {
      "definition": "CONSTRAINT ck_mutable_document_publication_attempts_revision CHECK (document_revision >= 1 AND document_revision <= 9007199254740991)",
      "expression": "(document_revision >= 1 AND document_revision <= 9007199254740991)",
      "kind": "check",
      "name": "ck_mutable_document_publication_attempts_revision"
    },
    {
      "definition": "CONSTRAINT ck_mutable_document_publication_attempts_document_identity CHECK (length(document_identity) = 64 AND lower(document_identity) = document_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(document_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(document_identity) = 64 AND lower(document_identity) = document_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(document_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_mutable_document_publication_attempts_document_identity"
    },
    {
      "definition": "CONSTRAINT ck_mutable_document_publication_attempts_bytes CHECK (octet_length(document_bytes) > 0 AND octet_length(document_bytes) <= 65807)",
      "expression": "(octet_length(document_bytes) > 0 AND octet_length(document_bytes) <= 65807)",
      "kind": "check",
      "name": "ck_mutable_document_publication_attempts_bytes"
    },
    {
      "definition": "CONSTRAINT ck_mutable_document_publication_attempts_prior_receipt CHECK (prior_object_path IS NULL AND prior_provider_revision IS NULL AND prior_stored_bytes IS NULL AND prior_stored_sha256 IS NULL OR prior_object_path IS NOT NULL AND prior_stored_bytes IS NOT NULL AND prior_stored_bytes > 0 AND prior_stored_sha256 IS NOT NULL)",
      "expression": "(prior_object_path IS NULL AND prior_provider_revision IS NULL AND prior_stored_bytes IS NULL AND prior_stored_sha256 IS NULL OR prior_object_path IS NOT NULL AND prior_stored_bytes IS NOT NULL AND prior_stored_bytes > 0 AND prior_stored_sha256 IS NOT NULL)",
      "kind": "check",
      "name": "ck_mutable_document_publication_attempts_prior_receipt"
    },
    {
      "definition": "CONSTRAINT ck_mutable_document_publication_attempts_prior_sha256 CHECK (prior_stored_sha256 IS NULL OR length(prior_stored_sha256) = 64 AND lower(prior_stored_sha256) = prior_stored_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(prior_stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(prior_stored_sha256 IS NULL OR length(prior_stored_sha256) = 64 AND lower(prior_stored_sha256) = prior_stored_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(prior_stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_mutable_document_publication_attempts_prior_sha256"
    },
    {
      "columns": [
        "attempt_identity"
      ],
      "definition": "UNIQUE (attempt_identity)",
      "kind": "unique"
    },
    {
      "definition": "CONSTRAINT ck_sha256_0f9f415e5b2f4112 CHECK (length(attempt_identity) = 64 AND lower(attempt_identity) = attempt_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(attempt_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(attempt_identity) = 64 AND lower(attempt_identity) = attempt_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(attempt_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_0f9f415e5b2f4112"
    },
    {
      "definition": "CONSTRAINT ck_sha256_2b9f56df047af7b5 CHECK (length(document_identity) = 64 AND lower(document_identity) = document_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(document_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(document_identity) = 64 AND lower(document_identity) = document_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(document_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_2b9f56df047af7b5"
    },
    {
      "definition": "CONSTRAINT ck_sha256_a1e9d33ca9fd7958 CHECK (prior_stored_sha256 IS NULL OR length(prior_stored_sha256) = 64 AND lower(prior_stored_sha256) = prior_stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(prior_stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(prior_stored_sha256 IS NULL OR length(prior_stored_sha256) = 64 AND lower(prior_stored_sha256) = prior_stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(prior_stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_a1e9d33ca9fd7958"
    }
  ],
  "name": "collection_mutable_document_publication_attempts"
}
```

</details>
