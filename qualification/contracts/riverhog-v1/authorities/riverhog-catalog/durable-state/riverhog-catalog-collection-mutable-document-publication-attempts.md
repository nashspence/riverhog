# riverhog-catalog: collection_mutable_document_publication_attempts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-mutable-docum-5d22d086b5:9708ab603c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-e603651366"></a>
- Table: `collection_mutable_document_publication_attempts`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-256aa570c4"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-680c6c8297"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-398eaa9b42"></a>`document_kind` | `VARCHAR` | no | `—` | — |
| <a id="s-3518585307"></a>`attempt_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-aab5b57aa1"></a>`document_revision` | `BIGINT` | no | `—` | — |
| <a id="s-250d746ecd"></a>`document_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-93236b7a6e"></a>`document_bytes` | `BYTEA` | no | `—` | — |
| <a id="s-200b09fb20"></a>`archive_storage_prefix` | `VARCHAR` | no | `—` | — |
| <a id="s-9137b51056"></a>`passphrase_id` | `VARCHAR` | no | `—` | — |
| <a id="s-ccddc9e12c"></a>`prior_object_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-13e9682b08"></a>`prior_provider_revision` | `VARCHAR` | yes | `—` | — |
| <a id="s-d17a18680a"></a>`prior_stored_bytes` | `BIGINT` | yes | `—` | — |
| <a id="s-23183b3bcb"></a>`prior_stored_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-c31a5cb7d8"></a>`created_at` | `VARCHAR` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-502b238b80"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store, document_kind)` |
| <a id="s-b2f90327c3"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, store) REFERENCES collection_archive_copies (collection_id, store) ON DELETE CASCADE` |
| <a id="s-3309eea4fd"></a>`check` | `ck_mutable_document_publication_attempts_kind` | `CONSTRAINT ck_mutable_document_publication_attempts_kind CHECK (document_kind IN ('description','tag_head'))` |
| <a id="s-80def0e45d"></a>`check` | `ck_mutable_document_publication_attempts_identity` | `CONSTRAINT ck_mutable_document_publication_attempts_identity CHECK (length(attempt_identity) = 64 AND lower(attempt_identity) = attempt_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(attempt_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-1c41895ad3"></a>`check` | `ck_mutable_document_publication_attempts_revision` | `CONSTRAINT ck_mutable_document_publication_attempts_revision CHECK (document_revision >= 1 AND document_revision <= 9007199254740991)` |
| <a id="s-90d867887b"></a>`check` | `ck_mutable_document_publication_attempts_document_identity` | `CONSTRAINT ck_mutable_document_publication_attempts_document_identity CHECK (length(document_identity) = 64 AND lower(document_identity) = document_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(document_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-e85c1e85a2"></a>`check` | `ck_mutable_document_publication_attempts_bytes` | `CONSTRAINT ck_mutable_document_publication_attempts_bytes CHECK (octet_length(document_bytes) > 0 AND octet_length(document_bytes) <= 65807)` |
| <a id="s-07534d7c12"></a>`check` | `ck_mutable_document_publication_attempts_prior_receipt` | `CONSTRAINT ck_mutable_document_publication_attempts_prior_receipt CHECK (prior_object_path IS NULL AND prior_provider_revision IS NULL AND prior_stored_bytes IS NULL AND prior_stored_sha256 IS NULL OR prior_object_path IS NOT NULL AND prior_stored_bytes IS NOT NULL AND prior_stored_bytes > 0 AND prior_stored_sha256 IS NOT NULL)` |
| <a id="s-b84dd2336b"></a>`check` | `ck_mutable_document_publication_attempts_prior_sha256` | `CONSTRAINT ck_mutable_document_publication_attempts_prior_sha256 CHECK (prior_stored_sha256 IS NULL OR length(prior_stored_sha256) = 64 AND lower(prior_stored_sha256) = prior_stored_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(prior_stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-ab127e4002"></a>`check` | `ck_sha256_0f9f415e5b2f4112` | `CONSTRAINT ck_sha256_0f9f415e5b2f4112 CHECK (length(attempt_identity) = 64 AND lower(attempt_identity) = attempt_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(attempt_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-4265bda273"></a>`check` | `ck_sha256_2b9f56df047af7b5` | `CONSTRAINT ck_sha256_2b9f56df047af7b5 CHECK (length(document_identity) = 64 AND lower(document_identity) = document_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(document_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-e22b96e11c"></a>`check` | `ck_sha256_a1e9d33ca9fd7958` | `CONSTRAINT ck_sha256_a1e9d33ca9fd7958 CHECK (prior_stored_sha256 IS NULL OR length(prior_stored_sha256) = 64 AND lower(prior_stored_sha256) = prior_stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(prior_stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-900cf64e93"></a>`unique` | `—` | `UNIQUE (attempt_identity)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-22a45f126f"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/57`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7d9f455755df179f5ec1ea0039c2825ffed660dfefadfaaa9488e0fa6c21398f -->

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
    },
    {
      "columns": [
        "attempt_identity"
      ],
      "definition": "UNIQUE (attempt_identity)",
      "kind": "unique"
    }
  ],
  "name": "collection_mutable_document_publication_attempts"
}
```
