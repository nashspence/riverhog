# riverhog-catalog: collection_tag_publications

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-tag-publications:95488a8ba3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-d8e2cc9a0e"></a>

### Table: `collection_tag_publications`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-5d77180643"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-bd66413ccc"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-e8bc9bc109"></a>`desired_revision` | `BIGINT` | no | `—` | — |
| <a id="s-7a336bd933"></a>`desired_tag_set_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-99a03b594f"></a>`desired_head_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-9bff7e14d5"></a>`published_revision` | `BIGINT` | no | `—` | — |
| <a id="s-7706fa3f74"></a>`published_tag_set_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-cb4f0241c3"></a>`published_head_identity` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-32d503c792"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-b94d0e4940"></a>`next_attempt_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-21ddb59a23"></a>`failure` | `TEXT` | yes | `—` | — |
| <a id="s-319de813c6"></a>`head_object_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-c4227362d2"></a>`head_provider_revision` | `VARCHAR` | yes | `—` | — |
| <a id="s-e76d6a8db7"></a>`head_stored_bytes` | `BIGINT` | yes | `—` | — |
| <a id="s-a1d9f17783"></a>`head_stored_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-64665f52c6"></a>`published_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-8d9384f18c"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store)` |
| <a id="s-3159d74f9c"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, store) REFERENCES collection_archive_copies (collection_id, store) ON DELETE CASCADE` |
| <a id="s-f9f80d0430"></a>`check` | `ck_collection_tag_publications_desired_revision` | `CONSTRAINT ck_collection_tag_publications_desired_revision CHECK (desired_revision >= 1 AND desired_revision <= 9007199254740991)` |
| <a id="s-545d9c713f"></a>`check` | `ck_collection_tag_publications_published_revision` | `CONSTRAINT ck_collection_tag_publications_published_revision CHECK (published_revision >= 0 AND published_revision <= 9007199254740991)` |
| <a id="s-6d38c2a732"></a>`check` | `ck_collection_tag_publications_state` | `CONSTRAINT ck_collection_tag_publications_state CHECK (state IN ('pending','publishing_nodes','publishing_head','published','retry_wait'))` |
| <a id="s-dc1e3b2e65"></a>`check` | `ck_collection_tag_publications_desired_set_identity` | `CONSTRAINT ck_collection_tag_publications_desired_set_identity CHECK (length(desired_tag_set_identity) = 64 AND lower(desired_tag_set_identity) = desired_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-1e470c0bbe"></a>`check` | `ck_collection_tag_publications_desired_head_identity` | `CONSTRAINT ck_collection_tag_publications_desired_head_identity CHECK (length(desired_head_identity) = 64 AND lower(desired_head_identity) = desired_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-7c88cdbf32"></a>`check` | `ck_collection_tag_publications_published_set_identity` | `CONSTRAINT ck_collection_tag_publications_published_set_identity CHECK (length(published_tag_set_identity) = 64 AND lower(published_tag_set_identity) = published_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(published_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-7ac6640e4b"></a>`check` | `ck_collection_tag_publications_desired_tag_set_identity_hex` | `CONSTRAINT ck_collection_tag_publications_desired_tag_set_identity_hex CHECK (length(desired_tag_set_identity) = 64 AND lower(desired_tag_set_identity) = desired_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-99833bf2ab"></a>`check` | `ck_collection_tag_publications_desired_head_identity_hex` | `CONSTRAINT ck_collection_tag_publications_desired_head_identity_hex CHECK (length(desired_head_identity) = 64 AND lower(desired_head_identity) = desired_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-890c16c9a6"></a>`check` | `ck_sha256_9da01d6c5fd7290f` | `CONSTRAINT ck_sha256_9da01d6c5fd7290f CHECK (length(published_tag_set_identity) = 64 AND lower(published_tag_set_identity) = published_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(published_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-f68bf1a9d9"></a>`check` | `ck_collection_tag_publications_published_head_identity_hex` | `CONSTRAINT ck_collection_tag_publications_published_head_identity_hex CHECK (published_head_identity IS NULL OR length(published_head_identity) = 64 AND lower(published_head_identity) = published_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(published_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-65e079f58c"></a>`check` | `ck_collection_tag_publications_head_stored_sha256_hex` | `CONSTRAINT ck_collection_tag_publications_head_stored_sha256_hex CHECK (head_stored_sha256 IS NULL OR length(head_stored_sha256) = 64 AND lower(head_stored_sha256) = head_stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-8976bd80f2"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/59`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2e99aeab2c66811bbae62f55a8ecc02a62f6af3bfa41f4d9b016e8d9de89193c -->

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
      "definition": "desired_revision BIGINT NOT NULL",
      "name": "desired_revision",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "desired_tag_set_identity VARCHAR(64) NOT NULL",
      "name": "desired_tag_set_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "desired_head_identity VARCHAR(64) NOT NULL",
      "name": "desired_head_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "published_revision BIGINT NOT NULL",
      "name": "published_revision",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "published_tag_set_identity VARCHAR(64) NOT NULL",
      "name": "published_tag_set_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "published_head_identity VARCHAR(64)",
      "name": "published_head_identity",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "next_attempt_at VARCHAR",
      "name": "next_attempt_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "failure TEXT",
      "name": "failure",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "head_object_path VARCHAR",
      "name": "head_object_path",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "head_provider_revision VARCHAR",
      "name": "head_provider_revision",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "head_stored_bytes BIGINT",
      "name": "head_stored_bytes",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "head_stored_sha256 VARCHAR(64)",
      "name": "head_stored_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "published_at VARCHAR",
      "name": "published_at",
      "nullable": true,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "store"
      ],
      "definition": "PRIMARY KEY (collection_id, store)",
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
      "definition": "CONSTRAINT ck_collection_tag_publications_desired_revision CHECK (desired_revision >= 1 AND desired_revision <= 9007199254740991)",
      "expression": "(desired_revision >= 1 AND desired_revision <= 9007199254740991)",
      "kind": "check",
      "name": "ck_collection_tag_publications_desired_revision"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_publications_published_revision CHECK (published_revision >= 0 AND published_revision <= 9007199254740991)",
      "expression": "(published_revision >= 0 AND published_revision <= 9007199254740991)",
      "kind": "check",
      "name": "ck_collection_tag_publications_published_revision"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_publications_state CHECK (state IN ('pending','publishing_nodes','publishing_head','published','retry_wait'))",
      "expression": "(state IN ('pending','publishing_nodes','publishing_head','published','retry_wait'))",
      "kind": "check",
      "name": "ck_collection_tag_publications_state"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_publications_desired_set_identity CHECK (length(desired_tag_set_identity) = 64 AND lower(desired_tag_set_identity) = desired_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(desired_tag_set_identity) = 64 AND lower(desired_tag_set_identity) = desired_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_publications_desired_set_identity"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_publications_desired_head_identity CHECK (length(desired_head_identity) = 64 AND lower(desired_head_identity) = desired_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(desired_head_identity) = 64 AND lower(desired_head_identity) = desired_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_publications_desired_head_identity"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_publications_published_set_identity CHECK (length(published_tag_set_identity) = 64 AND lower(published_tag_set_identity) = published_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(published_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(published_tag_set_identity) = 64 AND lower(published_tag_set_identity) = published_tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(published_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collection_tag_publications_published_set_identity"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_publications_desired_tag_set_identity_hex CHECK (length(desired_tag_set_identity) = 64 AND lower(desired_tag_set_identity) = desired_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(desired_tag_set_identity) = 64 AND lower(desired_tag_set_identity) = desired_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_publications_desired_tag_set_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_publications_desired_head_identity_hex CHECK (length(desired_head_identity) = 64 AND lower(desired_head_identity) = desired_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(desired_head_identity) = 64 AND lower(desired_head_identity) = desired_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_publications_desired_head_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_sha256_9da01d6c5fd7290f CHECK (length(published_tag_set_identity) = 64 AND lower(published_tag_set_identity) = published_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(published_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(published_tag_set_identity) = 64 AND lower(published_tag_set_identity) = published_tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(published_tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_9da01d6c5fd7290f"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_publications_published_head_identity_hex CHECK (published_head_identity IS NULL OR length(published_head_identity) = 64 AND lower(published_head_identity) = published_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(published_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(published_head_identity IS NULL OR length(published_head_identity) = 64 AND lower(published_head_identity) = published_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(published_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_publications_published_head_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_collection_tag_publications_head_stored_sha256_hex CHECK (head_stored_sha256 IS NULL OR length(head_stored_sha256) = 64 AND lower(head_stored_sha256) = head_stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(head_stored_sha256 IS NULL OR length(head_stored_sha256) = 64 AND lower(head_stored_sha256) = head_stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(head_stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_tag_publications_head_stored_sha256_hex"
    }
  ],
  "name": "collection_tag_publications"
}
```

</details>
