# riverhog-catalog: collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collections:fd18f9106d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-9f17464520"></a>

### Table: `collections`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1683c8820f"></a>`id` | `BIGSERIAL` | no | `—` | — |
| <a id="s-f399da7285"></a>`search_text` | `VARCHAR` | no | `—` | {"generated":"GENERATED ALWAYS AS (CAST(id AS TEXT)) STORED"} |
| <a id="s-b3e44d0509"></a>`creation_idempotency_key` | `VARCHAR` | no | `—` | — |
| <a id="s-be2edcf9a0"></a>`creation_identity_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-cd859c2958"></a>`creation_custody_mode` | `VARCHAR` | no | `—` | — |
| <a id="s-cf6f09bdb4"></a>`archive_generation` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-60a24873ef"></a>`content_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-3e8e3b39be"></a>`encryption_format` | `VARCHAR` | no | `—` | — |
| <a id="s-c22939e57b"></a>`passphrase_id` | `VARCHAR` | no | `—` | — |
| <a id="s-c7ddb3cc39"></a>`provenance_mode` | `VARCHAR` | no | `—` | — |
| <a id="s-4781da89b7"></a>`provenance_identity` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-3ae8a4b083"></a>`inventory_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-a3c200b128"></a>`archive_root_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-770ae5b08d"></a>`catalog_revision` | `BIGINT` | yes | `—` | — |
| <a id="s-f292b70346"></a>`ingest_source` | `VARCHAR` | yes | `—` | — |
| <a id="s-4fe82066b8"></a>`description` | `TEXT` | yes | `—` | — |
| <a id="s-64f28d00df"></a>`description_search` | `TEXT` | no | `''` | — |
| <a id="s-8082bbb7f0"></a>`description_revision` | `BIGINT` | no | `0` | — |
| <a id="s-ad73583389"></a>`description_identity` | `VARCHAR(64)` | no | `'0000000000000000000000000000000000000000000000000000000000000000'` | — |
| <a id="s-ffb6b02421"></a>`description_mutation_state` | `VARCHAR` | no | `'idle'` | — |
| <a id="s-48649de323"></a>`pending_description` | `TEXT` | yes | `—` | — |
| <a id="s-29a7dec281"></a>`pending_description_revision` | `BIGINT` | yes | `—` | — |
| <a id="s-8f089176d9"></a>`pending_description_identity` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-0be397f68d"></a>`description_attempt_count` | `INTEGER` | no | `0` | — |
| <a id="s-046a593ad4"></a>`description_next_attempt_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-8b6f9e4d5e"></a>`description_last_attempt_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-1aa6771db8"></a>`description_failure` | `TEXT` | yes | `—` | — |
| <a id="s-8fcb6a9bf0"></a>`tag_revision` | `BIGINT` | no | `1` | — |
| <a id="s-5d9d339360"></a>`tag_root_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-4c9a0c0e4c"></a>`tag_set_identity` | `VARCHAR(64)` | no | `'d99a47346b904680a2b3182b3950c159b297a427edfd0c8a23124b7bfb296ed9'` | — |
| <a id="s-23c54b34d5"></a>`tag_head_identity` | `VARCHAR(64)` | no | `'0000000000000000000000000000000000000000000000000000000000000000'` | — |
| <a id="s-2e0a1127de"></a>`tag_mutation_operation_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-d7f5ace036"></a>`created_by_app` | `VARCHAR` | no | `—` | — |
| <a id="s-8e1c717c10"></a>`created_by_key_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-34f6a271c0"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-b8e779dcc6"></a>`is_published` | `BOOLEAN` | no | `true` | — |
| <a id="s-8bfcba4bd2"></a>`file_count` | `BIGINT` | no | `0` | — |
| <a id="s-c730528086"></a>`file_bytes` | `BIGINT` | no | `0` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-a77d7bc5f1"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |
| <a id="s-3f1ce690b9"></a>`unique` | `uq_collections_application_idempotency_key` | `CONSTRAINT uq_collections_application_idempotency_key UNIQUE (created_by_app, creation_idempotency_key)` |
| <a id="s-98ec8eb1b1"></a>`check` | `ck_collections_file_count` | `CONSTRAINT ck_collections_file_count CHECK (file_count >= 0)` |
| <a id="s-6be6d760af"></a>`check` | `ck_collections_file_bytes` | `CONSTRAINT ck_collections_file_bytes CHECK (file_bytes >= 0)` |
| <a id="s-6074530449"></a>`check` | `ck_collections_provenance_mode` | `CONSTRAINT ck_collections_provenance_mode CHECK (provenance_mode IN ('captured','mixed','omitted'))` |
| <a id="s-b39cdfae0d"></a>`check` | `ck_collections_provenance_identity` | `CONSTRAINT ck_collections_provenance_identity CHECK (provenance_mode IN ('captured','mixed') AND provenance_identity IS NOT NULL OR provenance_mode = 'omitted' AND provenance_identity IS NULL)` |
| <a id="s-ac73efdb6a"></a>`check` | `ck_collections_content_identity` | `CONSTRAINT ck_collections_content_identity CHECK (length(content_identity) = 64)` |
| <a id="s-86ae384c8b"></a>`check` | `ck_collections_inventory_identity` | `CONSTRAINT ck_collections_inventory_identity CHECK (length(inventory_identity) = 64)` |
| <a id="s-b41d9bc1b8"></a>`check` | `ck_collections_archive_root_sha256` | `CONSTRAINT ck_collections_archive_root_sha256 CHECK (archive_root_sha256 IS NULL OR length(archive_root_sha256) = 64)` |
| <a id="s-e8f7b4d2d7"></a>`check` | `ck_collections_catalog_revision` | `CONSTRAINT ck_collections_catalog_revision CHECK (catalog_revision IS NULL OR catalog_revision > 0)` |
| <a id="s-cb4b641f35"></a>`check` | `ck_collections_description_bytes` | `CONSTRAINT ck_collections_description_bytes CHECK (description IS NULL OR octet_length(description) <= 32768)` |
| <a id="s-2185cfe6a9"></a>`check` | `ck_collections_description_search` | `CONSTRAINT ck_collections_description_search CHECK (description IS NULL AND description_search = '' OR description IS NOT NULL AND length(description_search) > 0)` |
| <a id="s-03a7a88fec"></a>`check` | `ck_collections_description_revision` | `CONSTRAINT ck_collections_description_revision CHECK (description_revision >= 0 AND description_revision <= 9007199254740991)` |
| <a id="s-7e758bcaf9"></a>`check` | `ck_collections_description_identity` | `CONSTRAINT ck_collections_description_identity CHECK (length(description_identity) = 64 AND lower(description_identity) = description_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-8ddae845ae"></a>`check` | `ck_collections_initial_description` | `CONSTRAINT ck_collections_initial_description CHECK (description_revision > 0 OR description IS NULL)` |
| <a id="s-0cea1f0935"></a>`check` | `ck_collections_description_mutation_state` | `CONSTRAINT ck_collections_description_mutation_state CHECK (description_mutation_state IN ('idle','pending','publishing','retry_wait'))` |
| <a id="s-f1e355b136"></a>`check` | `ck_collections_description_pending` | `CONSTRAINT ck_collections_description_pending CHECK (description_mutation_state = 'idle' AND pending_description_revision IS NULL AND pending_description_identity IS NULL AND description_next_attempt_at IS NULL OR description_mutation_state != 'idle' AND pending_description_revision IS NOT NULL AND pending_description_revision = description_revision + 1 AND pending_description_identity IS NOT NULL AND description_next_attempt_at IS NOT NULL)` |
| <a id="s-c968bf5c82"></a>`check` | `ck_collections_pending_description_revision` | `CONSTRAINT ck_collections_pending_description_revision CHECK (pending_description_revision IS NULL OR pending_description_revision <= 9007199254740991)` |
| <a id="s-0cd3ba6791"></a>`check` | `ck_collections_pending_description_bytes` | `CONSTRAINT ck_collections_pending_description_bytes CHECK (pending_description IS NULL OR octet_length(pending_description) <= 32768)` |
| <a id="s-e1843c1425"></a>`check` | `ck_collections_pending_description_identity` | `CONSTRAINT ck_collections_pending_description_identity CHECK (pending_description_identity IS NULL OR length(pending_description_identity) = 64 AND lower(pending_description_identity) = pending_description_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(pending_description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-72c56154e6"></a>`check` | `ck_collections_description_attempt_count` | `CONSTRAINT ck_collections_description_attempt_count CHECK (description_attempt_count >= 0)` |
| <a id="s-a84802e54b"></a>`check` | `ck_collections_tag_revision` | `CONSTRAINT ck_collections_tag_revision CHECK (tag_revision >= 1 AND tag_revision <= 9007199254740991)` |
| <a id="s-5a8578e38c"></a>`check` | `ck_collections_tag_root_sha256` | `CONSTRAINT ck_collections_tag_root_sha256 CHECK (tag_root_sha256 IS NULL OR length(tag_root_sha256) = 64 AND lower(tag_root_sha256) = tag_root_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-9ef834b1f9"></a>`check` | `ck_collections_tag_set_identity` | `CONSTRAINT ck_collections_tag_set_identity CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-640e506747"></a>`check` | `ck_collections_tag_head_identity` | `CONSTRAINT ck_collections_tag_head_identity CHECK (length(tag_head_identity) = 64 AND lower(tag_head_identity) = tag_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-e43b4e570b"></a>`check` | `ck_collections_creation_identity_sha256_hex` | `CONSTRAINT ck_collections_creation_identity_sha256_hex CHECK (length(creation_identity_sha256) = 64 AND lower(creation_identity_sha256) = creation_identity_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(creation_identity_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-d9d11f1128"></a>`check` | `ck_collections_archive_generation_hex` | `CONSTRAINT ck_collections_archive_generation_hex CHECK (length(archive_generation) = 64 AND lower(archive_generation) = archive_generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-376b5cf02d"></a>`check` | `ck_collections_content_identity_hex` | `CONSTRAINT ck_collections_content_identity_hex CHECK (length(content_identity) = 64 AND lower(content_identity) = content_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(content_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-e5ed6ba9bb"></a>`check` | `ck_collections_provenance_identity_hex` | `CONSTRAINT ck_collections_provenance_identity_hex CHECK (provenance_identity IS NULL OR length(provenance_identity) = 64 AND lower(provenance_identity) = provenance_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(provenance_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-f282b862f9"></a>`check` | `ck_collections_inventory_identity_hex` | `CONSTRAINT ck_collections_inventory_identity_hex CHECK (length(inventory_identity) = 64 AND lower(inventory_identity) = inventory_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(inventory_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-be38eb8946"></a>`check` | `ck_collections_archive_root_sha256_hex` | `CONSTRAINT ck_collections_archive_root_sha256_hex CHECK (archive_root_sha256 IS NULL OR length(archive_root_sha256) = 64 AND lower(archive_root_sha256) = archive_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-59c43925a2"></a>`check` | `ck_collections_description_identity_hex` | `CONSTRAINT ck_collections_description_identity_hex CHECK (length(description_identity) = 64 AND lower(description_identity) = description_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-73d7be2b26"></a>`check` | `ck_collections_pending_description_identity_hex` | `CONSTRAINT ck_collections_pending_description_identity_hex CHECK (pending_description_identity IS NULL OR length(pending_description_identity) = 64 AND lower(pending_description_identity) = pending_description_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(pending_description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-ffd123cb6d"></a>`check` | `ck_collections_tag_root_sha256_hex` | `CONSTRAINT ck_collections_tag_root_sha256_hex CHECK (tag_root_sha256 IS NULL OR length(tag_root_sha256) = 64 AND lower(tag_root_sha256) = tag_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-8b190eace3"></a>`check` | `ck_collections_tag_set_identity_hex` | `CONSTRAINT ck_collections_tag_set_identity_hex CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-a1421e3013"></a>`check` | `ck_collections_tag_head_identity_hex` | `CONSTRAINT ck_collections_tag_head_identity_hex CHECK (length(tag_head_identity) = 64 AND lower(tag_head_identity) = tag_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-c708654bd9"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/11`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e742175ee76b92855fc1a153992c3e9204aafe9832c4feb00f89dfa8890e9502 -->

```json
{
  "columns": [
    {
      "definition": "id BIGSERIAL NOT NULL",
      "name": "id",
      "nullable": false,
      "type": "BIGSERIAL"
    },
    {
      "definition": "search_text VARCHAR GENERATED ALWAYS AS (CAST(id AS TEXT)) STORED NOT NULL",
      "generated": "GENERATED ALWAYS AS (CAST(id AS TEXT)) STORED",
      "name": "search_text",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "creation_idempotency_key VARCHAR NOT NULL",
      "name": "creation_idempotency_key",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "creation_identity_sha256 VARCHAR(64) NOT NULL",
      "name": "creation_identity_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "creation_custody_mode VARCHAR NOT NULL",
      "name": "creation_custody_mode",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "archive_generation VARCHAR(64) NOT NULL",
      "name": "archive_generation",
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
      "definition": "encryption_format VARCHAR NOT NULL",
      "name": "encryption_format",
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
      "definition": "provenance_mode VARCHAR NOT NULL",
      "name": "provenance_mode",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "provenance_identity VARCHAR(64)",
      "name": "provenance_identity",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "inventory_identity VARCHAR(64) NOT NULL",
      "name": "inventory_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "archive_root_sha256 VARCHAR(64)",
      "name": "archive_root_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "catalog_revision BIGINT",
      "name": "catalog_revision",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "ingest_source VARCHAR",
      "name": "ingest_source",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "description TEXT",
      "name": "description",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "default": "''",
      "definition": "description_search TEXT DEFAULT '' NOT NULL",
      "name": "description_search",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "default": "0",
      "definition": "description_revision BIGINT DEFAULT 0 NOT NULL",
      "name": "description_revision",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "default": "'0000000000000000000000000000000000000000000000000000000000000000'",
      "definition": "description_identity VARCHAR(64) DEFAULT '0000000000000000000000000000000000000000000000000000000000000000' NOT NULL",
      "name": "description_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "default": "'idle'",
      "definition": "description_mutation_state VARCHAR DEFAULT 'idle' NOT NULL",
      "name": "description_mutation_state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "pending_description TEXT",
      "name": "pending_description",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "pending_description_revision BIGINT",
      "name": "pending_description_revision",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "pending_description_identity VARCHAR(64)",
      "name": "pending_description_identity",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "default": "0",
      "definition": "description_attempt_count INTEGER DEFAULT 0 NOT NULL",
      "name": "description_attempt_count",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "description_next_attempt_at VARCHAR",
      "name": "description_next_attempt_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "description_last_attempt_at VARCHAR",
      "name": "description_last_attempt_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "description_failure TEXT",
      "name": "description_failure",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "default": "1",
      "definition": "tag_revision BIGINT DEFAULT 1 NOT NULL",
      "name": "tag_revision",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "tag_root_sha256 VARCHAR(64)",
      "name": "tag_root_sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "default": "'d99a47346b904680a2b3182b3950c159b297a427edfd0c8a23124b7bfb296ed9'",
      "definition": "tag_set_identity VARCHAR(64) DEFAULT 'd99a47346b904680a2b3182b3950c159b297a427edfd0c8a23124b7bfb296ed9' NOT NULL",
      "name": "tag_set_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "default": "'0000000000000000000000000000000000000000000000000000000000000000'",
      "definition": "tag_head_identity VARCHAR(64) DEFAULT '0000000000000000000000000000000000000000000000000000000000000000' NOT NULL",
      "name": "tag_head_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "tag_mutation_operation_id VARCHAR",
      "name": "tag_mutation_operation_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "created_by_app VARCHAR NOT NULL",
      "name": "created_by_app",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "created_by_key_id VARCHAR",
      "name": "created_by_key_id",
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
      "default": "true",
      "definition": "is_published BOOLEAN DEFAULT true NOT NULL",
      "name": "is_published",
      "nullable": false,
      "type": "BOOLEAN"
    },
    {
      "default": "0",
      "definition": "file_count BIGINT DEFAULT 0 NOT NULL",
      "name": "file_count",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "default": "0",
      "definition": "file_bytes BIGINT DEFAULT 0 NOT NULL",
      "name": "file_bytes",
      "nullable": false,
      "type": "BIGINT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "id"
      ],
      "definition": "PRIMARY KEY (id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "created_by_app",
        "creation_idempotency_key"
      ],
      "definition": "CONSTRAINT uq_collections_application_idempotency_key UNIQUE (created_by_app, creation_idempotency_key)",
      "kind": "unique",
      "name": "uq_collections_application_idempotency_key"
    },
    {
      "definition": "CONSTRAINT ck_collections_file_count CHECK (file_count >= 0)",
      "expression": "(file_count >= 0)",
      "kind": "check",
      "name": "ck_collections_file_count"
    },
    {
      "definition": "CONSTRAINT ck_collections_file_bytes CHECK (file_bytes >= 0)",
      "expression": "(file_bytes >= 0)",
      "kind": "check",
      "name": "ck_collections_file_bytes"
    },
    {
      "definition": "CONSTRAINT ck_collections_provenance_mode CHECK (provenance_mode IN ('captured','mixed','omitted'))",
      "expression": "(provenance_mode IN ('captured','mixed','omitted'))",
      "kind": "check",
      "name": "ck_collections_provenance_mode"
    },
    {
      "definition": "CONSTRAINT ck_collections_provenance_identity CHECK (provenance_mode IN ('captured','mixed') AND provenance_identity IS NOT NULL OR provenance_mode = 'omitted' AND provenance_identity IS NULL)",
      "expression": "(provenance_mode IN ('captured','mixed') AND provenance_identity IS NOT NULL OR provenance_mode = 'omitted' AND provenance_identity IS NULL)",
      "kind": "check",
      "name": "ck_collections_provenance_identity"
    },
    {
      "definition": "CONSTRAINT ck_collections_content_identity CHECK (length(content_identity) = 64)",
      "expression": "(length(content_identity) = 64)",
      "kind": "check",
      "name": "ck_collections_content_identity"
    },
    {
      "definition": "CONSTRAINT ck_collections_inventory_identity CHECK (length(inventory_identity) = 64)",
      "expression": "(length(inventory_identity) = 64)",
      "kind": "check",
      "name": "ck_collections_inventory_identity"
    },
    {
      "definition": "CONSTRAINT ck_collections_archive_root_sha256 CHECK (archive_root_sha256 IS NULL OR length(archive_root_sha256) = 64)",
      "expression": "(archive_root_sha256 IS NULL OR length(archive_root_sha256) = 64)",
      "kind": "check",
      "name": "ck_collections_archive_root_sha256"
    },
    {
      "definition": "CONSTRAINT ck_collections_catalog_revision CHECK (catalog_revision IS NULL OR catalog_revision > 0)",
      "expression": "(catalog_revision IS NULL OR catalog_revision > 0)",
      "kind": "check",
      "name": "ck_collections_catalog_revision"
    },
    {
      "definition": "CONSTRAINT ck_collections_description_bytes CHECK (description IS NULL OR octet_length(description) <= 32768)",
      "expression": "(description IS NULL OR octet_length(description) <= 32768)",
      "kind": "check",
      "name": "ck_collections_description_bytes"
    },
    {
      "definition": "CONSTRAINT ck_collections_description_search CHECK (description IS NULL AND description_search = '' OR description IS NOT NULL AND length(description_search) > 0)",
      "expression": "(description IS NULL AND description_search = '' OR description IS NOT NULL AND length(description_search) > 0)",
      "kind": "check",
      "name": "ck_collections_description_search"
    },
    {
      "definition": "CONSTRAINT ck_collections_description_revision CHECK (description_revision >= 0 AND description_revision <= 9007199254740991)",
      "expression": "(description_revision >= 0 AND description_revision <= 9007199254740991)",
      "kind": "check",
      "name": "ck_collections_description_revision"
    },
    {
      "definition": "CONSTRAINT ck_collections_description_identity CHECK (length(description_identity) = 64 AND lower(description_identity) = description_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(description_identity) = 64 AND lower(description_identity) = description_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collections_description_identity"
    },
    {
      "definition": "CONSTRAINT ck_collections_initial_description CHECK (description_revision > 0 OR description IS NULL)",
      "expression": "(description_revision > 0 OR description IS NULL)",
      "kind": "check",
      "name": "ck_collections_initial_description"
    },
    {
      "definition": "CONSTRAINT ck_collections_description_mutation_state CHECK (description_mutation_state IN ('idle','pending','publishing','retry_wait'))",
      "expression": "(description_mutation_state IN ('idle','pending','publishing','retry_wait'))",
      "kind": "check",
      "name": "ck_collections_description_mutation_state"
    },
    {
      "definition": "CONSTRAINT ck_collections_description_pending CHECK (description_mutation_state = 'idle' AND pending_description_revision IS NULL AND pending_description_identity IS NULL AND description_next_attempt_at IS NULL OR description_mutation_state != 'idle' AND pending_description_revision IS NOT NULL AND pending_description_revision = description_revision + 1 AND pending_description_identity IS NOT NULL AND description_next_attempt_at IS NOT NULL)",
      "expression": "(description_mutation_state = 'idle' AND pending_description_revision IS NULL AND pending_description_identity IS NULL AND description_next_attempt_at IS NULL OR description_mutation_state != 'idle' AND pending_description_revision IS NOT NULL AND pending_description_revision = description_revision + 1 AND pending_description_identity IS NOT NULL AND description_next_attempt_at IS NOT NULL)",
      "kind": "check",
      "name": "ck_collections_description_pending"
    },
    {
      "definition": "CONSTRAINT ck_collections_pending_description_revision CHECK (pending_description_revision IS NULL OR pending_description_revision <= 9007199254740991)",
      "expression": "(pending_description_revision IS NULL OR pending_description_revision <= 9007199254740991)",
      "kind": "check",
      "name": "ck_collections_pending_description_revision"
    },
    {
      "definition": "CONSTRAINT ck_collections_pending_description_bytes CHECK (pending_description IS NULL OR octet_length(pending_description) <= 32768)",
      "expression": "(pending_description IS NULL OR octet_length(pending_description) <= 32768)",
      "kind": "check",
      "name": "ck_collections_pending_description_bytes"
    },
    {
      "definition": "CONSTRAINT ck_collections_pending_description_identity CHECK (pending_description_identity IS NULL OR length(pending_description_identity) = 64 AND lower(pending_description_identity) = pending_description_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(pending_description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(pending_description_identity IS NULL OR length(pending_description_identity) = 64 AND lower(pending_description_identity) = pending_description_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(pending_description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collections_pending_description_identity"
    },
    {
      "definition": "CONSTRAINT ck_collections_description_attempt_count CHECK (description_attempt_count >= 0)",
      "expression": "(description_attempt_count >= 0)",
      "kind": "check",
      "name": "ck_collections_description_attempt_count"
    },
    {
      "definition": "CONSTRAINT ck_collections_tag_revision CHECK (tag_revision >= 1 AND tag_revision <= 9007199254740991)",
      "expression": "(tag_revision >= 1 AND tag_revision <= 9007199254740991)",
      "kind": "check",
      "name": "ck_collections_tag_revision"
    },
    {
      "definition": "CONSTRAINT ck_collections_tag_root_sha256 CHECK (tag_root_sha256 IS NULL OR length(tag_root_sha256) = 64 AND lower(tag_root_sha256) = tag_root_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(tag_root_sha256 IS NULL OR length(tag_root_sha256) = 64 AND lower(tag_root_sha256) = tag_root_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collections_tag_root_sha256"
    },
    {
      "definition": "CONSTRAINT ck_collections_tag_set_identity CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collections_tag_set_identity"
    },
    {
      "definition": "CONSTRAINT ck_collections_tag_head_identity CHECK (length(tag_head_identity) = 64 AND lower(tag_head_identity) = tag_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(tag_head_identity) = 64 AND lower(tag_head_identity) = tag_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_collections_tag_head_identity"
    },
    {
      "definition": "CONSTRAINT ck_collections_creation_identity_sha256_hex CHECK (length(creation_identity_sha256) = 64 AND lower(creation_identity_sha256) = creation_identity_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(creation_identity_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(creation_identity_sha256) = 64 AND lower(creation_identity_sha256) = creation_identity_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(creation_identity_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collections_creation_identity_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collections_archive_generation_hex CHECK (length(archive_generation) = 64 AND lower(archive_generation) = archive_generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(archive_generation) = 64 AND lower(archive_generation) = archive_generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collections_archive_generation_hex"
    },
    {
      "definition": "CONSTRAINT ck_collections_content_identity_hex CHECK (length(content_identity) = 64 AND lower(content_identity) = content_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(content_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(content_identity) = 64 AND lower(content_identity) = content_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(content_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collections_content_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_collections_provenance_identity_hex CHECK (provenance_identity IS NULL OR length(provenance_identity) = 64 AND lower(provenance_identity) = provenance_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(provenance_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(provenance_identity IS NULL OR length(provenance_identity) = 64 AND lower(provenance_identity) = provenance_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(provenance_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collections_provenance_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_collections_inventory_identity_hex CHECK (length(inventory_identity) = 64 AND lower(inventory_identity) = inventory_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(inventory_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(inventory_identity) = 64 AND lower(inventory_identity) = inventory_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(inventory_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collections_inventory_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_collections_archive_root_sha256_hex CHECK (archive_root_sha256 IS NULL OR length(archive_root_sha256) = 64 AND lower(archive_root_sha256) = archive_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(archive_root_sha256 IS NULL OR length(archive_root_sha256) = 64 AND lower(archive_root_sha256) = archive_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collections_archive_root_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collections_description_identity_hex CHECK (length(description_identity) = 64 AND lower(description_identity) = description_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(description_identity) = 64 AND lower(description_identity) = description_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collections_description_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_collections_pending_description_identity_hex CHECK (pending_description_identity IS NULL OR length(pending_description_identity) = 64 AND lower(pending_description_identity) = pending_description_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(pending_description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(pending_description_identity IS NULL OR length(pending_description_identity) = 64 AND lower(pending_description_identity) = pending_description_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(pending_description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collections_pending_description_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_collections_tag_root_sha256_hex CHECK (tag_root_sha256 IS NULL OR length(tag_root_sha256) = 64 AND lower(tag_root_sha256) = tag_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(tag_root_sha256 IS NULL OR length(tag_root_sha256) = 64 AND lower(tag_root_sha256) = tag_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collections_tag_root_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_collections_tag_set_identity_hex CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collections_tag_set_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_collections_tag_head_identity_hex CHECK (length(tag_head_identity) = 64 AND lower(tag_head_identity) = tag_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(tag_head_identity) = 64 AND lower(tag_head_identity) = tag_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collections_tag_head_identity_hex"
    }
  ],
  "name": "collections"
}
```

</details>
