# riverhog-catalog: collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collections:5813b6c4ad -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-c29b83c623"></a>

### Table: `collections`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-3ac7e44768"></a>`id` | `BIGSERIAL` | no | `—` | — |
| <a id="s-884731af6c"></a>`search_text` | `VARCHAR` | no | `—` | {"generated":"GENERATED ALWAYS AS (CAST(id AS TEXT)) STORED"} |
| <a id="s-2e0b94d2ef"></a>`creation_idempotency_key` | `VARCHAR` | no | `—` | — |
| <a id="s-e59568dd7c"></a>`creation_identity_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-dbd374c071"></a>`creation_custody_mode` | `VARCHAR` | no | `—` | — |
| <a id="s-cefb50b33c"></a>`creation_archive_store` | `VARCHAR` | no | `—` | — |
| <a id="s-15ca3fce71"></a>`creation_use_cache` | `BOOLEAN` | no | `—` | — |
| <a id="s-2b4a7ccbf4"></a>`creation_copy_to_json` | `TEXT` | no | `—` | — |
| <a id="s-513bb222b5"></a>`archive_generation` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-c017bb9293"></a>`content_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-d3b391fc10"></a>`encryption_format` | `VARCHAR` | no | `—` | — |
| <a id="s-1ed48f9c17"></a>`passphrase_id` | `VARCHAR` | no | `—` | — |
| <a id="s-958e6e6fd3"></a>`provenance_mode` | `VARCHAR` | no | `—` | — |
| <a id="s-5c771a003f"></a>`provenance_identity` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-23f3ff0b4b"></a>`inventory_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-724faae316"></a>`archive_root_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-5136a292e2"></a>`catalog_revision` | `BIGINT` | yes | `—` | — |
| <a id="s-cd9579e1bf"></a>`ingest_source` | `VARCHAR` | yes | `—` | — |
| <a id="s-785ce8c954"></a>`description` | `TEXT` | yes | `—` | — |
| <a id="s-adaf516234"></a>`description_search` | `TEXT` | no | `''` | — |
| <a id="s-eface2f450"></a>`description_revision` | `BIGINT` | no | `0` | — |
| <a id="s-0c63624731"></a>`description_identity` | `VARCHAR(64)` | no | `'0000000000000000000000000000000000000000000000000000000000000000'` | — |
| <a id="s-118998becc"></a>`description_mutation_state` | `VARCHAR` | no | `'idle'` | — |
| <a id="s-33748d9050"></a>`pending_description` | `TEXT` | yes | `—` | — |
| <a id="s-2c2c35c360"></a>`pending_description_revision` | `BIGINT` | yes | `—` | — |
| <a id="s-d45fed2c50"></a>`pending_description_identity` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-328205aff9"></a>`description_attempt_count` | `INTEGER` | no | `0` | — |
| <a id="s-f0e3e82274"></a>`description_next_attempt_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-7b7cbde334"></a>`description_last_attempt_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-09e5932571"></a>`description_failure` | `TEXT` | yes | `—` | — |
| <a id="s-5c305e345d"></a>`tag_revision` | `BIGINT` | no | `1` | — |
| <a id="s-d887812584"></a>`tag_root_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-d6a8cede3e"></a>`tag_set_identity` | `VARCHAR(64)` | no | `'d99a47346b904680a2b3182b3950c159b297a427edfd0c8a23124b7bfb296ed9'` | — |
| <a id="s-ab80db4c29"></a>`tag_head_identity` | `VARCHAR(64)` | no | `'0000000000000000000000000000000000000000000000000000000000000000'` | — |
| <a id="s-5a52ed9525"></a>`tag_mutation_operation_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-6ec3e77c31"></a>`created_by_principal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-7a55339f0b"></a>`created_by_key_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-e2789e7da9"></a>`created_at` | `VARCHAR` | no | `—` | — |
| <a id="s-7fbdc12fbb"></a>`is_published` | `BOOLEAN` | no | `true` | — |
| <a id="s-3067bd984e"></a>`file_count` | `BIGINT` | no | `0` | — |
| <a id="s-34bdcedfc2"></a>`file_bytes` | `BIGINT` | no | `0` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-a0c685c4a7"></a>`primary-key` | `—` | `PRIMARY KEY (id)` |
| <a id="s-6330a66a8e"></a>`unique` | `uq_collections_application_idempotency_key` | `CONSTRAINT uq_collections_application_idempotency_key UNIQUE (created_by_principal_id, creation_idempotency_key)` |
| <a id="s-5d4fc26b4c"></a>`check` | `ck_collections_file_count` | `CONSTRAINT ck_collections_file_count CHECK (file_count >= 0)` |
| <a id="s-3280762f34"></a>`check` | `ck_collections_file_bytes` | `CONSTRAINT ck_collections_file_bytes CHECK (file_bytes >= 0)` |
| <a id="s-de68bae95e"></a>`check` | `ck_collections_provenance_mode` | `CONSTRAINT ck_collections_provenance_mode CHECK (provenance_mode IN ('captured','mixed','omitted'))` |
| <a id="s-7fb31f9c37"></a>`check` | `ck_collections_provenance_identity` | `CONSTRAINT ck_collections_provenance_identity CHECK (provenance_mode IN ('captured','mixed') AND provenance_identity IS NOT NULL OR provenance_mode = 'omitted' AND provenance_identity IS NULL)` |
| <a id="s-303192555c"></a>`check` | `ck_collections_content_identity` | `CONSTRAINT ck_collections_content_identity CHECK (length(content_identity) = 64)` |
| <a id="s-50f2fb2c31"></a>`check` | `ck_collections_inventory_identity` | `CONSTRAINT ck_collections_inventory_identity CHECK (length(inventory_identity) = 64)` |
| <a id="s-a340e1eb74"></a>`check` | `ck_collections_archive_root_sha256` | `CONSTRAINT ck_collections_archive_root_sha256 CHECK (archive_root_sha256 IS NULL OR length(archive_root_sha256) = 64)` |
| <a id="s-76d1ee4671"></a>`check` | `ck_collections_catalog_revision` | `CONSTRAINT ck_collections_catalog_revision CHECK (catalog_revision IS NULL OR catalog_revision > 0)` |
| <a id="s-b01dfe70eb"></a>`check` | `ck_collections_description_bytes` | `CONSTRAINT ck_collections_description_bytes CHECK (description IS NULL OR octet_length(description) <= 32768)` |
| <a id="s-502a5c8f16"></a>`check` | `ck_collections_description_search` | `CONSTRAINT ck_collections_description_search CHECK (description IS NULL AND description_search = '' OR description IS NOT NULL AND length(description_search) > 0)` |
| <a id="s-ef90e64105"></a>`check` | `ck_collections_description_revision` | `CONSTRAINT ck_collections_description_revision CHECK (description_revision >= 0 AND description_revision <= 9007199254740991)` |
| <a id="s-df79f1f5b3"></a>`check` | `ck_collections_description_identity` | `CONSTRAINT ck_collections_description_identity CHECK (length(description_identity) = 64 AND lower(description_identity) = description_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-195f82e586"></a>`check` | `ck_collections_initial_description` | `CONSTRAINT ck_collections_initial_description CHECK (description_revision > 0 OR description IS NULL)` |
| <a id="s-8db3fe1342"></a>`check` | `ck_collections_description_mutation_state` | `CONSTRAINT ck_collections_description_mutation_state CHECK (description_mutation_state IN ('idle','pending','publishing','retry_wait'))` |
| <a id="s-fe06f71e97"></a>`check` | `ck_collections_description_pending` | `CONSTRAINT ck_collections_description_pending CHECK (description_mutation_state = 'idle' AND pending_description_revision IS NULL AND pending_description_identity IS NULL AND description_next_attempt_at IS NULL OR description_mutation_state != 'idle' AND pending_description_revision IS NOT NULL AND pending_description_revision = description_revision + 1 AND pending_description_identity IS NOT NULL AND description_next_attempt_at IS NOT NULL)` |
| <a id="s-1833162e83"></a>`check` | `ck_collections_pending_description_revision` | `CONSTRAINT ck_collections_pending_description_revision CHECK (pending_description_revision IS NULL OR pending_description_revision <= 9007199254740991)` |
| <a id="s-f165340579"></a>`check` | `ck_collections_pending_description_bytes` | `CONSTRAINT ck_collections_pending_description_bytes CHECK (pending_description IS NULL OR octet_length(pending_description) <= 32768)` |
| <a id="s-693f3a99c7"></a>`check` | `ck_collections_pending_description_identity` | `CONSTRAINT ck_collections_pending_description_identity CHECK (pending_description_identity IS NULL OR length(pending_description_identity) = 64 AND lower(pending_description_identity) = pending_description_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(pending_description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-cc96eb31b8"></a>`check` | `ck_collections_description_attempt_count` | `CONSTRAINT ck_collections_description_attempt_count CHECK (description_attempt_count >= 0)` |
| <a id="s-07a1fa313d"></a>`check` | `ck_collections_tag_revision` | `CONSTRAINT ck_collections_tag_revision CHECK (tag_revision >= 1 AND tag_revision <= 9007199254740991)` |
| <a id="s-13e36a0040"></a>`check` | `ck_collections_tag_root_sha256` | `CONSTRAINT ck_collections_tag_root_sha256 CHECK (tag_root_sha256 IS NULL OR length(tag_root_sha256) = 64 AND lower(tag_root_sha256) = tag_root_sha256 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-a1935f6cad"></a>`check` | `ck_collections_tag_set_identity` | `CONSTRAINT ck_collections_tag_set_identity CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-106f6d461a"></a>`check` | `ck_collections_tag_head_identity` | `CONSTRAINT ck_collections_tag_head_identity CHECK (length(tag_head_identity) = 64 AND lower(tag_head_identity) = tag_head_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-4e73ec074c"></a>`check` | `ck_collections_creation_identity_sha256_hex` | `CONSTRAINT ck_collections_creation_identity_sha256_hex CHECK (length(creation_identity_sha256) = 64 AND lower(creation_identity_sha256) = creation_identity_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(creation_identity_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-264370f028"></a>`check` | `ck_collections_archive_generation_hex` | `CONSTRAINT ck_collections_archive_generation_hex CHECK (length(archive_generation) = 64 AND lower(archive_generation) = archive_generation AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_generation, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-bc183c5670"></a>`check` | `ck_collections_content_identity_hex` | `CONSTRAINT ck_collections_content_identity_hex CHECK (length(content_identity) = 64 AND lower(content_identity) = content_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(content_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-d3ce566c5a"></a>`check` | `ck_collections_provenance_identity_hex` | `CONSTRAINT ck_collections_provenance_identity_hex CHECK (provenance_identity IS NULL OR length(provenance_identity) = 64 AND lower(provenance_identity) = provenance_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(provenance_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-8b8ff3a6f1"></a>`check` | `ck_collections_inventory_identity_hex` | `CONSTRAINT ck_collections_inventory_identity_hex CHECK (length(inventory_identity) = 64 AND lower(inventory_identity) = inventory_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(inventory_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-1048348af3"></a>`check` | `ck_collections_archive_root_sha256_hex` | `CONSTRAINT ck_collections_archive_root_sha256_hex CHECK (archive_root_sha256 IS NULL OR length(archive_root_sha256) = 64 AND lower(archive_root_sha256) = archive_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-500d8242f2"></a>`check` | `ck_collections_description_identity_hex` | `CONSTRAINT ck_collections_description_identity_hex CHECK (length(description_identity) = 64 AND lower(description_identity) = description_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-eb3852990a"></a>`check` | `ck_collections_pending_description_identity_hex` | `CONSTRAINT ck_collections_pending_description_identity_hex CHECK (pending_description_identity IS NULL OR length(pending_description_identity) = 64 AND lower(pending_description_identity) = pending_description_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(pending_description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-205e64fab8"></a>`check` | `ck_collections_tag_root_sha256_hex` | `CONSTRAINT ck_collections_tag_root_sha256_hex CHECK (tag_root_sha256 IS NULL OR length(tag_root_sha256) = 64 AND lower(tag_root_sha256) = tag_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-eeba73634c"></a>`check` | `ck_collections_tag_set_identity_hex` | `CONSTRAINT ck_collections_tag_set_identity_hex CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-27c2843377"></a>`check` | `ck_collections_tag_head_identity_hex` | `CONSTRAINT ck_collections_tag_head_identity_hex CHECK (length(tag_head_identity) = 64 AND lower(tag_head_identity) = tag_head_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_head_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-7504368607"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/12`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75118effdc5b1c02e42160f1a055d5ba4f58dd6504a8228b20542a69b511bef9 -->

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
      "definition": "creation_archive_store VARCHAR NOT NULL",
      "name": "creation_archive_store",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "creation_use_cache BOOLEAN NOT NULL",
      "name": "creation_use_cache",
      "nullable": false,
      "type": "BOOLEAN"
    },
    {
      "definition": "creation_copy_to_json TEXT NOT NULL",
      "name": "creation_copy_to_json",
      "nullable": false,
      "type": "TEXT"
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
      "definition": "created_by_principal_id VARCHAR NOT NULL",
      "name": "created_by_principal_id",
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
        "created_by_principal_id",
        "creation_idempotency_key"
      ],
      "definition": "CONSTRAINT uq_collections_application_idempotency_key UNIQUE (created_by_principal_id, creation_idempotency_key)",
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
