# riverhog-catalog: collection_description_publications

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-description-publications:9d3b5013d4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-5135cc46d8"></a>

### Table: `collection_description_publications`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-6dfd570257"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-4f8ba8a21c"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-e061d4aecb"></a>`desired_revision` | `BIGINT` | no | `—` | — |
| <a id="s-5442036c1a"></a>`desired_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-ba869643a0"></a>`published_revision` | `BIGINT` | no | `—` | — |
| <a id="s-fc0510f0ef"></a>`published_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-bd919e02c0"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-c1a4b16909"></a>`attempt_count` | `INTEGER` | no | `0` | — |
| <a id="s-d182b1ad8a"></a>`next_attempt_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-001ce14272"></a>`last_attempt_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-6c9404505d"></a>`failure` | `TEXT` | yes | `—` | — |
| <a id="s-f8678b4e88"></a>`object_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-6d480a27b0"></a>`provider_revision` | `VARCHAR` | yes | `—` | — |
| <a id="s-40eefe40ac"></a>`stored_bytes` | `BIGINT` | yes | `—` | — |
| <a id="s-c475fbb1d2"></a>`stored_sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-8dc72cc230"></a>`published_at` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-2f0fb38e63"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store)` |
| <a id="s-fcf4c9fc58"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, store) REFERENCES collection_archive_copies (collection_id, store) ON DELETE CASCADE` |
| <a id="s-caa2e2b8fd"></a>`check` | `ck_description_publications_desired_revision` | `CONSTRAINT ck_description_publications_desired_revision CHECK (desired_revision >= 0 AND desired_revision <= 9007199254740991)` |
| <a id="s-39108e3409"></a>`check` | `ck_description_publications_published_revision` | `CONSTRAINT ck_description_publications_published_revision CHECK (published_revision >= 0 AND published_revision <= 9007199254740991)` |
| <a id="s-3c88fa1677"></a>`check` | `ck_description_publications_desired_identity` | `CONSTRAINT ck_description_publications_desired_identity CHECK (length(desired_identity) = 64 AND lower(desired_identity) = desired_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-450b7c8a71"></a>`check` | `ck_description_publications_published_identity` | `CONSTRAINT ck_description_publications_published_identity CHECK (length(published_identity) = 64 AND lower(published_identity) = published_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(published_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-36fab2d022"></a>`check` | `ck_description_publications_attempt_count` | `CONSTRAINT ck_description_publications_attempt_count CHECK (attempt_count >= 0)` |
| <a id="s-463f13a9c1"></a>`check` | `ck_description_publications_state` | `CONSTRAINT ck_description_publications_state CHECK (state IN ('pending','publishing','published','retry_wait'))` |
| <a id="s-fac512a5ed"></a>`check` | `ck_description_publications_next_attempt` | `CONSTRAINT ck_description_publications_next_attempt CHECK (state = 'published' AND next_attempt_at IS NULL OR state != 'published' AND next_attempt_at IS NOT NULL)` |
| <a id="s-099cd253e6"></a>`check` | `ck_description_publications_receipt` | `CONSTRAINT ck_description_publications_receipt CHECK (published_revision = 0 AND object_path IS NULL AND stored_bytes IS NULL AND stored_sha256 IS NULL OR published_revision > 0 AND object_path IS NOT NULL AND stored_bytes IS NOT NULL AND stored_sha256 IS NOT NULL)` |
| <a id="s-ef601aacb6"></a>`check` | `ck_collection_description_publications_desired_identity_hex` | `CONSTRAINT ck_collection_description_publications_desired_identity_hex CHECK (length(desired_identity) = 64 AND lower(desired_identity) = desired_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-f1ddf0a677"></a>`check` | `ck_sha256_5f9e0ec935743970` | `CONSTRAINT ck_sha256_5f9e0ec935743970 CHECK (length(published_identity) = 64 AND lower(published_identity) = published_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(published_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-cea7abab8c"></a>`check` | `ck_collection_description_publications_stored_sha256_hex` | `CONSTRAINT ck_collection_description_publications_stored_sha256_hex CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-c81eb51591"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/44`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98f021c75a1e0fb1042eecc1deae995028c318e925b9693fc06ee0786821ea27 -->

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
      "definition": "desired_identity VARCHAR(64) NOT NULL",
      "name": "desired_identity",
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
      "definition": "published_identity VARCHAR(64) NOT NULL",
      "name": "published_identity",
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
      "default": "0",
      "definition": "attempt_count INTEGER DEFAULT 0 NOT NULL",
      "name": "attempt_count",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "next_attempt_at VARCHAR",
      "name": "next_attempt_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "last_attempt_at VARCHAR",
      "name": "last_attempt_at",
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
      "definition": "object_path VARCHAR",
      "name": "object_path",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "provider_revision VARCHAR",
      "name": "provider_revision",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "stored_bytes BIGINT",
      "name": "stored_bytes",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "stored_sha256 VARCHAR(64)",
      "name": "stored_sha256",
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
      "definition": "CONSTRAINT ck_description_publications_desired_revision CHECK (desired_revision >= 0 AND desired_revision <= 9007199254740991)",
      "expression": "(desired_revision >= 0 AND desired_revision <= 9007199254740991)",
      "kind": "check",
      "name": "ck_description_publications_desired_revision"
    },
    {
      "definition": "CONSTRAINT ck_description_publications_published_revision CHECK (published_revision >= 0 AND published_revision <= 9007199254740991)",
      "expression": "(published_revision >= 0 AND published_revision <= 9007199254740991)",
      "kind": "check",
      "name": "ck_description_publications_published_revision"
    },
    {
      "definition": "CONSTRAINT ck_description_publications_desired_identity CHECK (length(desired_identity) = 64 AND lower(desired_identity) = desired_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(desired_identity) = 64 AND lower(desired_identity) = desired_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_description_publications_desired_identity"
    },
    {
      "definition": "CONSTRAINT ck_description_publications_published_identity CHECK (length(published_identity) = 64 AND lower(published_identity) = published_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(published_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(published_identity) = 64 AND lower(published_identity) = published_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(published_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_description_publications_published_identity"
    },
    {
      "definition": "CONSTRAINT ck_description_publications_attempt_count CHECK (attempt_count >= 0)",
      "expression": "(attempt_count >= 0)",
      "kind": "check",
      "name": "ck_description_publications_attempt_count"
    },
    {
      "definition": "CONSTRAINT ck_description_publications_state CHECK (state IN ('pending','publishing','published','retry_wait'))",
      "expression": "(state IN ('pending','publishing','published','retry_wait'))",
      "kind": "check",
      "name": "ck_description_publications_state"
    },
    {
      "definition": "CONSTRAINT ck_description_publications_next_attempt CHECK (state = 'published' AND next_attempt_at IS NULL OR state != 'published' AND next_attempt_at IS NOT NULL)",
      "expression": "(state = 'published' AND next_attempt_at IS NULL OR state != 'published' AND next_attempt_at IS NOT NULL)",
      "kind": "check",
      "name": "ck_description_publications_next_attempt"
    },
    {
      "definition": "CONSTRAINT ck_description_publications_receipt CHECK (published_revision = 0 AND object_path IS NULL AND stored_bytes IS NULL AND stored_sha256 IS NULL OR published_revision > 0 AND object_path IS NOT NULL AND stored_bytes IS NOT NULL AND stored_sha256 IS NOT NULL)",
      "expression": "(published_revision = 0 AND object_path IS NULL AND stored_bytes IS NULL AND stored_sha256 IS NULL OR published_revision > 0 AND object_path IS NOT NULL AND stored_bytes IS NOT NULL AND stored_sha256 IS NOT NULL)",
      "kind": "check",
      "name": "ck_description_publications_receipt"
    },
    {
      "definition": "CONSTRAINT ck_collection_description_publications_desired_identity_hex CHECK (length(desired_identity) = 64 AND lower(desired_identity) = desired_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(desired_identity) = 64 AND lower(desired_identity) = desired_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(desired_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_description_publications_desired_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_sha256_5f9e0ec935743970 CHECK (length(published_identity) = 64 AND lower(published_identity) = published_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(published_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(published_identity) = 64 AND lower(published_identity) = published_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(published_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_5f9e0ec935743970"
    },
    {
      "definition": "CONSTRAINT ck_collection_description_publications_stored_sha256_hex CHECK (stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(stored_sha256 IS NULL OR length(stored_sha256) = 64 AND lower(stored_sha256) = stored_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(stored_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_collection_description_publications_stored_sha256_hex"
    }
  ],
  "name": "collection_description_publications"
}
```

</details>
