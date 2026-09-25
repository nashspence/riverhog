# riverhog-catalog: catalog_events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-catalog-events:23c9ceb032 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-08ed02092e"></a>

### Table: `catalog_events`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-a977a0f315"></a>`sequence` | `BIGSERIAL` | no | `—` | — |
| <a id="s-aaa5b757fb"></a>`revision` | `BIGINT` | yes | `—` | — |
| <a id="s-445eb048f6"></a>`change` | `VARCHAR` | no | `—` | — |
| <a id="s-21ccb15417"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-23fc593190"></a>`occurred_at` | `VARCHAR` | no | `—` | — |
| <a id="s-e1a842bb73"></a>`inventory_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-49fe5d5099"></a>`archive_root_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-334a4c3318"></a>`content_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-1ba4a5abc1"></a>`description` | `TEXT` | yes | `—` | — |
| <a id="s-a6be3a9238"></a>`description_revision` | `BIGINT` | no | `—` | — |
| <a id="s-036d6a86d9"></a>`description_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-5ca6110be5"></a>`tag_revision` | `BIGINT` | no | `—` | — |
| <a id="s-3dc1fbc7c3"></a>`tag_set_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-846732ace1"></a>`before_tag_revision` | `BIGINT` | yes | `—` | — |
| <a id="s-ce71a343c7"></a>`after_tag_revision` | `BIGINT` | yes | `—` | — |
| <a id="s-1d327137ee"></a>`committed_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-62a045f302"></a>`published` | `BOOLEAN` | no | `true` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-382bfdb1d4"></a>`primary-key` | `—` | `PRIMARY KEY (sequence)` |
| <a id="s-50e2b09328"></a>`check` | `ck_catalog_events_revision` | `CONSTRAINT ck_catalog_events_revision CHECK (revision IS NULL OR revision > 0)` |
| <a id="s-70a38d8476"></a>`check` | `ck_catalog_events_description_bytes` | `CONSTRAINT ck_catalog_events_description_bytes CHECK (description IS NULL OR octet_length(description) <= 32768)` |
| <a id="s-87abe469e7"></a>`check` | `ck_catalog_events_description_revision` | `CONSTRAINT ck_catalog_events_description_revision CHECK (description_revision >= 0 AND description_revision <= 9007199254740991)` |
| <a id="s-239c48418a"></a>`check` | `ck_catalog_events_tag_revision` | `CONSTRAINT ck_catalog_events_tag_revision CHECK (tag_revision >= 1 AND tag_revision <= 9007199254740991)` |
| <a id="s-5f4c8080c0"></a>`check` | `ck_catalog_events_before_tag_revision` | `CONSTRAINT ck_catalog_events_before_tag_revision CHECK (before_tag_revision IS NULL OR before_tag_revision >= 1 AND before_tag_revision <= 9007199254740991)` |
| <a id="s-da07fb85b9"></a>`check` | `ck_catalog_events_after_tag_revision` | `CONSTRAINT ck_catalog_events_after_tag_revision CHECK (after_tag_revision IS NULL OR after_tag_revision >= 1 AND after_tag_revision <= 9007199254740991)` |
| <a id="s-579a7c20a8"></a>`check` | `ck_catalog_events_tag_set_identity` | `CONSTRAINT ck_catalog_events_tag_set_identity CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-9b2212a91f"></a>`unique` | `—` | `UNIQUE (revision)` |
| <a id="s-fea3712339"></a>`check` | `ck_catalog_events_inventory_identity_hex` | `CONSTRAINT ck_catalog_events_inventory_identity_hex CHECK (length(inventory_identity) = 64 AND lower(inventory_identity) = inventory_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(inventory_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-725df53340"></a>`check` | `ck_catalog_events_archive_root_sha256_hex` | `CONSTRAINT ck_catalog_events_archive_root_sha256_hex CHECK (length(archive_root_sha256) = 64 AND lower(archive_root_sha256) = archive_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-58d7b3580b"></a>`check` | `ck_catalog_events_content_identity_hex` | `CONSTRAINT ck_catalog_events_content_identity_hex CHECK (length(content_identity) = 64 AND lower(content_identity) = content_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(content_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-f575708164"></a>`check` | `ck_catalog_events_description_identity_hex` | `CONSTRAINT ck_catalog_events_description_identity_hex CHECK (length(description_identity) = 64 AND lower(description_identity) = description_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-2a5a131305"></a>`check` | `ck_catalog_events_tag_set_identity_hex` | `CONSTRAINT ck_catalog_events_tag_set_identity_hex CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-55e8634b28"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/3`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9257057f591b8d4d6734b2f15789055a35f409b932c5784f7c42994437ea78db -->

```json
{
  "columns": [
    {
      "definition": "sequence BIGSERIAL NOT NULL",
      "name": "sequence",
      "nullable": false,
      "type": "BIGSERIAL"
    },
    {
      "definition": "revision BIGINT",
      "name": "revision",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "change VARCHAR NOT NULL",
      "name": "change",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "collection_id BIGINT NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "occurred_at VARCHAR NOT NULL",
      "name": "occurred_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "inventory_identity VARCHAR(64) NOT NULL",
      "name": "inventory_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
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
      "definition": "description TEXT",
      "name": "description",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "description_revision BIGINT NOT NULL",
      "name": "description_revision",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "description_identity VARCHAR(64) NOT NULL",
      "name": "description_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "tag_revision BIGINT NOT NULL",
      "name": "tag_revision",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "tag_set_identity VARCHAR(64) NOT NULL",
      "name": "tag_set_identity",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "before_tag_revision BIGINT",
      "name": "before_tag_revision",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "after_tag_revision BIGINT",
      "name": "after_tag_revision",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "committed_at VARCHAR",
      "name": "committed_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "default": "true",
      "definition": "published BOOLEAN DEFAULT true NOT NULL",
      "name": "published",
      "nullable": false,
      "type": "BOOLEAN"
    }
  ],
  "constraints": [
    {
      "columns": [
        "sequence"
      ],
      "definition": "PRIMARY KEY (sequence)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_catalog_events_revision CHECK (revision IS NULL OR revision > 0)",
      "expression": "(revision IS NULL OR revision > 0)",
      "kind": "check",
      "name": "ck_catalog_events_revision"
    },
    {
      "definition": "CONSTRAINT ck_catalog_events_description_bytes CHECK (description IS NULL OR octet_length(description) <= 32768)",
      "expression": "(description IS NULL OR octet_length(description) <= 32768)",
      "kind": "check",
      "name": "ck_catalog_events_description_bytes"
    },
    {
      "definition": "CONSTRAINT ck_catalog_events_description_revision CHECK (description_revision >= 0 AND description_revision <= 9007199254740991)",
      "expression": "(description_revision >= 0 AND description_revision <= 9007199254740991)",
      "kind": "check",
      "name": "ck_catalog_events_description_revision"
    },
    {
      "definition": "CONSTRAINT ck_catalog_events_tag_revision CHECK (tag_revision >= 1 AND tag_revision <= 9007199254740991)",
      "expression": "(tag_revision >= 1 AND tag_revision <= 9007199254740991)",
      "kind": "check",
      "name": "ck_catalog_events_tag_revision"
    },
    {
      "definition": "CONSTRAINT ck_catalog_events_before_tag_revision CHECK (before_tag_revision IS NULL OR before_tag_revision >= 1 AND before_tag_revision <= 9007199254740991)",
      "expression": "(before_tag_revision IS NULL OR before_tag_revision >= 1 AND before_tag_revision <= 9007199254740991)",
      "kind": "check",
      "name": "ck_catalog_events_before_tag_revision"
    },
    {
      "definition": "CONSTRAINT ck_catalog_events_after_tag_revision CHECK (after_tag_revision IS NULL OR after_tag_revision >= 1 AND after_tag_revision <= 9007199254740991)",
      "expression": "(after_tag_revision IS NULL OR after_tag_revision >= 1 AND after_tag_revision <= 9007199254740991)",
      "kind": "check",
      "name": "ck_catalog_events_after_tag_revision"
    },
    {
      "definition": "CONSTRAINT ck_catalog_events_tag_set_identity CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_catalog_events_tag_set_identity"
    },
    {
      "columns": [
        "revision"
      ],
      "definition": "UNIQUE (revision)",
      "kind": "unique"
    },
    {
      "definition": "CONSTRAINT ck_catalog_events_inventory_identity_hex CHECK (length(inventory_identity) = 64 AND lower(inventory_identity) = inventory_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(inventory_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(inventory_identity) = 64 AND lower(inventory_identity) = inventory_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(inventory_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_catalog_events_inventory_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_catalog_events_archive_root_sha256_hex CHECK (length(archive_root_sha256) = 64 AND lower(archive_root_sha256) = archive_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(archive_root_sha256) = 64 AND lower(archive_root_sha256) = archive_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_catalog_events_archive_root_sha256_hex"
    },
    {
      "definition": "CONSTRAINT ck_catalog_events_content_identity_hex CHECK (length(content_identity) = 64 AND lower(content_identity) = content_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(content_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(content_identity) = 64 AND lower(content_identity) = content_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(content_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_catalog_events_content_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_catalog_events_description_identity_hex CHECK (length(description_identity) = 64 AND lower(description_identity) = description_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(description_identity) = 64 AND lower(description_identity) = description_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_catalog_events_description_identity_hex"
    },
    {
      "definition": "CONSTRAINT ck_catalog_events_tag_set_identity_hex CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_catalog_events_tag_set_identity_hex"
    }
  ],
  "name": "catalog_events"
}
```

</details>
