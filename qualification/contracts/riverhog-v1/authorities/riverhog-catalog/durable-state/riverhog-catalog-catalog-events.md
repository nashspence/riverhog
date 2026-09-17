# riverhog-catalog: catalog_events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-catalog-events:04b07eff6e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-fa8f69a215"></a>

### Table: `catalog_events`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1e52ad9c40"></a>`sequence` | `BIGSERIAL` | no | `—` | — |
| <a id="s-1e5460704b"></a>`revision` | `BIGINT` | yes | `—` | — |
| <a id="s-0898bbdc0b"></a>`change` | `VARCHAR` | no | `—` | — |
| <a id="s-9af8ae4edd"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-79b4a0c8b3"></a>`occurred_at` | `VARCHAR` | no | `—` | — |
| <a id="s-bbc4e9c8ff"></a>`inventory_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-536818fc0e"></a>`archive_root_sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-79827b2e43"></a>`content_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-91e9a7869f"></a>`description` | `TEXT` | yes | `—` | — |
| <a id="s-fdae917d99"></a>`description_revision` | `BIGINT` | no | `—` | — |
| <a id="s-985f68b1d9"></a>`description_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-e7f4ddf44a"></a>`tag_revision` | `BIGINT` | no | `—` | — |
| <a id="s-d786cd0c47"></a>`tag_set_identity` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-1ac4ecd0d3"></a>`before_tag_revision` | `BIGINT` | yes | `—` | — |
| <a id="s-625652328b"></a>`after_tag_revision` | `BIGINT` | yes | `—` | — |
| <a id="s-21693ef0fc"></a>`committed_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-2015fd2fd9"></a>`published` | `BOOLEAN` | no | `true` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-a5cb3a6dcb"></a>`primary-key` | `—` | `PRIMARY KEY (sequence)` |
| <a id="s-999db9919c"></a>`check` | `ck_catalog_events_revision` | `CONSTRAINT ck_catalog_events_revision CHECK (revision IS NULL OR revision > 0)` |
| <a id="s-51afc60daa"></a>`check` | `ck_catalog_events_description_bytes` | `CONSTRAINT ck_catalog_events_description_bytes CHECK (description IS NULL OR octet_length(description) <= 32768)` |
| <a id="s-7275e488f9"></a>`check` | `ck_catalog_events_description_revision` | `CONSTRAINT ck_catalog_events_description_revision CHECK (description_revision >= 0 AND description_revision <= 9007199254740991)` |
| <a id="s-f8b3f92d02"></a>`check` | `ck_catalog_events_tag_revision` | `CONSTRAINT ck_catalog_events_tag_revision CHECK (tag_revision >= 1 AND tag_revision <= 9007199254740991)` |
| <a id="s-018e17d941"></a>`check` | `ck_catalog_events_before_tag_revision` | `CONSTRAINT ck_catalog_events_before_tag_revision CHECK (before_tag_revision IS NULL OR before_tag_revision >= 1 AND before_tag_revision <= 9007199254740991)` |
| <a id="s-9468406fe8"></a>`check` | `ck_catalog_events_after_tag_revision` | `CONSTRAINT ck_catalog_events_after_tag_revision CHECK (after_tag_revision IS NULL OR after_tag_revision >= 1 AND after_tag_revision <= 9007199254740991)` |
| <a id="s-c42262a70d"></a>`check` | `ck_catalog_events_tag_set_identity` | `CONSTRAINT ck_catalog_events_tag_set_identity CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-5bcd13580b"></a>`unique` | `—` | `UNIQUE (revision)` |
| <a id="s-51e023b1dd"></a>`check` | `ck_catalog_events_inventory_identity_hex` | `CONSTRAINT ck_catalog_events_inventory_identity_hex CHECK (length(inventory_identity) = 64 AND lower(inventory_identity) = inventory_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(inventory_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-b5f0107d74"></a>`check` | `ck_catalog_events_archive_root_sha256_hex` | `CONSTRAINT ck_catalog_events_archive_root_sha256_hex CHECK (length(archive_root_sha256) = 64 AND lower(archive_root_sha256) = archive_root_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(archive_root_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-12a1fb8d20"></a>`check` | `ck_catalog_events_content_identity_hex` | `CONSTRAINT ck_catalog_events_content_identity_hex CHECK (length(content_identity) = 64 AND lower(content_identity) = content_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(content_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-5963427231"></a>`check` | `ck_catalog_events_description_identity_hex` | `CONSTRAINT ck_catalog_events_description_identity_hex CHECK (length(description_identity) = 64 AND lower(description_identity) = description_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(description_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |
| <a id="s-2a33e0aea4"></a>`check` | `ck_catalog_events_tag_set_identity_hex` | `CONSTRAINT ck_catalog_events_tag_set_identity_hex CHECK (length(tag_set_identity) = 64 AND lower(tag_set_identity) = tag_set_identity AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(tag_set_identity, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-d9c07cf8a0"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/2`

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
