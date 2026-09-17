# piggity-local: desired_collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:piggity-local:piggity-local-desired-collections:1c6d0d598f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity-local](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-d7a99604bf"></a>

### Table: `desired_collections`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-c78d560741"></a>`collection_id` | `INTEGER` | no | `—` | — |
| <a id="s-54d8ae611e"></a>`inventory_identity` | `TEXT` | no | `—` | — |
| <a id="s-7d97cef19d"></a>`inventory_cursor` | `TEXT` | yes | `—` | — |
| <a id="s-c020171817"></a>`inventory_complete` | `INTEGER` | no | `0` | — |
| <a id="s-de9542fde4"></a>`tag_revision` | `INTEGER` | no | `—` | — |
| <a id="s-9a956e19e0"></a>`tag_set_identity` | `TEXT` | no | `—` | — |
| <a id="s-7abfffd018"></a>`tag_page_token` | `TEXT` | yes | `—` | — |
| <a id="s-5517c89638"></a>`tags_complete` | `INTEGER` | no | `0` | — |
| <a id="s-990f6e42e8"></a>`created_at` | `TEXT` | no | `—` | — |
| <a id="s-5f422b0429"></a>`remote_deleted` | `INTEGER` | no | `0` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-7a24366b1d"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id)` |
| <a id="s-4b73c8a83a"></a>`check` | `ck_desired_collections_id` | `CONSTRAINT ck_desired_collections_id CHECK (collection_id > 0)` |
| <a id="s-299ac851d5"></a>`check` | `ck_desired_collections_etag` | `CONSTRAINT ck_desired_collections_etag CHECK (length(inventory_identity) = 64 AND inventory_identity = lower(inventory_identity) AND inventory_identity NOT GLOB '*[^0-9a-f]*')` |
| <a id="s-6d31ebc9d6"></a>`check` | `ck_desired_collections_inventory_complete` | `CONSTRAINT ck_desired_collections_inventory_complete CHECK (inventory_complete IN (0, 1))` |
| <a id="s-164bc1633b"></a>`check` | `ck_desired_collections_tag_revision` | `CONSTRAINT ck_desired_collections_tag_revision CHECK (tag_revision >= 1)` |
| <a id="s-c3da244d41"></a>`check` | `ck_desired_collections_tag_set_identity` | `CONSTRAINT ck_desired_collections_tag_set_identity CHECK (length(tag_set_identity) = 64 AND tag_set_identity = lower(tag_set_identity) AND tag_set_identity NOT GLOB '*[^0-9a-f]*')` |
| <a id="s-a3e713fb81"></a>`check` | `ck_desired_collections_tags_complete` | `CONSTRAINT ck_desired_collections_tags_complete CHECK (tags_complete IN (0, 1))` |
| <a id="s-d80cb1420a"></a>`check` | `ck_desired_collections_remote_deleted` | `CONSTRAINT ck_desired_collections_remote_deleted CHECK (remote_deleted IN (0, 1))` |

## Maintained corroboration

### Related interface records

- [Schema identity](piggity-local-durable-state-identity.md)

## Governing policies

- <a id="pa-7b2c0e2091"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:piggity-local](../../../evidence/sources/authorities.md#src-f6a1289f67) — [reference/riverhog/applications/piggity/src/piggity/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../reference/riverhog/applications/piggity/src/piggity/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/1/structure/tables/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 330d5fe194ee2d362f930f802a2745e04eb008975a067e860c429d95c6c3e12f -->

```json
{
  "columns": [
    {
      "definition": "collection_id INTEGER NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "inventory_identity TEXT NOT NULL",
      "name": "inventory_identity",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "inventory_cursor TEXT",
      "name": "inventory_cursor",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "default": "0",
      "definition": "inventory_complete INTEGER DEFAULT 0 NOT NULL",
      "name": "inventory_complete",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "tag_revision INTEGER NOT NULL",
      "name": "tag_revision",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "tag_set_identity TEXT NOT NULL",
      "name": "tag_set_identity",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "tag_page_token TEXT",
      "name": "tag_page_token",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "default": "0",
      "definition": "tags_complete INTEGER DEFAULT 0 NOT NULL",
      "name": "tags_complete",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "created_at TEXT NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "default": "0",
      "definition": "remote_deleted INTEGER DEFAULT 0 NOT NULL",
      "name": "remote_deleted",
      "nullable": false,
      "type": "INTEGER"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id"
      ],
      "definition": "PRIMARY KEY (collection_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_desired_collections_id CHECK (collection_id > 0)",
      "expression": "(collection_id > 0)",
      "kind": "check",
      "name": "ck_desired_collections_id"
    },
    {
      "definition": "CONSTRAINT ck_desired_collections_etag CHECK (length(inventory_identity) = 64 AND inventory_identity = lower(inventory_identity) AND inventory_identity NOT GLOB '*[^0-9a-f]*')",
      "expression": "(length(inventory_identity) = 64 AND inventory_identity = lower(inventory_identity) AND inventory_identity NOT GLOB '*[^0-9a-f]*')",
      "kind": "check",
      "name": "ck_desired_collections_etag"
    },
    {
      "definition": "CONSTRAINT ck_desired_collections_inventory_complete CHECK (inventory_complete IN (0, 1))",
      "expression": "(inventory_complete IN (0, 1))",
      "kind": "check",
      "name": "ck_desired_collections_inventory_complete"
    },
    {
      "definition": "CONSTRAINT ck_desired_collections_tag_revision CHECK (tag_revision >= 1)",
      "expression": "(tag_revision >= 1)",
      "kind": "check",
      "name": "ck_desired_collections_tag_revision"
    },
    {
      "definition": "CONSTRAINT ck_desired_collections_tag_set_identity CHECK (length(tag_set_identity) = 64 AND tag_set_identity = lower(tag_set_identity) AND tag_set_identity NOT GLOB '*[^0-9a-f]*')",
      "expression": "(length(tag_set_identity) = 64 AND tag_set_identity = lower(tag_set_identity) AND tag_set_identity NOT GLOB '*[^0-9a-f]*')",
      "kind": "check",
      "name": "ck_desired_collections_tag_set_identity"
    },
    {
      "definition": "CONSTRAINT ck_desired_collections_tags_complete CHECK (tags_complete IN (0, 1))",
      "expression": "(tags_complete IN (0, 1))",
      "kind": "check",
      "name": "ck_desired_collections_tags_complete"
    },
    {
      "definition": "CONSTRAINT ck_desired_collections_remote_deleted CHECK (remote_deleted IN (0, 1))",
      "expression": "(remote_deleted IN (0, 1))",
      "kind": "check",
      "name": "ck_desired_collections_remote_deleted"
    }
  ],
  "name": "desired_collections"
}
```

</details>
