# riverhog-catalog: collection_archive_copies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-archive-copies:60e87a9b2b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-fc8d1d92cf"></a>

### Table: `collection_archive_copies`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-ece90bd4cf"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-5df0aae33f"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-1c59f17454"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-ada38e2f6a"></a>`archive_storage_prefix` | `VARCHAR` | yes | `—` | — |
| <a id="s-b656f1317a"></a>`last_uploaded_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-a7a308f106"></a>`last_verified_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-ad1b8c9435"></a>`failure` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-f3d5d14e20"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store)` |
| <a id="s-c146b926df"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE` |
| <a id="s-2f65e84d6a"></a>`check` | `ck_collection_archive_copies_state` | `CONSTRAINT ck_collection_archive_copies_state CHECK (state IN ('pending','uploading','uploaded','retrying','failed'))` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-1b97c21950"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/19`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 35c9bfcd5b72565fb246c456a2eaff585e35a9ff74d17a33b8b79697e50ea88a -->

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
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "archive_storage_prefix VARCHAR",
      "name": "archive_storage_prefix",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "last_uploaded_at VARCHAR",
      "name": "last_uploaded_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "last_verified_at VARCHAR",
      "name": "last_verified_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "failure VARCHAR",
      "name": "failure",
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
        "collection_id"
      ],
      "definition": "FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "id"
        ],
        "table": "collections"
      }
    },
    {
      "definition": "CONSTRAINT ck_collection_archive_copies_state CHECK (state IN ('pending','uploading','uploaded','retrying','failed'))",
      "expression": "(state IN ('pending','uploading','uploaded','retrying','failed'))",
      "kind": "check",
      "name": "ck_collection_archive_copies_state"
    }
  ],
  "name": "collection_archive_copies"
}
```

</details>
