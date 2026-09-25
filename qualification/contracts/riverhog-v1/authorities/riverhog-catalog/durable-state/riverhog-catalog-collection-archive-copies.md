# riverhog-catalog: collection_archive_copies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-archive-copies:4fccc9cfe4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-2729cfe6c9"></a>

### Table: `collection_archive_copies`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-36c735cf5e"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-2dd54321a8"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-ae0229fc91"></a>`incarnation_id` | `VARCHAR(36)` | no | `—` | — |
| <a id="s-3334dd13c2"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-c462329e99"></a>`archive_storage_prefix` | `VARCHAR` | yes | `—` | — |
| <a id="s-38d74ddcc7"></a>`last_uploaded_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-54b239eebf"></a>`last_verified_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-5cbf479950"></a>`failure` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-1de11207d3"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store)` |
| <a id="s-02e5aeae57"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE` |
| <a id="s-be9a17bd71"></a>`foreign-key` | `—` | `FOREIGN KEY(incarnation_id, store) REFERENCES storage_incarnations (id, name)` |
| <a id="s-7d4f3eec9b"></a>`check` | `ck_collection_archive_copies_state` | `CONSTRAINT ck_collection_archive_copies_state CHECK (state IN ('pending','uploading','uploaded','retrying','failed'))` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-5f9017ba28"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/20`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bed1b6a2ce1a45133d1bca3a23c2c0695551a754c9b4c6bac74933a5df29c93a -->

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
