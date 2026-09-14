# riverhog-catalog: collection_archive_copies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-archive-copies:cfb6223bf9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-a2879b730f"></a>
- Table: `collection_archive_copies`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-bd4666ca28"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-bfefcfb058"></a>`store` | `VARCHAR` | no | `—` | — |
| <a id="s-10e4b0d4d7"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-9ae5495748"></a>`archive_storage_prefix` | `VARCHAR` | yes | `—` | — |
| <a id="s-1ef5d906e5"></a>`last_uploaded_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-249afa008b"></a>`last_verified_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-e935eb09f8"></a>`failure` | `VARCHAR` | yes | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-d780583b4c"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, store)` |
| <a id="s-3af3c2b24c"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collections (id) ON DELETE CASCADE` |
| <a id="s-3438fcb898"></a>`check` | `ck_collection_archive_copies_state` | `CONSTRAINT ck_collection_archive_copies_state CHECK (state IN ('pending','uploading','uploaded','retrying','failed'))` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-bcc8ca36ae"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/18`

### Exact owned JSON

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
