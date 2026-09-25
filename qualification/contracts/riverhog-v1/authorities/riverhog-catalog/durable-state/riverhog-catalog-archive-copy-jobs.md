# riverhog-catalog: archive_copy_jobs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-archive-copy-jobs:69e030ac91 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-2ebafc64bb"></a>

### Table: `archive_copy_jobs`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-f63ffef409"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-57624bf7ea"></a>`destination_store` | `VARCHAR` | no | `—` | — |
| <a id="s-5cdf070c38"></a>`destination_incarnation_id` | `VARCHAR(36)` | no | `—` | — |
| <a id="s-4feee4738f"></a>`destination_storage_prefix` | `VARCHAR` | no | `—` | — |
| <a id="s-5f19f7960d"></a>`source_store` | `VARCHAR` | no | `—` | — |
| <a id="s-1ffe295c16"></a>`source_incarnation_id` | `VARCHAR(36)` | no | `—` | — |
| <a id="s-9b4616192e"></a>`use_cache` | `BOOLEAN` | no | `—` | — |
| <a id="s-574d54fde2"></a>`initiated_by_app` | `VARCHAR` | no | `—` | — |
| <a id="s-b8571c4ae6"></a>`initiated_by_key_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-46eba23a11"></a>`event_context_json` | `TEXT` | yes | `—` | — |
| <a id="s-ebf01e77f7"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-630171d91d"></a>`requested_at` | `VARCHAR` | no | `—` | — |
| <a id="s-05f311b0ff"></a>`read_requested_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-12ec9e615b"></a>`ready_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-d1ae132b78"></a>`expires_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-d3804ee5f6"></a>`batch_start_order` | `VARCHAR(65)` | yes | `—` | — |
| <a id="s-a9377ecb0b"></a>`batch_end_order` | `VARCHAR(65)` | yes | `—` | — |
| <a id="s-20b0468032"></a>`destination_discarded_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-07fbe8eeff"></a>`next_attempt_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-e35738d567"></a>`finished_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-f8f75f2f77"></a>`failure` | `VARCHAR` | yes | `—` | — |
| <a id="s-859caa1df6"></a>`search_text` | `VARCHAR` | no | `—` | {"generated":"GENERATED ALWAYS AS (lower(CAST(collection_id AS TEXT) \|\| ' ' \|\| source_store \|\| ' ' \|\| destination_store \|\| ' ' \|\| state)) STORED"} |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-464336e6f6"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, destination_store)` |
| <a id="s-360f676840"></a>`foreign-key` | `—` | `FOREIGN KEY(source_incarnation_id, source_store) REFERENCES storage_incarnations (id, name)` |
| <a id="s-5c23c1927e"></a>`foreign-key` | `—` | `FOREIGN KEY(destination_incarnation_id, destination_store) REFERENCES storage_incarnations (id, name)` |
| <a id="s-f8be7174b7"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, source_store) REFERENCES collection_archive_copies (collection_id, store) ON DELETE CASCADE` |
| <a id="s-d02c86d25a"></a>`check` | `ck_archive_copy_jobs_state` | `CONSTRAINT ck_archive_copy_jobs_state CHECK (state IN ('requested','waiting','checking','copying','canceling','completed','failed','canceled'))` |
| <a id="s-71e9a5c5a3"></a>`check` | `ck_archive_copy_jobs_batch` | `CONSTRAINT ck_archive_copy_jobs_batch CHECK (batch_start_order IS NULL AND batch_end_order IS NULL OR batch_start_order IS NOT NULL AND batch_end_order >= batch_start_order)` |
| <a id="s-94d70ff825"></a>`check` | `ck_archive_copy_jobs_batch_start_order` | `CONSTRAINT ck_archive_copy_jobs_batch_start_order CHECK (batch_start_order IS NULL OR length(batch_start_order) = 65 AND lower(batch_start_order) = batch_start_order AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(batch_start_order, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-402f976223"></a>`check` | `ck_archive_copy_jobs_batch_end_order` | `CONSTRAINT ck_archive_copy_jobs_batch_end_order CHECK (batch_end_order IS NULL OR length(batch_end_order) = 65 AND lower(batch_end_order) = batch_end_order AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(batch_end_order, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-a8483e73b1"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/41`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2c46e828aafddb334194a931de6aa68d2eff24f7f045c673fe4ba5327ed699dd -->

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
      "definition": "destination_store VARCHAR NOT NULL",
      "name": "destination_store",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "destination_incarnation_id VARCHAR(36) NOT NULL",
      "name": "destination_incarnation_id",
      "nullable": false,
      "type": "VARCHAR(36)"
    },
    {
      "definition": "destination_storage_prefix VARCHAR NOT NULL",
      "name": "destination_storage_prefix",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "source_store VARCHAR NOT NULL",
      "name": "source_store",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "source_incarnation_id VARCHAR(36) NOT NULL",
      "name": "source_incarnation_id",
      "nullable": false,
      "type": "VARCHAR(36)"
    },
    {
      "definition": "use_cache BOOLEAN NOT NULL",
      "name": "use_cache",
      "nullable": false,
      "type": "BOOLEAN"
    },
    {
      "definition": "initiated_by_app VARCHAR NOT NULL",
      "name": "initiated_by_app",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "initiated_by_key_id VARCHAR",
      "name": "initiated_by_key_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "event_context_json TEXT",
      "name": "event_context_json",
      "nullable": true,
      "type": "TEXT"
    },
    {
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "requested_at VARCHAR NOT NULL",
      "name": "requested_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "read_requested_at VARCHAR",
      "name": "read_requested_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "ready_at VARCHAR",
      "name": "ready_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "expires_at VARCHAR",
      "name": "expires_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "batch_start_order VARCHAR(65)",
      "name": "batch_start_order",
      "nullable": true,
      "type": "VARCHAR(65)"
    },
    {
      "definition": "batch_end_order VARCHAR(65)",
      "name": "batch_end_order",
      "nullable": true,
      "type": "VARCHAR(65)"
    },
    {
      "definition": "destination_discarded_at VARCHAR",
      "name": "destination_discarded_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "next_attempt_at VARCHAR",
      "name": "next_attempt_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "finished_at VARCHAR",
      "name": "finished_at",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "failure VARCHAR",
      "name": "failure",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "search_text VARCHAR GENERATED ALWAYS AS (lower(CAST(collection_id AS TEXT) || ' ' || source_store || ' ' || destination_store || ' ' || state)) STORED NOT NULL",
      "generated": "GENERATED ALWAYS AS (lower(CAST(collection_id AS TEXT) || ' ' || source_store || ' ' || destination_store || ' ' || state)) STORED",
      "name": "search_text",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "destination_store"
      ],
      "definition": "PRIMARY KEY (collection_id, destination_store)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "source_incarnation_id",
        "source_store"
      ],
      "definition": "FOREIGN KEY(source_incarnation_id, source_store) REFERENCES storage_incarnations (id, name)",
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
      "columns": [
        "destination_incarnation_id",
        "destination_store"
      ],
      "definition": "FOREIGN KEY(destination_incarnation_id, destination_store) REFERENCES storage_incarnations (id, name)",
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
      "columns": [
        "collection_id",
        "source_store"
      ],
      "definition": "FOREIGN KEY(collection_id, source_store) REFERENCES collection_archive_copies (collection_id, store) ON DELETE CASCADE",
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
      "definition": "CONSTRAINT ck_archive_copy_jobs_state CHECK (state IN ('requested','waiting','checking','copying','canceling','completed','failed','canceled'))",
      "expression": "(state IN ('requested','waiting','checking','copying','canceling','completed','failed','canceled'))",
      "kind": "check",
      "name": "ck_archive_copy_jobs_state"
    },
    {
      "definition": "CONSTRAINT ck_archive_copy_jobs_batch CHECK (batch_start_order IS NULL AND batch_end_order IS NULL OR batch_start_order IS NOT NULL AND batch_end_order >= batch_start_order)",
      "expression": "(batch_start_order IS NULL AND batch_end_order IS NULL OR batch_start_order IS NOT NULL AND batch_end_order >= batch_start_order)",
      "kind": "check",
      "name": "ck_archive_copy_jobs_batch"
    },
    {
      "definition": "CONSTRAINT ck_archive_copy_jobs_batch_start_order CHECK (batch_start_order IS NULL OR length(batch_start_order) = 65 AND lower(batch_start_order) = batch_start_order AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(batch_start_order, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(batch_start_order IS NULL OR length(batch_start_order) = 65 AND lower(batch_start_order) = batch_start_order AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(batch_start_order, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_archive_copy_jobs_batch_start_order"
    },
    {
      "definition": "CONSTRAINT ck_archive_copy_jobs_batch_end_order CHECK (batch_end_order IS NULL OR length(batch_end_order) = 65 AND lower(batch_end_order) = batch_end_order AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(batch_end_order, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(batch_end_order IS NULL OR length(batch_end_order) = 65 AND lower(batch_end_order) = batch_end_order AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(batch_end_order, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_archive_copy_jobs_batch_end_order"
    }
  ],
  "name": "archive_copy_jobs"
}
```

</details>
