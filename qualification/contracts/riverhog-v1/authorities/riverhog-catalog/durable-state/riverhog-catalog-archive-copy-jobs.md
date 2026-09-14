# riverhog-catalog: archive_copy_jobs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-archive-copy-jobs:94f5e6e713 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-07d9a1b0cb"></a>
- Table: `archive_copy_jobs`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-9942861055"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-692751482a"></a>`destination_store` | `VARCHAR` | no | `—` | — |
| <a id="s-639a288dcd"></a>`destination_storage_prefix` | `VARCHAR` | no | `—` | — |
| <a id="s-5e16f41caf"></a>`source_store` | `VARCHAR` | no | `—` | — |
| <a id="s-c86be3b2f0"></a>`initiated_by_app` | `VARCHAR` | no | `—` | — |
| <a id="s-5faac2e4f4"></a>`initiated_by_key_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-62bbcd68c5"></a>`event_context_json` | `TEXT` | yes | `—` | — |
| <a id="s-de5ae5cefc"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-6cb8b2ae8d"></a>`requested_at` | `VARCHAR` | no | `—` | — |
| <a id="s-1e0a5aeeac"></a>`read_requested_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-d1288dc50e"></a>`ready_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-41735a1629"></a>`expires_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-d1aa1af007"></a>`batch_start_order` | `VARCHAR(65)` | yes | `—` | — |
| <a id="s-4a093cd57d"></a>`batch_end_order` | `VARCHAR(65)` | yes | `—` | — |
| <a id="s-6683d93033"></a>`destination_discarded_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-47c367fc6b"></a>`next_attempt_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-bdaf1de960"></a>`completed_at` | `VARCHAR` | yes | `—` | — |
| <a id="s-da04114afb"></a>`failure` | `VARCHAR` | yes | `—` | — |
| <a id="s-9173b6f815"></a>`search_text` | `VARCHAR` | no | `—` | {"generated":"GENERATED ALWAYS AS (lower(CAST(collection_id AS TEXT) \|\| ' ' \|\| source_store \|\| ' ' \|\| destination_store \|\| ' ' \|\| state)) STORED"} |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-68f7abe07f"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, destination_store)` |
| <a id="s-95a164f0a5"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, source_store) REFERENCES collection_archive_copies (collection_id, store) ON DELETE CASCADE` |
| <a id="s-b54f413ebf"></a>`check` | `ck_archive_copy_jobs_state` | `CONSTRAINT ck_archive_copy_jobs_state CHECK (state IN ('requested','waiting','checking','copying','canceling','completed','failed','canceled'))` |
| <a id="s-67c3ba74b0"></a>`check` | `ck_archive_copy_jobs_batch` | `CONSTRAINT ck_archive_copy_jobs_batch CHECK (batch_start_order IS NULL AND batch_end_order IS NULL OR batch_start_order IS NOT NULL AND batch_end_order >= batch_start_order)` |
| <a id="s-c30c7de264"></a>`check` | `ck_archive_copy_jobs_batch_start_order` | `CONSTRAINT ck_archive_copy_jobs_batch_start_order CHECK (batch_start_order IS NULL OR length(batch_start_order) = 65 AND lower(batch_start_order) = batch_start_order AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(batch_start_order, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-ab18e67502"></a>`check` | `ck_archive_copy_jobs_batch_end_order` | `CONSTRAINT ck_archive_copy_jobs_batch_end_order CHECK (batch_end_order IS NULL OR length(batch_end_order) = 65 AND lower(batch_end_order) = batch_end_order AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(batch_end_order, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |

## Maintained corroboration

### Related interface records

- [riverhog-catalog durable-state identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-3871780656"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/39`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa2992cbfd56f0d2acd4ce2405a4fe0c238c62b6a00698b2bf5a71849a58ddc9 -->

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
      "definition": "completed_at VARCHAR",
      "name": "completed_at",
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
