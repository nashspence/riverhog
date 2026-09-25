# riverhog-catalog: retrieval_plan_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-plan-files:68b3aecaeb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-dcea214d6b"></a>

### Table: `retrieval_plan_files`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-84e33dea59"></a>`plan_id` | `VARCHAR` | no | `—` | — |
| <a id="s-01e4ab62b3"></a>`file_order` | `INTEGER` | no | `—` | — |
| <a id="s-91b79ad15a"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-2895a5a292"></a>`path` | `VARCHAR` | no | `—` | — |
| <a id="s-0f69aae951"></a>`bytes` | `BIGINT` | no | `—` | — |
| <a id="s-f4eae8af8f"></a>`sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-8e9588bd72"></a>`source_store` | `VARCHAR` | no | `—` | — |
| <a id="s-4b05d6272b"></a>`source_incarnation_id` | `VARCHAR(36)` | no | `—` | — |
| <a id="s-7fbb554fa1"></a>`requires_restore` | `BOOLEAN` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-41ec05c921"></a>`primary-key` | `—` | `PRIMARY KEY (plan_id, file_order)` |
| <a id="s-3080f32aac"></a>`foreign-key` | `—` | `FOREIGN KEY(source_incarnation_id, source_store) REFERENCES storage_incarnations (id, name)` |
| <a id="s-5833f4eb0b"></a>`foreign-key` | `—` | `FOREIGN KEY(plan_id) REFERENCES retrieval_plans (id) ON DELETE CASCADE` |
| <a id="s-aa27c23f66"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, path) REFERENCES collection_files (collection_id, path)` |
| <a id="s-e50cc0b2df"></a>`unique` | `—` | `UNIQUE (plan_id, collection_id, path)` |
| <a id="s-f5fb204bd6"></a>`check` | `ck_retrieval_plan_files_order` | `CONSTRAINT ck_retrieval_plan_files_order CHECK (file_order >= 0)` |
| <a id="s-24251e3321"></a>`check` | `ck_retrieval_plan_files_bytes` | `CONSTRAINT ck_retrieval_plan_files_bytes CHECK (bytes >= 0)` |
| <a id="s-a281875a0d"></a>`check` | `ck_retrieval_plan_files_sha256_hex` | `CONSTRAINT ck_retrieval_plan_files_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-c53f808117"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/69`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14d9afa074011083d8f9dd4f6b2c386371d2d1c259f18dfce1e221176a2a07ce -->

```json
{
  "columns": [
    {
      "definition": "plan_id VARCHAR NOT NULL",
      "name": "plan_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "file_order INTEGER NOT NULL",
      "name": "file_order",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "collection_id BIGINT NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "path VARCHAR NOT NULL",
      "name": "path",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "bytes BIGINT NOT NULL",
      "name": "bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "sha256 VARCHAR(64) NOT NULL",
      "name": "sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
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
      "definition": "requires_restore BOOLEAN NOT NULL",
      "name": "requires_restore",
      "nullable": false,
      "type": "BOOLEAN"
    }
  ],
  "constraints": [
    {
      "columns": [
        "plan_id",
        "file_order"
      ],
      "definition": "PRIMARY KEY (plan_id, file_order)",
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
        "plan_id"
      ],
      "definition": "FOREIGN KEY(plan_id) REFERENCES retrieval_plans (id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "id"
        ],
        "table": "retrieval_plans"
      }
    },
    {
      "columns": [
        "collection_id",
        "path"
      ],
      "definition": "FOREIGN KEY(collection_id, path) REFERENCES collection_files (collection_id, path)",
      "kind": "foreign-key",
      "references": {
        "columns": [
          "collection_id",
          "path"
        ],
        "table": "collection_files"
      }
    },
    {
      "columns": [
        "plan_id",
        "collection_id",
        "path"
      ],
      "definition": "UNIQUE (plan_id, collection_id, path)",
      "kind": "unique"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plan_files_order CHECK (file_order >= 0)",
      "expression": "(file_order >= 0)",
      "kind": "check",
      "name": "ck_retrieval_plan_files_order"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plan_files_bytes CHECK (bytes >= 0)",
      "expression": "(bytes >= 0)",
      "kind": "check",
      "name": "ck_retrieval_plan_files_bytes"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plan_files_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_retrieval_plan_files_sha256_hex"
    }
  ],
  "name": "retrieval_plan_files"
}
```

</details>
