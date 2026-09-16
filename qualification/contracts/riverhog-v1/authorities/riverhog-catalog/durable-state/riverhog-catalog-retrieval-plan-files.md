# riverhog-catalog: retrieval_plan_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-plan-files:00b01ee16a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-6c12389cec"></a>

### Table: `retrieval_plan_files`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1e83356b20"></a>`plan_id` | `VARCHAR` | no | `—` | — |
| <a id="s-4e108b72da"></a>`file_order` | `INTEGER` | no | `—` | — |
| <a id="s-64c0168df7"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-0a8fb37692"></a>`path` | `VARCHAR` | no | `—` | — |
| <a id="s-d79dd045ab"></a>`bytes` | `BIGINT` | no | `—` | — |
| <a id="s-076955ac8a"></a>`sha256` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-f12cd9cbdc"></a>`source_store` | `VARCHAR` | no | `—` | — |
| <a id="s-530927b08e"></a>`requires_restore` | `BOOLEAN` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-ce4246d144"></a>`primary-key` | `—` | `PRIMARY KEY (plan_id, file_order)` |
| <a id="s-dc42421300"></a>`foreign-key` | `—` | `FOREIGN KEY(plan_id) REFERENCES retrieval_plans (id) ON DELETE CASCADE` |
| <a id="s-70678ad695"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, path) REFERENCES collection_files (collection_id, path)` |
| <a id="s-52beae72e1"></a>`unique` | `—` | `UNIQUE (plan_id, collection_id, path)` |
| <a id="s-4022d650d0"></a>`check` | `ck_retrieval_plan_files_order` | `CONSTRAINT ck_retrieval_plan_files_order CHECK (file_order >= 0)` |
| <a id="s-d680e923c9"></a>`check` | `ck_retrieval_plan_files_bytes` | `CONSTRAINT ck_retrieval_plan_files_bytes CHECK (bytes >= 0)` |
| <a id="s-2dbe05c489"></a>`check` | `ck_retrieval_plan_files_sha256_hex` | `CONSTRAINT ck_retrieval_plan_files_sha256_hex CHECK (length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-fdb19cc8b6"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/67`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6dd188201204dcc13c661b4dcf2059babddaaa35766a7702d94af356ad963013 -->

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
