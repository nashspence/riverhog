# riverhog-catalog: retrieval_plan_objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-plan-objects:1cb2527450 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-e666abd22e"></a>

### Table: `retrieval_plan_objects`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-850d878517"></a>`plan_id` | `VARCHAR` | no | `—` | — |
| <a id="s-200a837398"></a>`object_order` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-07eb624d96"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-b803f5e5ba"></a>`source_store` | `VARCHAR` | no | `—` | — |
| <a id="s-156946567e"></a>`source_incarnation_id` | `VARCHAR(36)` | no | `—` | — |
| <a id="s-da6130dfb0"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-3c0493031e"></a>`kind` | `VARCHAR` | no | `—` | — |
| <a id="s-a7a6ed2d06"></a>`plaintext_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-cc4f875fa4"></a>`stored_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-472bef5b9d"></a>`sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-ec67acdb06"></a>`read_mode` | `VARCHAR` | no | `—` | — |
| <a id="s-458f18b1b6"></a>`cache_store` | `VARCHAR` | yes | `—` | — |
| <a id="s-071e2a0ef9"></a>`cache_incarnation_id` | `VARCHAR(36)` | yes | `—` | — |
| <a id="s-f3e7cbdbe2"></a>`retrieval_bytes` | `VARCHAR(64)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-0604a034db"></a>`primary-key` | `—` | `PRIMARY KEY (plan_id, object_order)` |
| <a id="s-112a664b68"></a>`foreign-key` | `—` | `FOREIGN KEY(source_incarnation_id, source_store) REFERENCES storage_incarnations (id, name)` |
| <a id="s-913026d94c"></a>`foreign-key` | `—` | `FOREIGN KEY(cache_incarnation_id, cache_store) REFERENCES storage_incarnations (id, name)` |
| <a id="s-d58b710bdc"></a>`foreign-key` | `—` | `FOREIGN KEY(plan_id) REFERENCES retrieval_plans (id) ON DELETE CASCADE` |
| <a id="s-0ebddc89e0"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, source_store, object_id) REFERENCES collection_archive_objects (collection_id, store, object_id)` |
| <a id="s-59e10cbcfa"></a>`unique` | `—` | `UNIQUE (plan_id, collection_id, source_store, object_id)` |
| <a id="s-2ec8f67567"></a>`check` | `ck_retrieval_plan_objects_kind` | `CONSTRAINT ck_retrieval_plan_objects_kind CHECK (kind IN ('pack','segment'))` |
| <a id="s-c0aeb610cf"></a>`check` | `ck_retrieval_plan_objects_read_mode` | `CONSTRAINT ck_retrieval_plan_objects_read_mode CHECK (read_mode IN ('immediate','restore_required','cache'))` |
| <a id="s-ccbb4ffdd2"></a>`check` | `ck_retrieval_plan_objects_plaintext` | `CONSTRAINT ck_retrieval_plan_objects_plaintext CHECK (plaintext_bytes >= 0)` |
| <a id="s-25d6e18834"></a>`check` | `ck_retrieval_plan_objects_stored` | `CONSTRAINT ck_retrieval_plan_objects_stored CHECK (stored_bytes > 0)` |
| <a id="s-946a758902"></a>`check` | `ck_retrieval_plan_objects_sha256_hex` | `CONSTRAINT ck_retrieval_plan_objects_sha256_hex CHECK (sha256 IS NULL OR length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-f3050c7b70"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/77`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b860751fb28a86e625038b29532bba080e0e4b6f724bb56bf26ba8b5afc79e2f -->

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
      "definition": "object_order VARCHAR(64) NOT NULL",
      "name": "object_order",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "collection_id BIGINT NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "BIGINT"
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
      "definition": "object_id VARCHAR NOT NULL",
      "name": "object_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "kind VARCHAR NOT NULL",
      "name": "kind",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "plaintext_bytes BIGINT NOT NULL",
      "name": "plaintext_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "stored_bytes BIGINT NOT NULL",
      "name": "stored_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "sha256 VARCHAR(64)",
      "name": "sha256",
      "nullable": true,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "read_mode VARCHAR NOT NULL",
      "name": "read_mode",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "cache_store VARCHAR",
      "name": "cache_store",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "cache_incarnation_id VARCHAR(36)",
      "name": "cache_incarnation_id",
      "nullable": true,
      "type": "VARCHAR(36)"
    },
    {
      "definition": "retrieval_bytes VARCHAR(64) NOT NULL",
      "name": "retrieval_bytes",
      "nullable": false,
      "type": "VARCHAR(64)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "plan_id",
        "object_order"
      ],
      "definition": "PRIMARY KEY (plan_id, object_order)",
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
        "cache_incarnation_id",
        "cache_store"
      ],
      "definition": "FOREIGN KEY(cache_incarnation_id, cache_store) REFERENCES storage_incarnations (id, name)",
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
        "source_store",
        "object_id"
      ],
      "definition": "FOREIGN KEY(collection_id, source_store, object_id) REFERENCES collection_archive_objects (collection_id, store, object_id)",
      "kind": "foreign-key",
      "references": {
        "columns": [
          "collection_id",
          "store",
          "object_id"
        ],
        "table": "collection_archive_objects"
      }
    },
    {
      "columns": [
        "plan_id",
        "collection_id",
        "source_store",
        "object_id"
      ],
      "definition": "UNIQUE (plan_id, collection_id, source_store, object_id)",
      "kind": "unique"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plan_objects_kind CHECK (kind IN ('pack','segment'))",
      "expression": "(kind IN ('pack','segment'))",
      "kind": "check",
      "name": "ck_retrieval_plan_objects_kind"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plan_objects_read_mode CHECK (read_mode IN ('immediate','restore_required','cache'))",
      "expression": "(read_mode IN ('immediate','restore_required','cache'))",
      "kind": "check",
      "name": "ck_retrieval_plan_objects_read_mode"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plan_objects_plaintext CHECK (plaintext_bytes >= 0)",
      "expression": "(plaintext_bytes >= 0)",
      "kind": "check",
      "name": "ck_retrieval_plan_objects_plaintext"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plan_objects_stored CHECK (stored_bytes > 0)",
      "expression": "(stored_bytes > 0)",
      "kind": "check",
      "name": "ck_retrieval_plan_objects_stored"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_plan_objects_sha256_hex CHECK (sha256 IS NULL OR length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(sha256 IS NULL OR length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_retrieval_plan_objects_sha256_hex"
    }
  ],
  "name": "retrieval_plan_objects"
}
```

</details>
