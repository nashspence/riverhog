# riverhog-catalog: retrieval_plan_objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-plan-objects:fa721cae1d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-537eab488a"></a>

### Table: `retrieval_plan_objects`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-f64ca87e75"></a>`plan_id` | `VARCHAR` | no | `—` | — |
| <a id="s-59951e13aa"></a>`object_order` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-316ac23f8a"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-962e1b65b6"></a>`source_store` | `VARCHAR` | no | `—` | — |
| <a id="s-24382c7188"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-a2f61cec1b"></a>`kind` | `VARCHAR` | no | `—` | — |
| <a id="s-1619bd11de"></a>`plaintext_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-86e2a393f7"></a>`stored_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-3a78da1dd4"></a>`sha256` | `VARCHAR(64)` | yes | `—` | — |
| <a id="s-691bdd9d42"></a>`read_mode` | `VARCHAR` | no | `—` | — |
| <a id="s-4e7f3c6a41"></a>`cache_store` | `VARCHAR` | yes | `—` | — |
| <a id="s-10791c28e6"></a>`retrieval_bytes` | `VARCHAR(64)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-c68f809359"></a>`primary-key` | `—` | `PRIMARY KEY (plan_id, object_order)` |
| <a id="s-593e9abc12"></a>`foreign-key` | `—` | `FOREIGN KEY(plan_id) REFERENCES retrieval_plans (id) ON DELETE CASCADE` |
| <a id="s-7e3ab1913e"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, source_store, object_id) REFERENCES collection_archive_objects (collection_id, store, object_id)` |
| <a id="s-abf290f916"></a>`unique` | `—` | `UNIQUE (plan_id, collection_id, source_store, object_id)` |
| <a id="s-29c5088f4f"></a>`check` | `ck_retrieval_plan_objects_kind` | `CONSTRAINT ck_retrieval_plan_objects_kind CHECK (kind IN ('pack','segment'))` |
| <a id="s-c233e465c4"></a>`check` | `ck_retrieval_plan_objects_read_mode` | `CONSTRAINT ck_retrieval_plan_objects_read_mode CHECK (read_mode IN ('immediate','restore_required','cache'))` |
| <a id="s-49f60232d8"></a>`check` | `ck_retrieval_plan_objects_plaintext` | `CONSTRAINT ck_retrieval_plan_objects_plaintext CHECK (plaintext_bytes >= 0)` |
| <a id="s-3164d23c5a"></a>`check` | `ck_retrieval_plan_objects_stored` | `CONSTRAINT ck_retrieval_plan_objects_stored CHECK (stored_bytes > 0)` |
| <a id="s-5218f7b06e"></a>`check` | `ck_retrieval_plan_objects_sha256_hex` | `CONSTRAINT ck_retrieval_plan_objects_sha256_hex CHECK (sha256 IS NULL OR length(sha256) = 64 AND lower(sha256) = sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-f792fa0595"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/76`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e716bdde9689e8403f9863ace8b92d72eebe8b41abdaea761fdb015fc5e933b7 -->

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
