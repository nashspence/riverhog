# riverhog-catalog: retrieval_cache_populations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-cache-populations:3c47070936 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-5367ff929e"></a>

### Table: `retrieval_cache_populations`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-8c8a1721c6"></a>`source_store` | `VARCHAR` | no | `—` | — |
| <a id="s-bd76206005"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-ab21e4863b"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-0c48fe4f11"></a>`cache_store` | `VARCHAR` | yes | `—` | — |
| <a id="s-dced5fb5ac"></a>`object_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-a4d897bdd3"></a>`write_token` | `VARCHAR` | yes | `—` | — |
| <a id="s-cbf7c84e21"></a>`expected_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-5126703673"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-678c28d4d9"></a>`initiated_at` | `VARCHAR` | no | `—` | — |
| <a id="s-f99a7e8c27"></a>`updated_at` | `VARCHAR` | no | `—` | — |
| <a id="s-eee6902c70"></a>`failure` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-a65fc8e13e"></a>`primary-key` | `—` | `PRIMARY KEY (source_store, collection_id, object_id)` |
| <a id="s-da7808fbdc"></a>`check` | `ck_retrieval_cache_populations_expected_bytes` | `CONSTRAINT ck_retrieval_cache_populations_expected_bytes CHECK (expected_bytes >= 1)` |
| <a id="s-dd493bfebe"></a>`check` | `ck_retrieval_cache_populations_state` | `CONSTRAINT ck_retrieval_cache_populations_state CHECK (state IN ('waiting','admitting','admitted','writing','abandoning'))` |
| <a id="s-45e4776d06"></a>`check` | `ck_retrieval_cache_populations_session` | `CONSTRAINT ck_retrieval_cache_populations_session CHECK (cache_store IS NULL AND object_path IS NULL AND write_token IS NULL AND state IN ('waiting','abandoning') OR cache_store IS NOT NULL AND object_path IS NOT NULL AND (write_token IS NULL AND state = 'admitting' OR write_token IS NOT NULL AND state IN ('admitted','writing') OR state = 'abandoning'))` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-b7784cbca8"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/14`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3657ea485cc7c5762c66a684ae616093b4dd77c3e0f377e272259983ff12d4e6 -->

```json
{
  "columns": [
    {
      "definition": "source_store VARCHAR NOT NULL",
      "name": "source_store",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "collection_id BIGINT NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "object_id VARCHAR NOT NULL",
      "name": "object_id",
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
      "definition": "object_path VARCHAR",
      "name": "object_path",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "write_token VARCHAR",
      "name": "write_token",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "expected_bytes BIGINT NOT NULL",
      "name": "expected_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "state VARCHAR NOT NULL",
      "name": "state",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "initiated_at VARCHAR NOT NULL",
      "name": "initiated_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "updated_at VARCHAR NOT NULL",
      "name": "updated_at",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "failure TEXT",
      "name": "failure",
      "nullable": true,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "source_store",
        "collection_id",
        "object_id"
      ],
      "definition": "PRIMARY KEY (source_store, collection_id, object_id)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_cache_populations_expected_bytes CHECK (expected_bytes >= 1)",
      "expression": "(expected_bytes >= 1)",
      "kind": "check",
      "name": "ck_retrieval_cache_populations_expected_bytes"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_cache_populations_state CHECK (state IN ('waiting','admitting','admitted','writing','abandoning'))",
      "expression": "(state IN ('waiting','admitting','admitted','writing','abandoning'))",
      "kind": "check",
      "name": "ck_retrieval_cache_populations_state"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_cache_populations_session CHECK (cache_store IS NULL AND object_path IS NULL AND write_token IS NULL AND state IN ('waiting','abandoning') OR cache_store IS NOT NULL AND object_path IS NOT NULL AND (write_token IS NULL AND state = 'admitting' OR write_token IS NOT NULL AND state IN ('admitted','writing') OR state = 'abandoning'))",
      "expression": "(cache_store IS NULL AND object_path IS NULL AND write_token IS NULL AND state IN ('waiting','abandoning') OR cache_store IS NOT NULL AND object_path IS NOT NULL AND (write_token IS NULL AND state = 'admitting' OR write_token IS NOT NULL AND state IN ('admitted','writing') OR state = 'abandoning'))",
      "kind": "check",
      "name": "ck_retrieval_cache_populations_session"
    }
  ],
  "name": "retrieval_cache_populations"
}
```

</details>
