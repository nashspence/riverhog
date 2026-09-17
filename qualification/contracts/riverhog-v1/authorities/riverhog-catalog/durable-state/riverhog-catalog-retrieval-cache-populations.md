# riverhog-catalog: retrieval_cache_populations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-cache-populations:5ab855a602 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-a328098dd8"></a>

### Table: `retrieval_cache_populations`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-ea2d86c292"></a>`source_store` | `VARCHAR` | no | `—` | — |
| <a id="s-abbcfaf9f0"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-00c0d65d2d"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-6d60d395d1"></a>`cache_store` | `VARCHAR` | yes | `—` | — |
| <a id="s-53de699040"></a>`object_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-f17aad4d06"></a>`write_token` | `VARCHAR` | yes | `—` | — |
| <a id="s-eda0b3f43b"></a>`expected_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-eeb324aaac"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-298111f4d3"></a>`initiated_at` | `VARCHAR` | no | `—` | — |
| <a id="s-1439415982"></a>`updated_at` | `VARCHAR` | no | `—` | — |
| <a id="s-2e9ccace54"></a>`failure` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-1f43d8b600"></a>`primary-key` | `—` | `PRIMARY KEY (source_store, collection_id, object_id)` |
| <a id="s-baf42373de"></a>`check` | `ck_retrieval_cache_populations_expected_bytes` | `CONSTRAINT ck_retrieval_cache_populations_expected_bytes CHECK (expected_bytes >= 1)` |
| <a id="s-32c30a6343"></a>`check` | `ck_retrieval_cache_populations_state` | `CONSTRAINT ck_retrieval_cache_populations_state CHECK (state IN ('waiting','admitting','admitted','writing','abandoning'))` |
| <a id="s-28103f5224"></a>`check` | `ck_retrieval_cache_populations_session` | `CONSTRAINT ck_retrieval_cache_populations_session CHECK (cache_store IS NULL AND object_path IS NULL AND write_token IS NULL AND state IN ('waiting','abandoning') OR cache_store IS NOT NULL AND object_path IS NOT NULL AND (write_token IS NULL AND state = 'admitting' OR write_token IS NOT NULL AND state IN ('admitted','writing') OR state = 'abandoning'))` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-4a14133e09"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/13`

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
