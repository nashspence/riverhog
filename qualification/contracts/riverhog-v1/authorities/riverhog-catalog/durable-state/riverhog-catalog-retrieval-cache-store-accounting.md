# riverhog-catalog: retrieval_cache_store_accounting

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-cache-store-accounting:7424a4249a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-5367ff929e"></a>
- Table: `retrieval_cache_store_accounting`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-8c8a1721c6"></a>`cache_store` | `VARCHAR` | no | `—` | — |
| <a id="s-bd76206005"></a>`reserved_bytes` | `BIGINT` | no | `0` | — |
| <a id="s-ab21e4863b"></a>`committed_bytes` | `BIGINT` | no | `0` | — |
| <a id="s-0c48fe4f11"></a>`generation` | `BIGINT` | no | `0` | — |
| <a id="s-dced5fb5ac"></a>`updated_at` | `VARCHAR` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-a65fc8e13e"></a>`primary-key` | `—` | `PRIMARY KEY (cache_store)` |
| <a id="s-da7808fbdc"></a>`check` | `ck_retrieval_cache_store_accounting_reserved` | `CONSTRAINT ck_retrieval_cache_store_accounting_reserved CHECK (reserved_bytes >= 0)` |
| <a id="s-dd493bfebe"></a>`check` | `ck_retrieval_cache_store_accounting_committed` | `CONSTRAINT ck_retrieval_cache_store_accounting_committed CHECK (committed_bytes >= 0)` |
| <a id="s-45e4776d06"></a>`check` | `ck_retrieval_cache_store_accounting_generation` | `CONSTRAINT ck_retrieval_cache_store_accounting_generation CHECK (generation >= 0)` |

## Maintained corroboration

### Related interface records

- [riverhog-catalog durable-state identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-02df56b4bd"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/14`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d92b9a871ccaed6aae395e7d06cdbdf7d89b4da2bfb2aa4c14d5a859e374e092 -->

```json
{
  "columns": [
    {
      "definition": "cache_store VARCHAR NOT NULL",
      "name": "cache_store",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "default": "0",
      "definition": "reserved_bytes BIGINT DEFAULT 0 NOT NULL",
      "name": "reserved_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "default": "0",
      "definition": "committed_bytes BIGINT DEFAULT 0 NOT NULL",
      "name": "committed_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "default": "0",
      "definition": "generation BIGINT DEFAULT 0 NOT NULL",
      "name": "generation",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "updated_at VARCHAR NOT NULL",
      "name": "updated_at",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "cache_store"
      ],
      "definition": "PRIMARY KEY (cache_store)",
      "kind": "primary-key"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_cache_store_accounting_reserved CHECK (reserved_bytes >= 0)",
      "expression": "(reserved_bytes >= 0)",
      "kind": "check",
      "name": "ck_retrieval_cache_store_accounting_reserved"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_cache_store_accounting_committed CHECK (committed_bytes >= 0)",
      "expression": "(committed_bytes >= 0)",
      "kind": "check",
      "name": "ck_retrieval_cache_store_accounting_committed"
    },
    {
      "definition": "CONSTRAINT ck_retrieval_cache_store_accounting_generation CHECK (generation >= 0)",
      "expression": "(generation >= 0)",
      "kind": "check",
      "name": "ck_retrieval_cache_store_accounting_generation"
    }
  ],
  "name": "retrieval_cache_store_accounting"
}
```
