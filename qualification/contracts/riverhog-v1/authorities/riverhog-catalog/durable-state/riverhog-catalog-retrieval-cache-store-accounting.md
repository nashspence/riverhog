# riverhog-catalog: retrieval_cache_store_accounting

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-cache-store-accounting:646a4219ae -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-f43be24d3d"></a>

### Table: `retrieval_cache_store_accounting`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-b86e579b0a"></a>`cache_store` | `VARCHAR` | no | `—` | — |
| <a id="s-36749376fd"></a>`cache_incarnation_id` | `VARCHAR(36)` | no | `—` | — |
| <a id="s-47ee498a3d"></a>`reserved_bytes` | `BIGINT` | no | `0` | — |
| <a id="s-d6b73a2206"></a>`committed_bytes` | `BIGINT` | no | `0` | — |
| <a id="s-130a9f5caf"></a>`generation` | `BIGINT` | no | `0` | — |
| <a id="s-3ad1c99d8e"></a>`updated_at` | `VARCHAR` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-92e3b4b53c"></a>`primary-key` | `—` | `PRIMARY KEY (cache_store)` |
| <a id="s-a87ef7b851"></a>`foreign-key` | `—` | `FOREIGN KEY(cache_incarnation_id, cache_store) REFERENCES storage_incarnations (id, name)` |
| <a id="s-711642855e"></a>`check` | `ck_retrieval_cache_store_accounting_reserved` | `CONSTRAINT ck_retrieval_cache_store_accounting_reserved CHECK (reserved_bytes >= 0)` |
| <a id="s-ef575f5074"></a>`check` | `ck_retrieval_cache_store_accounting_committed` | `CONSTRAINT ck_retrieval_cache_store_accounting_committed CHECK (committed_bytes >= 0)` |
| <a id="s-892ef661e3"></a>`check` | `ck_retrieval_cache_store_accounting_generation` | `CONSTRAINT ck_retrieval_cache_store_accounting_generation CHECK (generation >= 0)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-0d7acc59f2"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/16`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0abfeb6482add1b221bff6fc17e4cfd9750c9e592fabc106f5212a16268dac5f -->

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
      "definition": "cache_incarnation_id VARCHAR(36) NOT NULL",
      "name": "cache_incarnation_id",
      "nullable": false,
      "type": "VARCHAR(36)"
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

</details>
