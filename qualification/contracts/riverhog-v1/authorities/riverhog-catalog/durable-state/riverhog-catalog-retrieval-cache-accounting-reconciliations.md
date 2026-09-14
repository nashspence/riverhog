# riverhog-catalog: retrieval_cache_accounting_reconciliations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-cache-accounti-b67d40a5ef:55e6838f5c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-b88b0fdc06"></a>
- Table: `retrieval_cache_accounting_reconciliations`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-6f2fd4b89b"></a>`cache_store` | `VARCHAR` | no | `—` | — |
| <a id="s-f844582a96"></a>`generation` | `BIGINT` | no | `—` | — |
| <a id="s-2450f8b7e5"></a>`after_source_store` | `VARCHAR` | yes | `—` | — |
| <a id="s-78564f81ec"></a>`after_collection_id` | `BIGINT` | yes | `—` | — |
| <a id="s-9e0a50ed9b"></a>`after_object_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-a9e30039de"></a>`accumulated_bytes` | `BIGINT` | no | `0` | — |
| <a id="s-ebb79dc128"></a>`started_at` | `VARCHAR` | no | `—` | — |
| <a id="s-dd60e6a558"></a>`updated_at` | `VARCHAR` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-c179159514"></a>`primary-key` | `—` | `PRIMARY KEY (cache_store)` |
| <a id="s-723775cae8"></a>`foreign-key` | `—` | `FOREIGN KEY(cache_store) REFERENCES retrieval_cache_store_accounting (cache_store) ON DELETE CASCADE` |
| <a id="s-3f69376baf"></a>`check` | `ck_cache_accounting_reconciliations_generation` | `CONSTRAINT ck_cache_accounting_reconciliations_generation CHECK (generation >= 0)` |
| <a id="s-d076971862"></a>`check` | `ck_cache_accounting_reconciliations_bytes` | `CONSTRAINT ck_cache_accounting_reconciliations_bytes CHECK (accumulated_bytes >= 0)` |

## Maintained corroboration

### Related interface records

- [riverhog-catalog durable-state identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-c9695a76bc"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/36`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89baa9b06db91c43c8ef117e8e83b63623cfa1ee7116176b43f8c44737345d37 -->

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
      "definition": "generation BIGINT NOT NULL",
      "name": "generation",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "after_source_store VARCHAR",
      "name": "after_source_store",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "after_collection_id BIGINT",
      "name": "after_collection_id",
      "nullable": true,
      "type": "BIGINT"
    },
    {
      "definition": "after_object_id VARCHAR",
      "name": "after_object_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "default": "0",
      "definition": "accumulated_bytes BIGINT DEFAULT 0 NOT NULL",
      "name": "accumulated_bytes",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "started_at VARCHAR NOT NULL",
      "name": "started_at",
      "nullable": false,
      "type": "VARCHAR"
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
        "cache_store"
      ],
      "definition": "FOREIGN KEY(cache_store) REFERENCES retrieval_cache_store_accounting (cache_store) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "cache_store"
        ],
        "table": "retrieval_cache_store_accounting"
      }
    },
    {
      "definition": "CONSTRAINT ck_cache_accounting_reconciliations_generation CHECK (generation >= 0)",
      "expression": "(generation >= 0)",
      "kind": "check",
      "name": "ck_cache_accounting_reconciliations_generation"
    },
    {
      "definition": "CONSTRAINT ck_cache_accounting_reconciliations_bytes CHECK (accumulated_bytes >= 0)",
      "expression": "(accumulated_bytes >= 0)",
      "kind": "check",
      "name": "ck_cache_accounting_reconciliations_bytes"
    }
  ],
  "name": "retrieval_cache_accounting_reconciliations"
}
```
