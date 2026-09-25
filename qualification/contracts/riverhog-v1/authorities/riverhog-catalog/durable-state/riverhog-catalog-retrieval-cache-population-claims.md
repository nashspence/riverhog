# riverhog-catalog: retrieval_cache_population_claims

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-cache-population-claims:b70f558a1b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-07d9a1b0cb"></a>

### Table: `retrieval_cache_population_claims`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-9942861055"></a>`owner` | `VARCHAR` | no | `—` | — |
| <a id="s-692751482a"></a>`source_store` | `VARCHAR` | no | `—` | — |
| <a id="s-639a288dcd"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-5e16f41caf"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-c86be3b2f0"></a>`created_at` | `VARCHAR` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-68f7abe07f"></a>`primary-key` | `—` | `PRIMARY KEY (owner, source_store, collection_id, object_id)` |
| <a id="s-95a164f0a5"></a>`foreign-key` | `—` | `FOREIGN KEY(source_store, collection_id, object_id) REFERENCES retrieval_cache_populations (source_store, collection_id, object_id) ON DELETE CASCADE` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-bddd9efc24"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/39`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8434b235ba520e3df170f7f3c11720658e0f73800cef128ca0a947d8c9bd768c -->

```json
{
  "columns": [
    {
      "definition": "owner VARCHAR NOT NULL",
      "name": "owner",
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
      "definition": "created_at VARCHAR NOT NULL",
      "name": "created_at",
      "nullable": false,
      "type": "VARCHAR"
    }
  ],
  "constraints": [
    {
      "columns": [
        "owner",
        "source_store",
        "collection_id",
        "object_id"
      ],
      "definition": "PRIMARY KEY (owner, source_store, collection_id, object_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "source_store",
        "collection_id",
        "object_id"
      ],
      "definition": "FOREIGN KEY(source_store, collection_id, object_id) REFERENCES retrieval_cache_populations (source_store, collection_id, object_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "source_store",
          "collection_id",
          "object_id"
        ],
        "table": "retrieval_cache_populations"
      }
    }
  ],
  "name": "retrieval_cache_population_claims"
}
```

</details>
