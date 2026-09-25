# riverhog-catalog: retrieval_cache_populations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-retrieval-cache-populations:e4aaee9324 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-a2c14f3731"></a>

### Table: `retrieval_cache_populations`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-4746cd74e6"></a>`source_store` | `VARCHAR` | no | `—` | — |
| <a id="s-ab1db00bfb"></a>`source_incarnation_id` | `VARCHAR(36)` | no | `—` | — |
| <a id="s-75af01c0c0"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-1be2709ecf"></a>`object_id` | `VARCHAR` | no | `—` | — |
| <a id="s-d0add89604"></a>`cache_store` | `VARCHAR` | yes | `—` | — |
| <a id="s-05c5e1fa0f"></a>`cache_incarnation_id` | `VARCHAR(36)` | yes | `—` | — |
| <a id="s-fc5c0a24fc"></a>`object_path` | `VARCHAR` | yes | `—` | — |
| <a id="s-560d19365f"></a>`write_token` | `VARCHAR` | yes | `—` | — |
| <a id="s-35daadf574"></a>`expected_bytes` | `BIGINT` | no | `—` | — |
| <a id="s-02e2b60c11"></a>`state` | `VARCHAR` | no | `—` | — |
| <a id="s-488da60f6d"></a>`initiated_at` | `VARCHAR` | no | `—` | — |
| <a id="s-d816727e85"></a>`updated_at` | `VARCHAR` | no | `—` | — |
| <a id="s-51a07b362e"></a>`failure` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-1ada749bca"></a>`primary-key` | `—` | `PRIMARY KEY (source_store, collection_id, object_id)` |
| <a id="s-03ba25055d"></a>`foreign-key` | `—` | `FOREIGN KEY(source_incarnation_id, source_store) REFERENCES storage_incarnations (id, name)` |
| <a id="s-34f714e0dd"></a>`foreign-key` | `—` | `FOREIGN KEY(cache_incarnation_id, cache_store) REFERENCES storage_incarnations (id, name)` |
| <a id="s-161ec1b5d5"></a>`check` | `ck_retrieval_cache_populations_expected_bytes` | `CONSTRAINT ck_retrieval_cache_populations_expected_bytes CHECK (expected_bytes >= 1)` |
| <a id="s-588cdfc014"></a>`check` | `ck_retrieval_cache_populations_state` | `CONSTRAINT ck_retrieval_cache_populations_state CHECK (state IN ('waiting','admitting','admitted','writing','abandoning'))` |
| <a id="s-c8f2a5e0db"></a>`check` | `ck_retrieval_cache_populations_session` | `CONSTRAINT ck_retrieval_cache_populations_session CHECK (cache_store IS NULL AND object_path IS NULL AND write_token IS NULL AND state IN ('waiting','abandoning') OR cache_store IS NOT NULL AND object_path IS NOT NULL AND (write_token IS NULL AND state = 'admitting' OR write_token IS NOT NULL AND state IN ('admitted','writing') OR state = 'abandoning'))` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-7a2a5cc932"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/15`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8eb32a2d55d39561be64c13b1991afcc9d72cddba87cb73fe9cf6066ea0cfc2a -->

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
      "definition": "source_incarnation_id VARCHAR(36) NOT NULL",
      "name": "source_incarnation_id",
      "nullable": false,
      "type": "VARCHAR(36)"
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
      "definition": "cache_incarnation_id VARCHAR(36)",
      "name": "cache_incarnation_id",
      "nullable": true,
      "type": "VARCHAR(36)"
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
