# riverhog-catalog: collection_provenance_verification_entities

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-provenance-ve-38316a8888:aad2dfd041 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-6a734109c9"></a>

### Table: `collection_provenance_verification_entities`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-9bf7f7a1e2"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-474c1f9cd6"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-cad2a54c30"></a>`entity_type` | `VARCHAR` | no | `—` | — |
| <a id="s-303f8cd283"></a>`entity_id` | `VARCHAR` | no | `—` | — |
| <a id="s-e09be07555"></a>`entry_id` | `VARCHAR` | no | `—` | — |
| <a id="s-bbd2c8342f"></a>`document_json` | `TEXT` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-32c375db91"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, journal_id, entity_type, entity_id)` |
| <a id="s-1b394ef3b0"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_provenance_verifications (collection_id) ON DELETE CASCADE` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-a67426c0f6"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/54`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 135ff3e830e022ae9dd4e308393e75fd755dfc8a0caa90da112a3a69d6b00f06 -->

```json
{
  "columns": [
    {
      "definition": "collection_id BIGINT NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "BIGINT"
    },
    {
      "definition": "journal_id VARCHAR NOT NULL",
      "name": "journal_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "entity_type VARCHAR NOT NULL",
      "name": "entity_type",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "entity_id VARCHAR NOT NULL",
      "name": "entity_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "entry_id VARCHAR NOT NULL",
      "name": "entry_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "document_json TEXT NOT NULL",
      "name": "document_json",
      "nullable": false,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "journal_id",
        "entity_type",
        "entity_id"
      ],
      "definition": "PRIMARY KEY (collection_id, journal_id, entity_type, entity_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id"
      ],
      "definition": "FOREIGN KEY(collection_id) REFERENCES collection_provenance_verifications (collection_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id"
        ],
        "table": "collection_provenance_verifications"
      }
    }
  ],
  "name": "collection_provenance_verification_entities"
}
```

</details>
