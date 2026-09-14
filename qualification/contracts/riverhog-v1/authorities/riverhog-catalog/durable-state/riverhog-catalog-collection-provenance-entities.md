# riverhog-catalog: collection_provenance_entities

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-provenance-entities:cb7cfc5b66 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-752449f3f7"></a>
- Table: `collection_provenance_entities`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1844fea9ca"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-a25c903146"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-7deb583524"></a>`entity_type` | `VARCHAR` | no | `—` | — |
| <a id="s-d9e917b22f"></a>`entity_id` | `VARCHAR` | no | `—` | — |
| <a id="s-6163934f46"></a>`entry_id` | `VARCHAR` | no | `—` | — |
| <a id="s-0cfbc54f10"></a>`document_json` | `TEXT` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-b5b1e0ce88"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, journal_id, entity_type, entity_id)` |
| <a id="s-1b54efaa44"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, journal_id) REFERENCES collection_provenance_journals (collection_id, journal_id) ON DELETE CASCADE` |

## Maintained corroboration

### Related interface records

- [riverhog-catalog durable-state identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-e2d006cbbe"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/48`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dfabcde85e075dc2eb932e7ef2319c7f597ecc108e15083c24876430f40df330 -->

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
        "collection_id",
        "journal_id"
      ],
      "definition": "FOREIGN KEY(collection_id, journal_id) REFERENCES collection_provenance_journals (collection_id, journal_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id",
          "journal_id"
        ],
        "table": "collection_provenance_journals"
      }
    }
  ],
  "name": "collection_provenance_entities"
}
```
