# riverhog-catalog: collection_upload_provenance_reachability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-proven-4d44f80a4c:0fbe94f20d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-e6abdf96d1"></a>
- Table: `collection_upload_provenance_reachability`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-20c7e3e7df"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-d3d91fea84"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-97155ab8ee"></a>`after_external_fact_key` | `VARCHAR` | yes | `—` | — |
| <a id="s-0c3c5db4f5"></a>`expanded` | `BOOLEAN` | no | `false` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-8bfc344ae4"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, journal_id)` |
| <a id="s-b589cf0895"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, journal_id) REFERENCES collection_upload_provenance_journals (collection_id, journal_id) ON DELETE CASCADE` |

## Maintained corroboration

### Related interface records

- [riverhog-catalog durable-state identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-c86a28904f"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/63`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 857a39912bf81709faaea29cbd1d3516e6475b6262fc28243a4be6ce17ca7386 -->

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
      "definition": "after_external_fact_key VARCHAR",
      "name": "after_external_fact_key",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "default": "false",
      "definition": "expanded BOOLEAN DEFAULT false NOT NULL",
      "name": "expanded",
      "nullable": false,
      "type": "BOOLEAN"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "journal_id"
      ],
      "definition": "PRIMARY KEY (collection_id, journal_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id",
        "journal_id"
      ],
      "definition": "FOREIGN KEY(collection_id, journal_id) REFERENCES collection_upload_provenance_journals (collection_id, journal_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id",
          "journal_id"
        ],
        "table": "collection_upload_provenance_journals"
      }
    }
  ],
  "name": "collection_upload_provenance_reachability"
}
```
