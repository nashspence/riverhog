# riverhog-catalog: collection_upload_provenance_validation_facts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-proven-3e5c83c955:45450e7e7c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-c79285cc95"></a>
- Table: `collection_upload_provenance_validation_facts`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-cb32636a0a"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-6441c394a3"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-af281d0fd2"></a>`kind` | `VARCHAR` | no | `—` | — |
| <a id="s-7501722f60"></a>`fact_key` | `VARCHAR` | no | `—` | — |
| <a id="s-74ec617806"></a>`value_json` | `TEXT` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-ec22ab1087"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, journal_id, kind, fact_key)` |
| <a id="s-6edd2ad74d"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, journal_id) REFERENCES collection_upload_provenance_journals (collection_id, journal_id) ON DELETE CASCADE` |
| <a id="s-271a5ac3f6"></a>`check` | `ck_upload_provenance_validation_fact_kind` | `CONSTRAINT ck_upload_provenance_validation_fact_kind CHECK (kind IN ('entry','agent','event','state','binding','entity','external-state'))` |

## Maintained corroboration

### Related interface records

- [riverhog-catalog durable-state identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-dd4ce16d60"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/65`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b14818c645e9430bf3fd3c92318cb910597b35152fdb6aaf402008bebc0b012e -->

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
      "definition": "kind VARCHAR NOT NULL",
      "name": "kind",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "fact_key VARCHAR NOT NULL",
      "name": "fact_key",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "value_json TEXT NOT NULL",
      "name": "value_json",
      "nullable": false,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "journal_id",
        "kind",
        "fact_key"
      ],
      "definition": "PRIMARY KEY (collection_id, journal_id, kind, fact_key)",
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
    },
    {
      "definition": "CONSTRAINT ck_upload_provenance_validation_fact_kind CHECK (kind IN ('entry','agent','event','state','binding','entity','external-state'))",
      "expression": "(kind IN ('entry','agent','event','state','binding','entity','external-state'))",
      "kind": "check",
      "name": "ck_upload_provenance_validation_fact_kind"
    }
  ],
  "name": "collection_upload_provenance_validation_facts"
}
```
