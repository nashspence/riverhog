# riverhog-catalog: collection_upload_provenance_validation_facts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-proven-3e5c83c955:ae4fa7e866 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-6c12389cec"></a>

### Table: `collection_upload_provenance_validation_facts`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-1e83356b20"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-4e108b72da"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-64c0168df7"></a>`kind` | `VARCHAR` | no | `—` | — |
| <a id="s-0a8fb37692"></a>`fact_key` | `VARCHAR` | no | `—` | — |
| <a id="s-d79dd045ab"></a>`value_json` | `TEXT` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-ce4246d144"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, journal_id, kind, fact_key)` |
| <a id="s-dc42421300"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, journal_id) REFERENCES collection_upload_provenance_journals (collection_id, journal_id) ON DELETE CASCADE` |
| <a id="s-70678ad695"></a>`check` | `ck_upload_provenance_validation_fact_kind` | `CONSTRAINT ck_upload_provenance_validation_fact_kind CHECK (kind IN ('entry','agent','event','state','binding','entity','external-state'))` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-cf091ce936"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/67`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
