# riverhog-catalog: collection_file_provenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-file-provenance:1400044141 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-5135cc46d8"></a>
- Table: `collection_file_provenance`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-6dfd570257"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-4f8ba8a21c"></a>`path` | `VARCHAR` | no | `—` | — |
| <a id="s-e061d4aecb"></a>`status` | `VARCHAR` | no | `—` | — |
| <a id="s-5442036c1a"></a>`journal_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-ba869643a0"></a>`current_state_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-fc0510f0ef"></a>`omission_reason` | `TEXT` | yes | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-2f0fb38e63"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, path)` |
| <a id="s-fcf4c9fc58"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, path) REFERENCES collection_files (collection_id, path) ON DELETE CASCADE` |
| <a id="s-caa2e2b8fd"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, journal_id) REFERENCES collection_provenance_journals (collection_id, journal_id) ON DELETE CASCADE` |
| <a id="s-39108e3409"></a>`check` | `ck_collection_file_provenance_status` | `CONSTRAINT ck_collection_file_provenance_status CHECK (status IN ('captured','omitted'))` |
| <a id="s-3c88fa1677"></a>`check` | `ck_collection_file_provenance_binding` | `CONSTRAINT ck_collection_file_provenance_binding CHECK (status = 'captured' AND journal_id IS NOT NULL AND current_state_id IS NOT NULL AND omission_reason IS NULL OR status = 'omitted' AND journal_id IS NULL AND current_state_id IS NULL AND omission_reason IS NOT NULL)` |

## Maintained corroboration

### Related interface records

- [riverhog-catalog durable-state identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-3f4c72df2f"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/44`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91906974b320b2d6fc8ecc2594ce79862f28d80daf203b712a42fbf73e8a1b01 -->

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
      "definition": "path VARCHAR NOT NULL",
      "name": "path",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "status VARCHAR NOT NULL",
      "name": "status",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "journal_id VARCHAR",
      "name": "journal_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "current_state_id VARCHAR",
      "name": "current_state_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "omission_reason TEXT",
      "name": "omission_reason",
      "nullable": true,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "path"
      ],
      "definition": "PRIMARY KEY (collection_id, path)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id",
        "path"
      ],
      "definition": "FOREIGN KEY(collection_id, path) REFERENCES collection_files (collection_id, path) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id",
          "path"
        ],
        "table": "collection_files"
      }
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
    },
    {
      "definition": "CONSTRAINT ck_collection_file_provenance_status CHECK (status IN ('captured','omitted'))",
      "expression": "(status IN ('captured','omitted'))",
      "kind": "check",
      "name": "ck_collection_file_provenance_status"
    },
    {
      "definition": "CONSTRAINT ck_collection_file_provenance_binding CHECK (status = 'captured' AND journal_id IS NOT NULL AND current_state_id IS NOT NULL AND omission_reason IS NULL OR status = 'omitted' AND journal_id IS NULL AND current_state_id IS NULL AND omission_reason IS NOT NULL)",
      "expression": "(status = 'captured' AND journal_id IS NOT NULL AND current_state_id IS NOT NULL AND omission_reason IS NULL OR status = 'omitted' AND journal_id IS NULL AND current_state_id IS NULL AND omission_reason IS NOT NULL)",
      "kind": "check",
      "name": "ck_collection_file_provenance_binding"
    }
  ],
  "name": "collection_file_provenance"
}
```
