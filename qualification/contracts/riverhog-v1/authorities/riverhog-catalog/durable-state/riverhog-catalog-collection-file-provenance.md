# riverhog-catalog: collection_file_provenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-file-provenance:f5dc5de065 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-cf59823348"></a>

### Table: `collection_file_provenance`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-4fe8090c32"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-0f291cd4e2"></a>`path` | `VARCHAR` | no | `—` | — |
| <a id="s-0f961b500a"></a>`status` | `VARCHAR` | no | `—` | — |
| <a id="s-23d2a36754"></a>`journal_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-c787b2f974"></a>`current_state_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-53c3fd3afc"></a>`omission_reason` | `TEXT` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-9927dc56fe"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, path)` |
| <a id="s-850043dd24"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, path) REFERENCES collection_files (collection_id, path) ON DELETE CASCADE` |
| <a id="s-68e5038c1f"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, journal_id) REFERENCES collection_provenance_journals (collection_id, journal_id) ON DELETE CASCADE` |
| <a id="s-d610feb878"></a>`check` | `ck_collection_file_provenance_status` | `CONSTRAINT ck_collection_file_provenance_status CHECK (status IN ('captured','omitted'))` |
| <a id="s-612721833c"></a>`check` | `ck_collection_file_provenance_binding` | `CONSTRAINT ck_collection_file_provenance_binding CHECK (status = 'captured' AND journal_id IS NOT NULL AND current_state_id IS NOT NULL AND omission_reason IS NULL OR status = 'omitted' AND journal_id IS NULL AND current_state_id IS NULL AND omission_reason IS NOT NULL)` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-3410f5c104"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/46`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
