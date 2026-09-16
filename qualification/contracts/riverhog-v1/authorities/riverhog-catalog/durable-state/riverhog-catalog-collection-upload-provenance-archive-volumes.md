# riverhog-catalog: collection_upload_provenance_archive_volumes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-proven-49eaa53a80:a9bdb31c85 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-1dab4d0f87"></a>

### Table: `collection_upload_provenance_archive_volumes`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-31df735f31"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-b90d2a8ab8"></a>`sequence` | `VARCHAR(64)` | no | `—` | — |
| <a id="s-629b7b318f"></a>`kind` | `VARCHAR` | no | `—` | — |
| <a id="s-17dab7c5b2"></a>`document_json` | `TEXT` | no | `—` | — |
| <a id="s-7636188887"></a>`payload_receipt_json` | `TEXT` | no | `—` | — |
| <a id="s-5d2f8f925b"></a>`metadata_receipt_json` | `TEXT` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-439b567377"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, sequence)` |
| <a id="s-8fb81ce7db"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_uploads (collection_id) ON DELETE CASCADE` |
| <a id="s-56bb0a08e7"></a>`check` | `ck_upload_provenance_archive_volumes_sequence` | `CONSTRAINT ck_upload_provenance_archive_volumes_sequence CHECK (length(sequence) = 64 AND lower(sequence) = sequence AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sequence, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)` |
| <a id="s-74c5a686c7"></a>`check` | `ck_upload_provenance_archive_volumes_kind` | `CONSTRAINT ck_upload_provenance_archive_volumes_kind CHECK (kind IN ('bindings','journal'))` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-66db23ce6d"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/29`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c7b5777f90e1be5b7b1fee080a6975b6e07853ca101aacdcfb8df377f63be161 -->

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
      "definition": "sequence VARCHAR(64) NOT NULL",
      "name": "sequence",
      "nullable": false,
      "type": "VARCHAR(64)"
    },
    {
      "definition": "kind VARCHAR NOT NULL",
      "name": "kind",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "document_json TEXT NOT NULL",
      "name": "document_json",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "payload_receipt_json TEXT NOT NULL",
      "name": "payload_receipt_json",
      "nullable": false,
      "type": "TEXT"
    },
    {
      "definition": "metadata_receipt_json TEXT NOT NULL",
      "name": "metadata_receipt_json",
      "nullable": false,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "sequence"
      ],
      "definition": "PRIMARY KEY (collection_id, sequence)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id"
      ],
      "definition": "FOREIGN KEY(collection_id) REFERENCES collection_uploads (collection_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id"
        ],
        "table": "collection_uploads"
      }
    },
    {
      "definition": "CONSTRAINT ck_upload_provenance_archive_volumes_sequence CHECK (length(sequence) = 64 AND lower(sequence) = sequence AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sequence, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "expression": "(length(sequence) = 64 AND lower(sequence) = sequence AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(sequence, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0)",
      "kind": "check",
      "name": "ck_upload_provenance_archive_volumes_sequence"
    },
    {
      "definition": "CONSTRAINT ck_upload_provenance_archive_volumes_kind CHECK (kind IN ('bindings','journal'))",
      "expression": "(kind IN ('bindings','journal'))",
      "kind": "check",
      "name": "ck_upload_provenance_archive_volumes_kind"
    }
  ],
  "name": "collection_upload_provenance_archive_volumes"
}
```

</details>
