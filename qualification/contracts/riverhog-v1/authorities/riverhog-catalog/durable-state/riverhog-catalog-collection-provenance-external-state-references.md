# riverhog-catalog: collection_provenance_external_state_references

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-provenance-ex-fbffbfe964:b38fc3fc7c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-b3ee13fb41"></a>

### Table: `collection_provenance_external_state_references`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-fcdf80f3e2"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-6e4b7c9414"></a>`from_journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-05e24eb8bc"></a>`to_journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-792fc35a57"></a>`entry_id` | `VARCHAR` | no | `—` | — |
| <a id="s-a70bdfdac4"></a>`state_id` | `VARCHAR` | no | `—` | — |
| <a id="s-7dde9e1ffd"></a>`entry_json_sha256` | `VARCHAR(64)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-166cd00937"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, from_journal_id, to_journal_id, entry_id, state_id)` |
| <a id="s-0c8b5c98c2"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, from_journal_id) REFERENCES collection_provenance_journals (collection_id, journal_id) ON DELETE CASCADE` |
| <a id="s-4ad1c6c286"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, to_journal_id) REFERENCES collection_provenance_journals (collection_id, journal_id) ON DELETE CASCADE` |
| <a id="s-fb64a14dd9"></a>`check` | `ck_sha256_f8b89f54b8ad6ccb` | `CONSTRAINT ck_sha256_f8b89f54b8ad6ccb CHECK (length(entry_json_sha256) = 64 AND lower(entry_json_sha256) = entry_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(entry_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-4f60591b33"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/51`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4bcaa7cb28e7e961c7acd88179975a73b681a98041007f132790e110119791aa -->

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
      "definition": "from_journal_id VARCHAR NOT NULL",
      "name": "from_journal_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "to_journal_id VARCHAR NOT NULL",
      "name": "to_journal_id",
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
      "definition": "state_id VARCHAR NOT NULL",
      "name": "state_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "entry_json_sha256 VARCHAR(64) NOT NULL",
      "name": "entry_json_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "from_journal_id",
        "to_journal_id",
        "entry_id",
        "state_id"
      ],
      "definition": "PRIMARY KEY (collection_id, from_journal_id, to_journal_id, entry_id, state_id)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id",
        "from_journal_id"
      ],
      "definition": "FOREIGN KEY(collection_id, from_journal_id) REFERENCES collection_provenance_journals (collection_id, journal_id) ON DELETE CASCADE",
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
      "columns": [
        "collection_id",
        "to_journal_id"
      ],
      "definition": "FOREIGN KEY(collection_id, to_journal_id) REFERENCES collection_provenance_journals (collection_id, journal_id) ON DELETE CASCADE",
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
      "definition": "CONSTRAINT ck_sha256_f8b89f54b8ad6ccb CHECK (length(entry_json_sha256) = 64 AND lower(entry_json_sha256) = entry_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(entry_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(entry_json_sha256) = 64 AND lower(entry_json_sha256) = entry_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(entry_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_f8b89f54b8ad6ccb"
    }
  ],
  "name": "collection_provenance_external_state_references"
}
```

</details>
