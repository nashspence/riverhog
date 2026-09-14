# riverhog-catalog: collection_provenance_verification_external_states

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-provenance-ve-ef3d32a52e:fdd910659e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-b88d9bcea4"></a>
- Table: `collection_provenance_verification_external_states`

### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-177b291522"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-aad9a6b897"></a>`from_journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-ab515c1989"></a>`to_journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-5e639700b6"></a>`entry_id` | `VARCHAR` | no | `—` | — |
| <a id="s-ddc11ba90e"></a>`state_id` | `VARCHAR` | no | `—` | — |
| <a id="s-cd1333726f"></a>`entry_json_sha256` | `VARCHAR(64)` | no | `—` | — |

### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-3b04e35b7b"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, from_journal_id, to_journal_id, entry_id, state_id)` |
| <a id="s-ed86862a2a"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_provenance_verifications (collection_id) ON DELETE CASCADE` |
| <a id="s-ed9123c208"></a>`check` | `ck_sha256_5bae1ac3001e4f72` | `CONSTRAINT ck_sha256_5bae1ac3001e4f72 CHECK (length(entry_json_sha256) = 64 AND lower(entry_json_sha256) = entry_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(entry_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-67a9466ca7"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/55`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39e229d7f3ce9348383d42c0c3686729016cd60d65e45f37843d50f806e90003 -->

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
    },
    {
      "definition": "CONSTRAINT ck_sha256_5bae1ac3001e4f72 CHECK (length(entry_json_sha256) = 64 AND lower(entry_json_sha256) = entry_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(entry_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(entry_json_sha256) = 64 AND lower(entry_json_sha256) = entry_json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(entry_json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_5bae1ac3001e4f72"
    }
  ],
  "name": "collection_provenance_verification_external_states"
}
```
