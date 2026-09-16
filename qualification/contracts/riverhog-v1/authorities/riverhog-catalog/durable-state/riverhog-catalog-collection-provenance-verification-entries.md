# riverhog-catalog: collection_provenance_verification_entries

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-provenance-ve-49fc46e2f5:b72fe3a9c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-6a734109c9"></a>

### Table: `collection_provenance_verification_entries`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-9bf7f7a1e2"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-474c1f9cd6"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-cad2a54c30"></a>`entry_id` | `VARCHAR` | no | `—` | — |
| <a id="s-303f8cd283"></a>`json_sha256` | `VARCHAR(64)` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-32c375db91"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, journal_id, entry_id)` |
| <a id="s-1b394ef3b0"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_provenance_verifications (collection_id) ON DELETE CASCADE` |
| <a id="s-772c88bbca"></a>`check` | `ck_sha256_10ab1519eb5bc179` | `CONSTRAINT ck_sha256_10ab1519eb5bc179 CHECK (length(json_sha256) = 64 AND lower(json_sha256) = json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-f51a7d71de"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/54`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 64927cf757f40705251cef6ce3743c6370f55afe7984a0b9d29d7e03db103b5c -->

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
      "definition": "entry_id VARCHAR NOT NULL",
      "name": "entry_id",
      "nullable": false,
      "type": "VARCHAR"
    },
    {
      "definition": "json_sha256 VARCHAR(64) NOT NULL",
      "name": "json_sha256",
      "nullable": false,
      "type": "VARCHAR(64)"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "journal_id",
        "entry_id"
      ],
      "definition": "PRIMARY KEY (collection_id, journal_id, entry_id)",
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
      "definition": "CONSTRAINT ck_sha256_10ab1519eb5bc179 CHECK (length(json_sha256) = 64 AND lower(json_sha256) = json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "expression": "(length(json_sha256) = 64 AND lower(json_sha256) = json_sha256 AND replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(json_sha256, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = '')",
      "kind": "check",
      "name": "ck_sha256_10ab1519eb5bc179"
    }
  ],
  "name": "collection_provenance_verification_entries"
}
```

</details>
