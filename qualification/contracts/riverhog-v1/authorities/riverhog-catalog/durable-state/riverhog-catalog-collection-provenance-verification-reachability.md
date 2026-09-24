# riverhog-catalog: collection_provenance_verification_reachability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-provenance-ve-249584597d:5edb554858 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-e603651366"></a>

### Table: `collection_provenance_verification_reachability`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-256aa570c4"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-680c6c8297"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-398eaa9b42"></a>`expanded` | `BOOLEAN` | no | `false` | — |
| <a id="s-3518585307"></a>`after_to_journal_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-aab5b57aa1"></a>`after_entry_id` | `VARCHAR` | yes | `—` | — |
| <a id="s-250d746ecd"></a>`after_state_id` | `VARCHAR` | yes | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-502b238b80"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, journal_id)` |
| <a id="s-b2f90327c3"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES collection_provenance_verifications (collection_id) ON DELETE CASCADE` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-d674316ba9"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/57`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a4647e5cb360bb087d49c1b68f666c47282e5408a523970126618265007fdc9 -->

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
      "default": "false",
      "definition": "expanded BOOLEAN DEFAULT false NOT NULL",
      "name": "expanded",
      "nullable": false,
      "type": "BOOLEAN"
    },
    {
      "definition": "after_to_journal_id VARCHAR",
      "name": "after_to_journal_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "after_entry_id VARCHAR",
      "name": "after_entry_id",
      "nullable": true,
      "type": "VARCHAR"
    },
    {
      "definition": "after_state_id VARCHAR",
      "name": "after_state_id",
      "nullable": true,
      "type": "VARCHAR"
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
    }
  ],
  "name": "collection_provenance_verification_reachability"
}
```

</details>
