# riverhog-catalog: collection_upload_provenance_reachability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-collection-upload-proven-4d44f80a4c:66dc205f05 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-d332239d07"></a>

### Table: `collection_upload_provenance_reachability`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-da0d3b8e3f"></a>`collection_id` | `BIGINT` | no | `—` | — |
| <a id="s-bf2d869b1b"></a>`journal_id` | `VARCHAR` | no | `—` | — |
| <a id="s-dc393254ef"></a>`after_external_fact_key` | `VARCHAR` | yes | `—` | — |
| <a id="s-6bc9e37048"></a>`expanded` | `BOOLEAN` | no | `false` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-0200030ffa"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, journal_id)` |
| <a id="s-a6fcd34690"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id, journal_id) REFERENCES collection_upload_provenance_journals (collection_id, journal_id) ON DELETE CASCADE` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-63b73f82c9"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/tables/64`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
