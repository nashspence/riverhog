# piggity-local: desired_collection_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:piggity-local:piggity-local-desired-collection-tags:766744ce3d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity-local](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-9bafbbe862"></a>

### Table: `desired_collection_tags`

#### Columns

| Column | Type | Nullable | Default | Other constraints |
|---|---|---:|---|---|
| <a id="s-147e9f12e2"></a>`collection_id` | `INTEGER` | no | `—` | — |
| <a id="s-6ec282f7c8"></a>`tag` | `TEXT` | no | `—` | — |

#### Table constraints

| Kind | Name | Exact definition |
|---|---|---|
| <a id="s-8dc121fed7"></a>`primary-key` | `—` | `PRIMARY KEY (collection_id, tag)` |
| <a id="s-98c1d63f57"></a>`foreign-key` | `—` | `FOREIGN KEY(collection_id) REFERENCES desired_collections (collection_id) ON DELETE CASCADE` |

## Maintained corroboration

### Related interface records

- [Schema identity](piggity-local-durable-state-identity.md)

## Governing policies

- <a id="pa-bd7fe39c0e"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:piggity-local](../../../evidence/sources.md#src-f6a1289f67) — [reference/riverhog/applications/piggity/src/piggity/state\_migrations/v1\_ddl.py::SQLITE\_DDL](../../../../../../reference/riverhog/applications/piggity/src/piggity/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/1/structure/tables/3`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd2ce3b85df0acf783ef961a7cd5d609c3aca830349c97769dee4aaca0039b80 -->

```json
{
  "columns": [
    {
      "definition": "collection_id INTEGER NOT NULL",
      "name": "collection_id",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "definition": "tag TEXT NOT NULL",
      "name": "tag",
      "nullable": false,
      "type": "TEXT"
    }
  ],
  "constraints": [
    {
      "columns": [
        "collection_id",
        "tag"
      ],
      "definition": "PRIMARY KEY (collection_id, tag)",
      "kind": "primary-key"
    },
    {
      "columns": [
        "collection_id"
      ],
      "definition": "FOREIGN KEY(collection_id) REFERENCES desired_collections (collection_id) ON DELETE CASCADE",
      "kind": "foreign-key",
      "references": {
        "actions": "ON DELETE CASCADE",
        "columns": [
          "collection_id"
        ],
        "table": "desired_collections"
      }
    }
  ],
  "name": "desired_collection_tags"
}
```

</details>
