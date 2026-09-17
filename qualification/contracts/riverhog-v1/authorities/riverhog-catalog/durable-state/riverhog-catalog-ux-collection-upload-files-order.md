# riverhog-catalog: ux_collection_upload_files_order

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-ux-collection-upload-files-order:0dd77269b2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-26733d2287"></a>

| Index fact | Value |
|---|---|
| `columns` | `["collection_id","file_order"]` |
| `definition` | `"CREATE UNIQUE INDEX ux_collection_upload_files_order ON collection_upload_files (collection_id, file_order)"` |
| `name` | `"ux_collection_upload_files_order"` |
| `table` | `"collection_upload_files"` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-6de2428404"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/unique_indexes/3`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 63b8e4237aeccbb74ea6f8124c79f0af5cd8cf101d870bd0e6d371cb720bce31 -->

```json
{
  "columns": [
    "collection_id",
    "file_order"
  ],
  "definition": "CREATE UNIQUE INDEX ux_collection_upload_files_order ON collection_upload_files (collection_id, file_order)",
  "name": "ux_collection_upload_files_order",
  "table": "collection_upload_files"
}
```

</details>
