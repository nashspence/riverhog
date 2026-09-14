# riverhog-catalog: ux_collection_archive_object_uploads_sequence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-ux-collection-archive-ob-5df9d4fd7b:3c2408b779 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-762029a242"></a>
| Index fact | Value |
|---|---|
| `columns` | `["collection_id","sequence"]` |
| `definition` | `"CREATE UNIQUE INDEX ux_collection_archive_object_uploads_sequence ON collection_archive_object_uploads (collection_id, sequence)"` |
| `name` | `"ux_collection_archive_object_uploads_sequence"` |
| `table` | `"collection_archive_object_uploads"` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-f8651ed18a"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/unique_indexes/2`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0edb26cd8a4749878addaa61a327ff40d023bcfb5d89460128485b6b1b10f05b -->

```json
{
  "columns": [
    "collection_id",
    "sequence"
  ],
  "definition": "CREATE UNIQUE INDEX ux_collection_archive_object_uploads_sequence ON collection_archive_object_uploads (collection_id, sequence)",
  "name": "ux_collection_archive_object_uploads_sequence",
  "table": "collection_archive_object_uploads"
}
```
