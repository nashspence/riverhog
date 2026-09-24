# riverhog-catalog: ix_collection_processing_capability_artifacts_order

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-ix-collection-processing-dbccd60a4b:21edef3aca -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-16d9a639a8"></a>

| Index fact | Value |
|---|---|
| `columns` | `["capability_id","artifact_order"]` |
| `definition` | `"CREATE UNIQUE INDEX ix_collection_processing_capability_artifacts_order ON collection_processing_capability_artifacts (capability_id, artifact_order)"` |
| `name` | `"ix_collection_processing_capability_artifacts_order"` |
| `table` | `"collection_processing_capability_artifacts"` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-31e955ea05"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources/authorities.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/unique_indexes/6`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 50323a434f62c32a0868a79f3c2bc98e502535a590cdc0d404951ae804ca7ab0 -->

```json
{
  "columns": [
    "capability_id",
    "artifact_order"
  ],
  "definition": "CREATE UNIQUE INDEX ix_collection_processing_capability_artifacts_order ON collection_processing_capability_artifacts (capability_id, artifact_order)",
  "name": "ix_collection_processing_capability_artifacts_order",
  "table": "collection_processing_capability_artifacts"
}
```

</details>
