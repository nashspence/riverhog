# riverhog-catalog: ix_collection_transform_capability_artifacts_order

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-ix-collection-transform-6fdd89b38d:edc2f43551 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-16d9a639a8"></a>
| Index fact | Value |
|---|---|
| `columns` | `["capability_id","artifact_order"]` |
| `definition` | `"CREATE UNIQUE INDEX ix_collection_transform_capability_artifacts_order ON collection_transform_capability_artifacts (capability_id, artifact_order)"` |
| `name` | `"ix_collection_transform_capability_artifacts_order"` |
| `table` | `"collection_transform_capability_artifacts"` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-47662b3342"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/unique_indexes/6`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 920245894a9481bbe5e50bd3dbf70643fd2932c9186b4267a22f3b5c652c26e7 -->

```json
{
  "columns": [
    "capability_id",
    "artifact_order"
  ],
  "definition": "CREATE UNIQUE INDEX ix_collection_transform_capability_artifacts_order ON collection_transform_capability_artifacts (capability_id, artifact_order)",
  "name": "ix_collection_transform_capability_artifacts_order",
  "table": "collection_transform_capability_artifacts"
}
```
