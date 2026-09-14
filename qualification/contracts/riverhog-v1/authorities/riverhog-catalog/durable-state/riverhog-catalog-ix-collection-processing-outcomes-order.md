# riverhog-catalog: ix_collection_processing_outcomes_order

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-ix-collection-processing-762180d213:aad32758b2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-74b005a8df"></a>
| Index fact | Value |
|---|---|
| `columns` | `["claim_id","outcome_order"]` |
| `definition` | `"CREATE UNIQUE INDEX ix_collection_processing_outcomes_order ON collection_processing_outcomes (claim_id, outcome_order)"` |
| `name` | `"ix_collection_processing_outcomes_order"` |
| `table` | `"collection_processing_outcomes"` |

## Maintained corroboration

### Related interface records

- [riverhog-catalog durable-state identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-a49ca06944"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/unique_indexes/4`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: db8640576be5517498b5113bb02d8a41fa4b3f68b00429772a08ac573cfa208f -->

```json
{
  "columns": [
    "claim_id",
    "outcome_order"
  ],
  "definition": "CREATE UNIQUE INDEX ix_collection_processing_outcomes_order ON collection_processing_outcomes (claim_id, outcome_order)",
  "name": "ix_collection_processing_outcomes_order",
  "table": "collection_processing_outcomes"
}
```
