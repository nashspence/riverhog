# riverhog-catalog: ix_processing_dispositions_order

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-ix-processing-dispositions-order:f857138da0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-c522473ca5"></a>

| Index fact | Value |
|---|---|
| `columns` | `["claim_id","disposition_order"]` |
| `definition` | `"CREATE UNIQUE INDEX ix_processing_dispositions_order ON collection_processing_dispositions (claim_id, disposition_order)"` |
| `name` | `"ix_processing_dispositions_order"` |
| `table` | `"collection_processing_dispositions"` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-2cc13d70bb"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — `riverhog/src/riverhog_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/0/structure/unique_indexes/7`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0e54b3f5fb83d158424b98cbcd142ffc831028a09a1746d145d79f71899a452 -->

```json
{
  "columns": [
    "claim_id",
    "disposition_order"
  ],
  "definition": "CREATE UNIQUE INDEX ix_processing_dispositions_order ON collection_processing_dispositions (claim_id, disposition_order)",
  "name": "ix_processing_dispositions_order",
  "table": "collection_processing_dispositions"
}
```

</details>
