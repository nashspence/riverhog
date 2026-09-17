# riverhog-catalog: ix_processing_disposition_outputs_order

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-ix-processing-dispositio-f0bb3a584a:3b4bd57fc9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-catalog](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-bf44c50d66"></a>

| Index fact | Value |
|---|---|
| `columns` | `["claim_id","output_order"]` |
| `definition` | `"CREATE UNIQUE INDEX ix_processing_disposition_outputs_order ON collection_processing_disposition_outputs (claim_id, output_order)"` |
| `name` | `"ix_processing_disposition_outputs_order"` |
| `table` | `"collection_processing_disposition_outputs"` |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-catalog-durable-state-identity.md)

## Governing policies

- <a id="pa-f658d39af2"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-catalog](../../../evidence/sources.md#src-d8b4a14670) — [riverhog/src/riverhog\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../riverhog/src/riverhog_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/0/structure/unique_indexes/8`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b196efa73d4beec99162bbcc1e686edeb3fb7e41ecbef523631e7f5cbf91585f -->

```json
{
  "columns": [
    "claim_id",
    "output_order"
  ],
  "definition": "CREATE UNIQUE INDEX ix_processing_disposition_outputs_order ON collection_processing_disposition_outputs (claim_id, output_order)",
  "name": "ix_processing_disposition_outputs_order",
  "table": "collection_processing_disposition_outputs"
}
```

</details>
