# stove0-control: ix_stove0_selection_members_continuation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-ix-stove0-selection-member-aa0c1263c9:19afebe997 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-fe5d89756e"></a>
| Index fact | Value |
|---|---|
| `columns` | `["selection_sha256","continuation_sha256"]` |
| `definition` | `"CREATE UNIQUE INDEX ix_stove0_selection_members_continuation ON stove0_artifact_selection_members (selection_sha256, continuation_sha256)"` |
| `name` | `"ix_stove0_selection_members_continuation"` |
| `table` | `"stove0_artifact_selection_members"` |

## Maintained corroboration

### Related interface records

- [stove0-control durable-state identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-32e00fce9f"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:stove0-control](../../../evidence/sources.md#src-45e44b17fd) — `reference/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/3/structure/unique_indexes/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 896ae9bb55a84adbfd2106b78c5877135dcd907087de5ef2f378632e813147c9 -->

```json
{
  "columns": [
    "selection_sha256",
    "continuation_sha256"
  ],
  "definition": "CREATE UNIQUE INDEX ix_stove0_selection_members_continuation ON stove0_artifact_selection_members (selection_sha256, continuation_sha256)",
  "name": "ix_stove0_selection_members_continuation",
  "table": "stove0_artifact_selection_members"
}
```
