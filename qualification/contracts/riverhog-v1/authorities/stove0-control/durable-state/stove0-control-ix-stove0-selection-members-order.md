# stove0-control: ix_stove0_selection_members_order

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-ix-stove0-selection-members-order:a7588d5f5f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-cce060c348"></a>

| Index fact | Value |
|---|---|
| `columns` | `["selection_sha256","artifact_order"]` |
| `definition` | `"CREATE UNIQUE INDEX ix_stove0_selection_members_order ON stove0_artifact_selection_members (selection_sha256, artifact_order)"` |
| `name` | `"ix_stove0_selection_members_order"` |
| `table` | `"stove0_artifact_selection_members"` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-a9a93649d5"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/3/structure/unique_indexes/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 975e71665dec520ac055d888c4a1b45b4919802b1817727be20a39a8d3255ba5 -->

```json
{
  "columns": [
    "selection_sha256",
    "artifact_order"
  ],
  "definition": "CREATE UNIQUE INDEX ix_stove0_selection_members_order ON stove0_artifact_selection_members (selection_sha256, artifact_order)",
  "name": "ix_stove0_selection_members_order",
  "table": "stove0_artifact_selection_members"
}
```

</details>
