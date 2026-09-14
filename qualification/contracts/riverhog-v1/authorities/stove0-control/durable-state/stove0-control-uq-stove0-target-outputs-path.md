# stove0-control: uq_stove0_target_outputs_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-uq-stove0-target-outputs-path:78a280ac0a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-c299de8d78"></a>
| Index fact | Value |
|---|---|
| `columns` | `["work_id","job_id","output_path"]` |
| `definition` | `"CREATE UNIQUE INDEX uq_stove0_target_outputs_path ON stove0_target_outputs (work_id, job_id, output_path)"` |
| `name` | `"uq_stove0_target_outputs_path"` |
| `table` | `"stove0_target_outputs"` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-control-durable-state-identity.md)

## Governing policies

- <a id="pa-7b77390b3c"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:stove0-control](../../../evidence/sources.md#src-45e44b17fd) — `reference/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py`

### Machine authority

- `/external_contract/durable_state/owners/3/structure/unique_indexes/2`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9ff2d64118fc592ef7189232a8c2d3057f8c35986d96463412c99294be34d57a -->

```json
{
  "columns": [
    "work_id",
    "job_id",
    "output_path"
  ],
  "definition": "CREATE UNIQUE INDEX uq_stove0_target_outputs_path ON stove0_target_outputs (work_id, job_id, output_path)",
  "name": "uq_stove0_target_outputs_path",
  "table": "stove0_target_outputs"
}
```
