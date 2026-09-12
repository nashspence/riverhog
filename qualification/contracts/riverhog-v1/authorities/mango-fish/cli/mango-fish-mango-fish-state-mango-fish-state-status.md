# mango-fish mango-fish state mango-fish state status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:mango-fish:mango-fish-mango-fish-state-mango-fish-state-status:cb951bc12c -->

| Audit field | Value |
|---|---|
| Authority | `mango-fish` |
| Interface | `cli` |
| Family | `mango-fish state` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/cli/mango-fish/commands/state/commands/status/name`
- `/external_contract/cli/mango-fish/commands/state/commands/status/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:mango-fish` — `reference/riverhog/applications/mango-fish/src/mango_fish/cli.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |

## Contract

- Parser name: `mango-fish state status`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreTrueAction | no |  | --json |
