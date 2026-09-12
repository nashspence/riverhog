# mango-fish mango-fish state mango-fish state upgrade

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:mango-fish:mango-fish-mango-fish-state-mango-fish-state-upgrade:a4849199e0 -->

| Audit field | Value |
|---|---|
| Authority | `mango-fish` |
| Interface | `cli` |
| Family | `mango-fish state` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/cli/mango-fish/commands/state/commands/upgrade/name`
- `/external_contract/cli/mango-fish/commands/state/commands/upgrade/parameters`

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

## Contract summary

- Parser name: `mango-fish state upgrade`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreTrueAction | no |  | --json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/mango-fish/commands/state/commands/upgrade/name`

<!-- exact-contract-value: 6c87db9d632bfdae0883d34c415798e02d392a141213ec3a8911b509b2cc965c -->

```json
"mango-fish state upgrade"
```

### `/external_contract/cli/mango-fish/commands/state/commands/upgrade/parameters`

<!-- exact-contract-value: a4eee56605df36f2261ef19e35bdc3c16631d89a610d870dece837b2b2866441 -->

```json
[
  {
    "default": false,
    "dest": "json",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--json"
    ],
    "required": false
  }
]
```
