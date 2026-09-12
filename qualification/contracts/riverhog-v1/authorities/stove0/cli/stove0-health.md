# stove0 health

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0-health:df4c9c7dfa -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `cli` |
| Family | `health` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/cli/stove0/commands/health/name`
- `/external_contract/cli/stove0/commands/health/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0` — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: health_live](../operation/operation-parity-health-live.md)
- [Operation parity: health_ready](../operation/operation-parity-health-ready.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `health`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `ready` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --ready |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/health/name`

<!-- exact-contract-value: 69b7c75b4a0260f2e018aae0172314b00e80b3899c79ba4d71e070ba74cd8db4 -->

```json
"health"
```

### `/external_contract/cli/stove0/commands/health/parameters`

<!-- exact-contract-value: bb7aa89b48a0fb797521cb9cfad5a62b55dce8cb3a19bcda2acbd80306355cb4 -->

```json
[
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "ready",
    "nargs": 1,
    "options": [
      "--ready"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  }
]
```
