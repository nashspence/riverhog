# stove0 admission show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0-admission-show:0ea7347105 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `cli` |
| Family | `admission` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/cli/stove0/commands/admission/commands/show/name`
- `/external_contract/cli/stove0/commands/admission/commands/show/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0` — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: get_admission](../operation/operation-parity-get-admission.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `show`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `admission_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | admission_id |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/admission/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/stove0/commands/admission/commands/show/parameters`

<!-- exact-contract-value: f0147f7c85fb6352af335636f4d2091373b3e3691b02d05f9bf96424681ec398 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "admission_id",
    "nargs": 1,
    "options": [
      "admission_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  }
]
```
