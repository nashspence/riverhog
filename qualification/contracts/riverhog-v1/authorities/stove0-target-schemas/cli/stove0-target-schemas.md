# stove0-target-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-target-schemas:stove0-target-schemas:f942f8f5bb -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-schemas` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/cli/stove0-target-schemas/name`
- `/external_contract/cli/stove0-target-schemas/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0-target-schemas` — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `stove0-target-schemas`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | no | Path | --output |
| `` | _StoreTrueAction | no |  | --compact |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-target-schemas/name`

<!-- exact-contract-value: 061cab384f391f332f2ed0807ef95bc47f0507c010da1026b316ff00f4fab52b -->

```json
"stove0-target-schemas"
```

### `/external_contract/cli/stove0-target-schemas/parameters`

<!-- exact-contract-value: d09dd1e324eea493304f34cc58b3f5fe965bfdb99ef580493ab2779a9afcca7d -->

```json
[
  {
    "dest": "output",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--output"
    ],
    "required": false,
    "type": "Path"
  },
  {
    "default": false,
    "dest": "compact",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--compact"
    ],
    "required": false
  }
]
```
