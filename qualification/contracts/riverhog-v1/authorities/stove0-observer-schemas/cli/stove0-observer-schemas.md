# stove0-observer-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-observer-schemas:stove0-observer-schemas:5ada40dcb8 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-schemas` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/cli/stove0-observer-schemas/name`
- `/external_contract/cli/stove0-observer-schemas/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0-observer-schemas` — `reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `stove0-observer-schemas`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | no | Path | --output |
| `` | _StoreTrueAction | no |  | --compact |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-observer-schemas/name`

<!-- exact-contract-value: 39b4b08f24b8bf64cfcb76e91c23b866c91276b8e46a6d44e0a83af9f5f646e3 -->

```json
"stove0-observer-schemas"
```

### `/external_contract/cli/stove0-observer-schemas/parameters`

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
