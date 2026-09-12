# riverhog-storage-adapter-schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-storage-adapter-schemas:riverhog-storage-adapter-schemas:70bfaac081 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-schemas` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/cli/riverhog-storage-adapter-schemas/name`
- `/external_contract/cli/riverhog-storage-adapter-schemas/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:riverhog-storage-adapter-schemas` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `riverhog-storage-adapter-schemas`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | no | Path | --output |
| `` | _StoreTrueAction | no |  | --compact |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-storage-adapter-schemas/name`

<!-- exact-contract-value: 5d3570bf51282d4e0e38219724ecae45dc42a71b7197217815a8e82244b0a3d4 -->

```json
"riverhog-storage-adapter-schemas"
```

### `/external_contract/cli/riverhog-storage-adapter-schemas/parameters`

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
