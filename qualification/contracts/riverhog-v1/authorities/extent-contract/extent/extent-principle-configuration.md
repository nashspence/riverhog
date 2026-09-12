# Extent principle: configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: extent:extent-contract:extent-principle-configuration:ed868d994d -->

| Audit field | Value |
|---|---|
| Authority | `extent-contract` |
| Interface | `extent` |
| Family | `principles` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/extents/principles/configuration`

## Effective policies

- `extent-principle/configuration/v1`

## Executable sources and proof

- `extent:extent-contract` — `scripts/extent_contract.py::extent_projection`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make contract-freeze`
- Proof: `make operation-qualification`

## Contract summary

- Shape: "Hardware- or environment-dependent limits that observably affect accepted work are operator-configurable and source-linked."

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad9c9db446693f985358d6094a1ca38d63c535fab151fb471f1916d1d9425083 -->

```json
"Hardware- or environment-dependent limits that observably affect accepted work are operator-configurable and source-linked."
```
