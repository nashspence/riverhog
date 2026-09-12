# Compatibility: python api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:compatibility-python-api:112aef323a -->

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `compatibility` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/release/compatibility/python_api`

## Effective policies

- `compatibility/python-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

- Shape: "Reusable-library declared public-module exports and public signatures remain backward compatible throughout v1; undeclared implementation submodules and other component roles are not Python API promises."

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ecf544dd59d5ef2c7fd2d9914ab0230ea456a75ae18045efa13467056c0c9287 -->

```json
"Reusable-library declared public-module exports and public signatures remain backward compatible throughout v1; undeclared implementation submodules and other component roles are not Python API promises."
```
