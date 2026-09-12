# Compatibility: configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:compatibility-configuration:ba0b4cc6a6 -->

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `compatibility` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/release/compatibility/configuration`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

- Shape: "Accepted v1 configuration remains valid throughout v1 unless an unsafe value must be rejected."

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de93df3805314638224be1b8f0f4a7b30bd0b1d03e6c8b6938130f9b08122aec -->

```json
"Accepted v1 configuration remains valid throughout v1 unless an unsafe value must be rejected."
```
