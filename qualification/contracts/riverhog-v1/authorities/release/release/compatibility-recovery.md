# Compatibility: recovery

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:compatibility-recovery:b9527b16ee -->

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `compatibility` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/release/compatibility/recovery`

## Effective policies

- `compatibility/recovery/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

- Shape: "Every later v1 recovery release reads every valid earlier v1 archive and provenance set."

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f7f0f48d479c3063f37943c291f033ea396284b3228af54b185b755b0b3e968 -->

```json
"Every later v1 recovery release reads every valid earlier v1 archive and provenance set."
```
