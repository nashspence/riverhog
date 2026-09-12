# Compatibility: archive

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:compatibility-archive:b5669567b0 -->

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `compatibility` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/release/compatibility/archive`

## Effective policies

- `compatibility/archive/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

- Shape: "A valid v1 archive remains independently recoverable throughout the supported v1 lifetime. Pack construction, ingress checkpointing, and resumable-write strategies are not archive-format compatibility promises."

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12131fa17ee13c3795532979ccfd8b5095c8239671134270909a68afdff5308a -->

```json
"A valid v1 archive remains independently recoverable throughout the supported v1 lifetime. Pack construction, ingress checkpointing, and resumable-write strategies are not archive-format compatibility promises."
```
