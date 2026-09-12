# Compatibility: http api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:compatibility-http-api:859d1abf1a -->

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `compatibility` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/release/compatibility/http_api`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

- Shape: "Published v1 HTTP and CloudEvents contracts remain backward compatible throughout v1."

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2585cfdddab6eb45045fb1e0a19776f07029a40553fc17b28811f710586512ac -->

```json
"Published v1 HTTP and CloudEvents contracts remain backward compatible throughout v1."
```
