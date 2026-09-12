# Release tag template

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:release-tag-template:2a4162b673 -->

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `release-contract` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/release/tag_template`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

- Shape: "v{version}"

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 268982a6c7e0cdd99400f4c234214a775df3b3dc046efd372f71ad85d564ff3e -->

```json
"v{version}"
```
