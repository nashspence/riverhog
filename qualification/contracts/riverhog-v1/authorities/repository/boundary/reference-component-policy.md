# Reference component policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:repository:reference-component-policy:661dcc27f7 -->

| Audit field | Value |
|---|---|
| Authority | `repository` |
| Interface | `boundary` |
| Family | `references` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/reference_policy`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract

```json
"Checked-in references form a closed, tightly scoped, maintainer-selected, nonnormative conformance set."
```
