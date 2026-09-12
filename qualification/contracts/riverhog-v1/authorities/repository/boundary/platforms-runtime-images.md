# Platforms runtime images

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:repository:platforms-runtime-images:85d134374a -->

| Audit field | Value |
|---|---|
| Authority | `repository` |
| Interface | `boundary` |
| Family | `runtime-images` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/runtime_images/platforms`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

- Shape: array (1 items)

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac8335d2e189797f6099e4d37b3fd5a69dcc42bfb5a2b077bd758e7de971ca62 -->

```json
[
  "linux/amd64"
]
```
