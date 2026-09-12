# stove0-review-sampler-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-sampler-protocol:stove0-review-sampler-protocol-component-boundary:f4aeeb66b3 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-sampler-protocol` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/67`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

| Field | Shape |
|---|---|
| `console_scripts` | object (0 fields) |
| `dependencies` | array (3 items) |
| `distribution` | "stove0-review-sampler-protocol" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/stove0/targets/review/sampler/protocol" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42b78928abf2886def21bfc83b2129b4b35d2edf36112df550bd4cb3e63e4002 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "riverhog-protocol",
    "stove0-protocol"
  ],
  "distribution": "stove0-review-sampler-protocol",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/sampler/protocol",
  "role": "reference_component"
}
```
