# stove0-review-target-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-target-support:stove0-review-target-support-component-boundary:656c45b9d0 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-target-support` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/69`

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
| `dependencies` | array (8 items) |
| `distribution` | "stove0-review-target-support" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/stove0/targets/review/support" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cf7b12ec9a1179e1d5447669ac783a8a5d059a639d10b3a3a478425b5143896c -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "riverhog-client",
    "riverhog-protocol",
    "stove0-protocol",
    "stove0-review-sampler-client",
    "stove0-review-sampler-protocol",
    "stove0-review-target-contracts",
    "stove0-target-support"
  ],
  "distribution": "stove0-review-target-support",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/support",
  "role": "reference_component"
}
```
