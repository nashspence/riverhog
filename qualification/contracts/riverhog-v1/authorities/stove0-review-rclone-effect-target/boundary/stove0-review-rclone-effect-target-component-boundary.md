# stove0-review-rclone-effect-target component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-compon-638caf4063:19957af060 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-rclone-effect-target` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/65`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract

```json
{
  "console_scripts": {
    "stove0-review-rclone-effect-target": "stove0_review_rclone_effect_target.app:main"
  },
  "dependencies": [
    "riverhog-client",
    "riverhog-protocol",
    "stove0-review-target-contracts",
    "stove0-review-target-support",
    "stove0-target-support"
  ],
  "distribution": "stove0-review-rclone-effect-target",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/rclone-effect-target",
  "role": "reference_component"
}
```
