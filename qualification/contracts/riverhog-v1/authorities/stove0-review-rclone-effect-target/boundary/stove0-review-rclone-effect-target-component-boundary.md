# stove0-review-rclone-effect-target component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-compon-638caf4063:19957af060 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-review-rclone-effect-target` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | additional keys=`stove0-review-rclone-effect-target` |
| `dependencies` | ["riverhog-client","riverhog-protocol","stove0-review-target-contracts","stove0-review-target-support","stove0-target-support"] |
| `distribution` | "stove0-review-rclone-effect-target" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/targets/review/rclone-effect-target" |
| `role` | "reference_component" |

## Governing policies

- `boundary/frozen-authority/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

### Machine authority

- `/boundaries/components/65`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4e79e8d3823950947e237347ed5551f7665fec65cc063f3edae49427861bdc5f -->

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
