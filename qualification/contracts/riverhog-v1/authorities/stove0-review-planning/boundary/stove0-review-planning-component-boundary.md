# stove0-review-planning component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-planning:stove0-review-planning-component-boundary:d9df6bda01 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-review-planning` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | additional keys=`stove0-review-planning` |
| `dependencies` | ["stove0-media-sampling-observer-contracts","stove0-protocol","stove0-review-target-contracts"] |
| `distribution` | "stove0-review-planning" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/targets/review/planning" |
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

- `/boundaries/components/64`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7ebd931b6d8a76f36f030b0228c4681748aa8701e58ea8059fe59e481efdbd3f -->

```json
{
  "console_scripts": {
    "stove0-review-planning": "stove0_review_planning.conformance:main"
  },
  "dependencies": [
    "stove0-media-sampling-observer-contracts",
    "stove0-protocol",
    "stove0-review-target-contracts"
  ],
  "distribution": "stove0-review-planning",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/planning",
  "role": "reference_component"
}
```
