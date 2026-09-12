# stove0-opus-review-sampler component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-opus-review-sampler:stove0-opus-review-sampler-component-boundary:9d75573a66 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-opus-review-sampler` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | additional keys=`stove0-opus-review-sampler` |
| `dependencies` | ["http-api-contracts","stove0-media-archive-target-contracts","stove0-review-sampler-protocol","stove0-review-sampler-support","stove0-review-target-contracts"] |
| `distribution` | "stove0-opus-review-sampler" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/targets/opus/review-sampler" |
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

- `/boundaries/components/60`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 45dedda654ab59cb23ee0afb6886562fecf9295e829d2372c5a077049d9224f5 -->

```json
{
  "console_scripts": {
    "stove0-opus-review-sampler": "stove0_opus_review_sampler.app:main"
  },
  "dependencies": [
    "http-api-contracts",
    "stove0-media-archive-target-contracts",
    "stove0-review-sampler-protocol",
    "stove0-review-sampler-support",
    "stove0-review-target-contracts"
  ],
  "distribution": "stove0-opus-review-sampler",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/opus/review-sampler",
  "role": "reference_component"
}
```
