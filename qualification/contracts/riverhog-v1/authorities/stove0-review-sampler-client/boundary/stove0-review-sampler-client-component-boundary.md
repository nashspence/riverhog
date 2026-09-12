# stove0-review-sampler-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-sampler-client:stove0-review-sampler-client-component-boundary:2da2ddf525 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-review-sampler-client` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["http-api-contracts","stove0-review-sampler-protocol"] |
| `distribution` | "stove0-review-sampler-client" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/targets/review/sampler/client" |
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

- `/boundaries/components/66`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 19df098b2326b6585b2c084521dbb826fd9ea9ed7dcc9ea3d36b7a9a0f7a9b7e -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "stove0-review-sampler-protocol"
  ],
  "distribution": "stove0-review-sampler-client",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/sampler/client",
  "role": "reference_component"
}
```
