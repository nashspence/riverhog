# stove0-review-sampler-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-sampler-protocol:stove0-review-sampler-protocol-component-boundary:f4aeeb66b3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-review-sampler-protocol` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["http-api-contracts","riverhog-protocol","stove0-protocol"] |
| `distribution` | "stove0-review-sampler-protocol" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/targets/review/sampler/protocol" |
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

- `/boundaries/components/67`

### Exact owned JSON

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
