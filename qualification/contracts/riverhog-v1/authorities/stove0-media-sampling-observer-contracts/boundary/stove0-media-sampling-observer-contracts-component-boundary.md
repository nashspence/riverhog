# stove0-media-sampling-observer-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-media-sampling-observer-contracts:stove0-media-sampling-observer-contracts-d85f92867b:a64052d707 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-media-sampling-observer-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["stove0-observer-protocol"] |
| `distribution` | "stove0-media-sampling-observer-contracts" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/observers/contracts/media-sampling" |
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

- `/boundaries/components/43`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a0100d502012b6d31761faf7a969f8b5bdab3258bd65b0b62ce686466f05361 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "stove0-observer-protocol"
  ],
  "distribution": "stove0-media-sampling-observer-contracts",
  "optional_dependencies": {},
  "path": "reference/stove0/observers/contracts/media-sampling",
  "role": "reference_component"
}
```
