# stove0-media-metadata-observer-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-5b9f85c41b:8900cf2c22 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-media-metadata-observer-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["stove0-observer-protocol"] |
| `distribution` | "stove0-media-metadata-observer-contracts" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/observers/contracts/media-metadata" |
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

- `/boundaries/components/42`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08ccde38e7b0d13fbacd1c26c58068780224373b88950d673a3e723d96349337 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "stove0-observer-protocol"
  ],
  "distribution": "stove0-media-metadata-observer-contracts",
  "optional_dependencies": {},
  "path": "reference/stove0/observers/contracts/media-metadata",
  "role": "reference_component"
}
```
