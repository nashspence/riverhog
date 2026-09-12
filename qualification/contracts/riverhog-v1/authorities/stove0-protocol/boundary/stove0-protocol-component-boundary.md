# stove0-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-protocol:stove0-protocol-component-boundary:4c1f816eb1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-protocol` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["riverhog-protocol"] |
| `distribution` | "stove0-protocol" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/packages/protocol" |
| `role` | "reusable_library" |

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

- `/boundaries/components/51`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f98eb3abcca65308b8f58d79cdfb5401b551d8cbe5c98361d71ea613c7350fcc -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-protocol"
  ],
  "distribution": "stove0-protocol",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/protocol",
  "role": "reusable_library"
}
```
