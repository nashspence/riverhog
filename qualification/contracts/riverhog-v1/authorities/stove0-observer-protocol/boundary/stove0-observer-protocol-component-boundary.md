# stove0-observer-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-observer-protocol:stove0-observer-protocol-component-boundary:fea970d8a5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-protocol` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["http-api-contracts","stove0-protocol"] |
| `distribution` | "stove0-observer-protocol" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/packages/observer-protocol" |
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

- `/boundaries/components/48`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3053f9f50e6579637c9beeacdfee3165baaf3bbe8fa86adfe7257e79ec534d5d -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "stove0-protocol"
  ],
  "distribution": "stove0-observer-protocol",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/observer-protocol",
  "role": "reusable_library"
}
```
