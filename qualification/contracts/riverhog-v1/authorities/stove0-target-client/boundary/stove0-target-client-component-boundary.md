# stove0-target-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-target-client:stove0-target-client-component-boundary:f9de47e9b5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-target-client` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["http-api-contracts","stove0-target-protocol"] |
| `distribution` | "stove0-target-client" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/packages/target-client" |
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

- `/boundaries/components/53`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b201d8b2c97c9751893de9bae7b0cc41aa826a67938c92a5be9c7876ae888a13 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-target-client",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/target-client",
  "role": "reusable_library"
}
```
