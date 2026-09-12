# stove0-api-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-api-client:stove0-api-client-component-boundary:0f99add1c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-api-client` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["http-api-contracts","stove0-operator-contracts","stove0-protocol"] |
| `distribution` | "stove0-api-client" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/packages/api-client" |
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

- `/boundaries/components/46`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bc419930d0e876f90972a328d4358adfe116adeee6179b337b077459aedd0387 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "stove0-operator-contracts",
    "stove0-protocol"
  ],
  "distribution": "stove0-api-client",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/api-client",
  "role": "reusable_library"
}
```
