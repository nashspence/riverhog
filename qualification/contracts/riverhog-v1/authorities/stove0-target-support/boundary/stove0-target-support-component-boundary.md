# stove0-target-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-target-support:stove0-target-support-component-boundary:a106e9e41a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-target-support` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | additional keys=`stove0-target-conformance`, `stove0-target-schemas` |
| `dependencies` | ["http-api-contracts","riverhog-client","riverhog-protocol","stove0-target-client","stove0-target-protocol"] |
| `distribution` | "stove0-target-support" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/packages/target-support" |
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

- `/boundaries/components/55`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a034d2c3c2fab7ca469e5eafe6b98ca16a4a97087a54d25aff3950e84366bc9 -->

```json
{
  "console_scripts": {
    "stove0-target-conformance": "stove0_target_support.conformance:main",
    "stove0-target-schemas": "stove0_target_support.schemas:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-client",
    "riverhog-protocol",
    "stove0-target-client",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-target-support",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/target-support",
  "role": "reusable_library"
}
```
