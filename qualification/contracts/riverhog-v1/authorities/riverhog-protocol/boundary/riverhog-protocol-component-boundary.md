# riverhog-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-protocol:riverhog-protocol-component-boundary:3737face3c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-protocol` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["http-api-contracts","lifecycle-events","riverhog-provenance-contracts"] |
| `distribution` | "riverhog-protocol" |
| `optional_dependencies` | empty object |
| `path` | "packages/riverhog-protocol" |
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

- `/boundaries/components/7`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 90b954e7efb6f9f8d9a0ed08063211e33ac04c9f992ac100ae80b01cacfc4c44 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "lifecycle-events",
    "riverhog-provenance-contracts"
  ],
  "distribution": "riverhog-protocol",
  "optional_dependencies": {},
  "path": "packages/riverhog-protocol",
  "role": "reusable_library"
}
```
