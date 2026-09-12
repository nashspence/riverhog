# riverhog-storage-adapter-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-component-boundary:161627301d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-protocol` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["time-formats"] |
| `distribution` | "riverhog-storage-adapter-protocol" |
| `optional_dependencies` | empty object |
| `path` | "packages/riverhog-storage-adapter-protocol" |
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

- `/boundaries/components/11`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 632e07c9f235fe41e4baaed295768b2c99f0e8a27458437b82f4838a7767afb9 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "time-formats"
  ],
  "distribution": "riverhog-storage-adapter-protocol",
  "optional_dependencies": {},
  "path": "packages/riverhog-storage-adapter-protocol",
  "role": "reusable_library"
}
```
