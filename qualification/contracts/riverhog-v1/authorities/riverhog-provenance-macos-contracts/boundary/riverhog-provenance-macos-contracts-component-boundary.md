# riverhog-provenance-macos-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-macos-contracts:riverhog-provenance-macos-contracts-compo-c34f47d4fe:154eb6c610 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-macos-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["riverhog-provenance-contracts"] |
| `distribution` | "riverhog-provenance-macos-contracts" |
| `optional_dependencies` | empty object |
| `path` | "reference/riverhog/provenance/contracts/macos" |
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

- `/boundaries/components/30`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b47c7c470d15d72f142a43764baae8d95a3c44d742dfb33bdb0f97ae9e1ee797 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance-contracts"
  ],
  "distribution": "riverhog-provenance-macos-contracts",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/contracts/macos",
  "role": "reference_component"
}
```
