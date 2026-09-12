# riverhog-provenance-linux-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-linux-contracts:riverhog-provenance-linux-contracts-compo-9fff7df012:2a32b7762b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-linux-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["riverhog-provenance-contracts"] |
| `distribution` | "riverhog-provenance-linux-contracts" |
| `optional_dependencies` | empty object |
| `path` | "reference/riverhog/provenance/contracts/linux" |
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

- `/boundaries/components/29`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 55a45b71147e2ad0c9d7766f7a12bb793971b867e2c09887990e17b3113798eb -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance-contracts"
  ],
  "distribution": "riverhog-provenance-linux-contracts",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/contracts/linux",
  "role": "reference_component"
}
```
