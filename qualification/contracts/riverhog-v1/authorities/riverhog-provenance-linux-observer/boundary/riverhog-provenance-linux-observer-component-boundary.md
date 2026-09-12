# riverhog-provenance-linux-observer component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-linux-observer:riverhog-provenance-linux-observer-compon-61290e85ef:09854638c0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-linux-observer` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["riverhog-provenance","riverhog-provenance-linux-contracts"] |
| `distribution` | "riverhog-provenance-linux-observer" |
| `optional_dependencies` | empty object |
| `path` | "reference/riverhog/provenance/observers/linux" |
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

- `/boundaries/components/32`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98771297bba0032b0230556687d61a8bb1ed608dad6b0ef4d7a477ea9bf8de53 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance",
    "riverhog-provenance-linux-contracts"
  ],
  "distribution": "riverhog-provenance-linux-observer",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/observers/linux",
  "role": "reference_component"
}
```
