# riverhog-recover component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-recover:riverhog-recover-component-boundary:23d8e1f86c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-recover` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | additional keys=`riverhog-recover` |
| `dependencies` | ["riverhog-archive-contracts","riverhog-protocol","riverhog-provenance"] |
| `distribution` | "riverhog-recover" |
| `optional_dependencies` | empty object |
| `path` | "reference/riverhog/recovery" |
| `role` | "reference_application" |

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

- `/boundaries/components/35`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cbf685bdb666a92c0942e8c92d1e82b4c6bc3811bad73b2808a9be283205053f -->

```json
{
  "console_scripts": {
    "riverhog-recover": "riverhog_recover.cli:main"
  },
  "dependencies": [
    "riverhog-archive-contracts",
    "riverhog-protocol",
    "riverhog-provenance"
  ],
  "distribution": "riverhog-recover",
  "optional_dependencies": {},
  "path": "reference/riverhog/recovery",
  "role": "reference_application"
}
```
