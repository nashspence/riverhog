# riverhog-ftp-adapter component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-ftp-adapter:riverhog-ftp-adapter-component-boundary:33f5955bfe -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | additional keys=`riverhog-ftp-adapter` |
| `dependencies` | ["http-api-contracts","riverhog-client","riverhog-ftp-adapter-api-client","riverhog-protocol","riverhog-provenance"] |
| `distribution` | "riverhog-ftp-adapter" |
| `optional_dependencies` | empty object |
| `path` | "reference/riverhog/ingress/ftp" |
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

- `/boundaries/components/27`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59d9e4097d12863a20e0d3c3d2fff5e7c2253b4dc1e0cdebce56e74f8bb883af -->

```json
{
  "console_scripts": {
    "riverhog-ftp-adapter": "riverhog_ftp_adapter.app:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-client",
    "riverhog-ftp-adapter-api-client",
    "riverhog-protocol",
    "riverhog-provenance"
  ],
  "distribution": "riverhog-ftp-adapter",
  "optional_dependencies": {},
  "path": "reference/riverhog/ingress/ftp",
  "role": "reference_component"
}
```
