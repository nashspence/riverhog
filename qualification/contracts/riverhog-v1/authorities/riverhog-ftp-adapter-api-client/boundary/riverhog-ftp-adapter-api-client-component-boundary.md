# riverhog-ftp-adapter-api-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-api-client-component-boundary:080243d900 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter-api-client` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["http-api-contracts"] |
| `distribution` | "riverhog-ftp-adapter-api-client" |
| `optional_dependencies` | empty object |
| `path` | "reference/riverhog/ingress/ftp-api-client" |
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

- `/boundaries/components/28`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 32f59f147ed4efabd494b32e3cfc2e7a7776a4e5fe4cc96549f9f100a90fb5f5 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts"
  ],
  "distribution": "riverhog-ftp-adapter-api-client",
  "optional_dependencies": {},
  "path": "reference/riverhog/ingress/ftp-api-client",
  "role": "reference_component"
}
```
