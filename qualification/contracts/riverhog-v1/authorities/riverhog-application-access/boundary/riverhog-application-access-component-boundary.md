# riverhog-application-access component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-application-access:riverhog-application-access-component-boundary:529bb88f14 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-application-access` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["riverhog-protocol"] |
| `distribution` | "riverhog-application-access" |
| `optional_dependencies` | empty object |
| `path` | "packages/riverhog-application-access" |
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

- `/boundaries/components/4`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 51196e02687ed1bed81a81e73945f59d7e16e2dab709e6d9cfbef6b2b22a437d -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-protocol"
  ],
  "distribution": "riverhog-application-access",
  "optional_dependencies": {},
  "path": "packages/riverhog-application-access",
  "role": "reusable_library"
}
```
