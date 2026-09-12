# stove0-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-client:stove0-client-component-boundary:11bea8fac1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-client` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | additional keys=`stove0` |
| `dependencies` | ["stove0-api-client","stove0-protocol","stove0-recipe-config"] |
| `distribution` | "stove0-client" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/application/client" |
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

- `/boundaries/components/40`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5050ad11e1faec9510d38db6cf5d9fe7106e65d8c58883b728a82e69afea1116 -->

```json
{
  "console_scripts": {
    "stove0": "stove0_cli.main:main"
  },
  "dependencies": [
    "stove0-api-client",
    "stove0-protocol",
    "stove0-recipe-config"
  ],
  "distribution": "stove0-client",
  "optional_dependencies": {},
  "path": "reference/stove0/application/client",
  "role": "reference_application"
}
```
