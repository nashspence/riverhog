# http-api-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:http-api-contracts:http-api-contracts-component-boundary:f712265b1f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `http-api-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | [] |
| `distribution` | "http-api-contracts" |
| `optional_dependencies` | empty object |
| `path` | "packages/http-api-contracts" |
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

- `/boundaries/components/1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 357534d34f65d1ee9ee0c71a933da003f11ecaca1db6d413c17490e4d4e92264 -->

```json
{
  "console_scripts": {},
  "dependencies": [],
  "distribution": "http-api-contracts",
  "optional_dependencies": {},
  "path": "packages/http-api-contracts",
  "role": "reusable_library"
}
```
