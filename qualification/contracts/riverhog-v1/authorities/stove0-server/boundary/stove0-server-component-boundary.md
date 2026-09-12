# stove0-server component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-server:stove0-server-component-boundary:151ebfc28c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-server` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | additional keys=`stove0-server` |
| `dependencies` | ["http-api-contracts","riverhog-client","riverhog-protocol","state-schema","stove0-observer-client","stove0-observer-protocol","stove0-operator-contracts","stove0-protocol","stove0-recipe-config","stove0-target-client","stove0-target-protocol","time-formats"] |
| `distribution` | "stove0-server" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/application/server" |
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

- `/boundaries/components/41`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 52133f596bcaf6efb19342c532476dae5e76936add38866137ba52ffd3dbb3c7 -->

```json
{
  "console_scripts": {
    "stove0-server": "stove0_api.app:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-client",
    "riverhog-protocol",
    "state-schema",
    "stove0-observer-client",
    "stove0-observer-protocol",
    "stove0-operator-contracts",
    "stove0-protocol",
    "stove0-recipe-config",
    "stove0-target-client",
    "stove0-target-protocol",
    "time-formats"
  ],
  "distribution": "stove0-server",
  "optional_dependencies": {},
  "path": "reference/stove0/application/server",
  "role": "reference_application"
}
```
